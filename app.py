"""Ghi cam EarthCam Abbey Road theo lịch và tải lên Google Drive bằng rclone."""
import asyncio
import json
import math
import os
import platform
import re
import shutil
import signal
import tarfile
import time
import uuid
import zipfile
from collections import deque
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Literal
from urllib.parse import parse_qs, quote, urljoin, urlparse
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, Response, StreamingResponse
from pydantic import BaseModel, Field

BASE = Path(__file__).resolve().parent
BIN = BASE / "bin"
CONFIG_PATH = BASE / "config.json"
DB_PATH = BASE / "files.json"
LONDON = ZoneInfo("Europe/London")
TZS = {"Europe/London": LONDON, "Asia/Ho_Chi_Minh": ZoneInfo("Asia/Ho_Chi_Minh")}
PAGE = "https://www.earthcam.com/world/england/london/abbeyroad/"
REFERER = "https://www.earthcam.com/"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"
HDRS = {"Referer": REFERER, "User-Agent": UA}
PIPE, STDOUT, DEVNULL = asyncio.subprocess.PIPE, asyncio.subprocess.STDOUT, asyncio.subprocess.DEVNULL
SLOT_DEFAULTS = {"repeat": "daily", "days": [0, 1, 2, 3, 4, 5, 6], "date": "", "tz": "Europe/London", "videos": 0, "segment_seconds": 0}


def _slot(i, name, start, end):
    # Drive folder IDs are personal: they live only in config.json (git-ignored), set from the web UI
    return {"id": i, "name": name, "start": start, "end": end, "folder_id": "", "enabled": True, **SLOT_DEFAULTS}


DEFAULT_CONFIG = {
    "stream_url": "",
    "schedule_enabled": True,
    "segment_seconds": 120,
    "prefix": "abbeyroad",
    "output_dir": str(BASE / "recordings"),
    "delete_after_upload": False,
    "storage": "local",  # local = only save into <output_dir>/<slot name>/ ; gdrive = also upload
    "slots": [
        _slot("s1", "Sáng 7-8", "07:00", "08:00"),
        _slot("s2", "Sáng 9-10", "09:00", "10:00"),
        _slot("s3", "Trưa 11-12", "11:00", "12:00"),
        _slot("s4", "Trưa 1-3", "13:00", "15:00"),
        _slot("s5", "Tối 6-7", "18:00", "19:00"),
        _slot("s6", "Tối 9-10", "21:00", "22:00"),
    ],
}


def load_json(path, default):
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        return json.loads(json.dumps(default))


def save_json(path, data):
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    tmp.replace(path)  # atomic: a crash never leaves a half-written config


CFG = {**DEFAULT_CONFIG, **load_json(CONFIG_PATH, DEFAULT_CONFIG)}
CFG["slots"] = [{**SLOT_DEFAULTS, **s} for s in CFG["slots"]]  # older configs lack the newer fields
FILES: dict = load_json(DB_PATH, {})  # name -> record
save_cfg = lambda: save_json(CONFIG_PATH, CFG)
save_db = lambda: save_json(DB_PATH, FILES)


def tool(name):
    """Prefer the copy installed by the in-app installer, then PATH (apt, nix…)."""
    p = BIN / name
    return str(p) if p.exists() else shutil.which(name)


def safe_name(name):
    return re.sub(r"[/\\\x00-\x1f]", "-", name).strip().strip(".") or "khac"


def slot_dir(name):
    """Each slot records into its own folder inside the chosen root."""
    return Path(CFG["output_dir"]) / safe_name(name)


def ensure_slot_dirs():
    for s in CFG["slots"]:
        slot_dir(s["name"]).mkdir(parents=True, exist_ok=True)


# ---------- log ----------
LOGS: deque = deque(maxlen=500)
SUBS: set = set()


def log(msg, level="info"):
    e = {"t": time.time(), "level": level, "msg": msg}
    LOGS.append(e)
    print(f"[{level}] {msg}", flush=True)
    for q in SUBS:
        q.put_nowait(e)


def mmss(sec):
    sec = int(sec)
    return f"{sec // 3600}:{sec // 60 % 60:02d}:{sec % 60:02d}" if sec >= 3600 else f"{sec // 60}:{sec % 60:02d}"


# ---------- lịch (mỗi khung có múi giờ riêng, tự theo giờ mùa hè/đông) ----------
def _hm(s):
    h, m = s.split(":")
    return int(h), int(m)


def runs_on(slot, d):
    r = slot["repeat"]
    return r == "daily" or (r == "weekly" and d.weekday() in slot["days"]) or (r == "once" and d.isoformat() == slot["date"])


def occurrence(slot, d):
    """(start_ts, end_ts) of the occurrence starting on local date d; end<=start means it crosses midnight."""
    tz = TZS[slot["tz"]]
    sh, sm = _hm(slot["start"])
    eh, em = _hm(slot["end"])
    ed = d + timedelta(days=1) if (eh, em) <= (sh, sm) else d
    start = datetime(d.year, d.month, d.day, sh, sm, tzinfo=tz)
    end = datetime(ed.year, ed.month, ed.day, eh, em, tzinfo=tz)
    return start.timestamp(), end.timestamp()  # compare as UTC timestamps; same-zone datetime compare ignores DST


def active_slot(slots, now, only_enabled=True):
    for s in slots:
        if only_enabled and not s["enabled"]:
            continue
        today = datetime.fromtimestamp(now, TZS[s["tz"]]).date()
        for d in (today - timedelta(days=1), today):
            if runs_on(s, d):
                st, en = occurrence(s, d)
                if st <= now < en:
                    return s, st, en
    return None


def slot_next(s, now):
    today = datetime.fromtimestamp(now, TZS[s["tz"]]).date()
    if s["repeat"] == "once":
        days = [date.fromisoformat(s["date"])] if s["date"] else []
    else:
        days = [today + timedelta(days=i) for i in range(8)]
    for d in days:
        if runs_on(s, d):
            st, en = occurrence(s, d)
            if st > now:
                return st, en
    return None


def next_slot(slots, now):
    best = None
    for s in slots:
        if s["enabled"] and (n := slot_next(s, now)) and (best is None or n[0] < best[1]):
            best = (s, *n)
    return best


def seg_len(seg, videos, duration):
    """Explicit length wins; else split the duration into `videos` files; else the global default."""
    if seg:
        return max(10, int(seg))
    if videos and duration:
        return max(10, round(duration / videos))
    return CFG["segment_seconds"]


# ---------- luồng m3u8 ----------
http = httpx.AsyncClient(follow_redirects=True, timeout=20)


def is_earthcam(u):
    p = urlparse(u)
    return p.scheme == "https" and (p.hostname or "").endswith(".earthcam.com")


async def check_url(url):
    """Open the master playlist and its first chunklist. Returns (ok, message)."""
    if not is_earthcam(url):
        return False, "Địa chỉ phải là https://…earthcam.com/…"
    try:
        r = await http.get(url, headers=HDRS)
        if r.status_code != 200 or "#EXTM3U" not in r.text:
            return False, f"Playlist trả về HTTP {r.status_code} (mã t= có thể đã hết hạn)"
        res = re.search(r"RESOLUTION=(\d+x\d+)", r.text)
        child = next((ln for ln in r.text.splitlines() if ln and not ln.startswith("#")), None)
        if child and ".m3u8" in child:
            r2 = await http.get(urljoin(str(r.url), child), headers=HDRS)
            if r2.status_code != 200 or "#EXTM3U" not in r2.text:
                return False, f"Chunklist trả về HTTP {r2.status_code} (mã t= có thể đã hết hạn)"
        return True, "Mở được luồng" + (f" ({res[1]})" if res else "")
    except httpx.HTTPError as e:
        return False, f"Lỗi mạng: {e!r}"


def url_td(url):
    return parse_qs(urlparse(url).query).get("td", ["?"])[0]


async def refresh_url(reason):
    """EarthCam embeds the signed URL in the page HTML as JSON key "stream"; no browser needed."""
    try:
        r = await http.get(PAGE, headers={"User-Agent": UA})
        if r.status_code != 200:
            raise RuntimeError(f"tải trang EarthCam bị HTTP {r.status_code} (có thể bị chặn bot)")
        urls = [json.loads(f'"{raw}"') for raw in re.findall(r'"stream":"(https:[^"]+?\.m3u8[^"]*)"', r.text)]
        if not urls:
            raise RuntimeError('trang không còn chứa khoá "stream" — EarthCam có thể đã đổi cách nhúng luồng')
        cur = urlparse(CFG["stream_url"]).path
        url = next((u for u in urls if cur and urlparse(u).path == cur), None) or next((u for u in urls if "AbbeyRoad" in u), urls[0])
        ok, msg = await check_url(url)
        if not ok:
            raise RuntimeError(f"lấy được địa chỉ mới nhưng không mở được: {msg}")
        CFG["stream_url"] = url
        save_cfg()
        rec.url_error = ""
        log(f"Đã tự lấy địa chỉ luồng mới ({reason}), td={url_td(url)}")
        return True
    except Exception as e:
        rec.url_error = (f"Không tự lấy được địa chỉ m3u8 mới: {e}. Hãy mở trang EarthCam Abbey Road, nhấn F12 → Network, "
                         "lọc 'm3u8', chép địa chỉ playlist.m3u8 rồi dán vào ô Địa chỉ luồng.")
        log(rec.url_error, "error")
        return False


# ---------- file & upload ----------
UPQ: asyncio.Queue = asyncio.Queue()
QUEUED: set = set()


async def probe_duration(path):
    if not tool("ffprobe"):
        return None
    p = await asyncio.create_subprocess_exec(tool("ffprobe"), "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0",
                                             str(path), stdout=PIPE, stderr=DEVNULL)
    out, _ = await p.communicate()
    try:
        return round(float(out.decode().strip()), 1)
    except ValueError:
        return None


async def register(path: Path, slot_name, folder_id):
    local = CFG["storage"] == "local"
    r = {"name": path.name, "dir": str(path.parent), "slot": slot_name, "folder_id": folder_id,
         "status": "saved" if local else "pending" if folder_id else "error",
         "error": "" if local or folder_id else "Khung giờ chưa có ID thư mục Drive",
         "size": path.stat().st_size, "duration": await probe_duration(path), "created": path.stat().st_mtime, "deleted": False}
    FILES[path.name] = r
    save_db()
    return r


def enqueue(name):
    if CFG["storage"] != "gdrive" or name in QUEUED:
        return False
    QUEUED.add(name)
    UPQ.put_nowait(name)
    return True


STATS_RE = re.compile(r"(\d+)%,\s*([^,]+/s)(?:,\s*ETA\s*(\S+))?")


async def upload_worker():
    while True:
        name = await UPQ.get()
        QUEUED.discard(name)
        r = FILES.get(name)
        if not r or r["status"] not in ("pending", "error") or not r.get("folder_id"):
            continue
        path = Path(r["dir"]) / name
        if not path.exists():
            r.update(status="error", error="File không còn trên máy")
            save_db()
            continue
        if not tool("rclone"):
            r.update(status="error", error="Chưa cài rclone — bấm 'Cài phần phụ thuộc'")
            save_db()
            continue
        r.update(status="uploading", error="", progress=0, speed="", eta="")
        save_db()
        log(f"Đang tải lên {name} ({r['size'] / 1e6:.1f} MB) → {r['slot']}")
        t0, tail = time.time(), deque(maxlen=4)
        try:
            p = await asyncio.create_subprocess_exec(
                tool("rclone"), "copy", str(path), "gdrive:", "--drive-root-folder-id", r["folder_id"],
                "--stats", "1s", "--stats-one-line", "--stats-log-level", "NOTICE", stdout=PIPE, stderr=STDOUT)
            while line := await p.stdout.readline():
                ln = line.decode(errors="replace").strip()
                if m := STATS_RE.search(ln):
                    r.update(progress=int(m[1]), speed=m[2].strip(), eta=m[3] or "")
                elif ln and "Config file" not in ln:
                    tail.append(ln)
            ok, msg = await p.wait() == 0, " | ".join(tail)[-500:]
        except OSError as e:
            ok, msg = False, str(e)
        dt = max(time.time() - t0, 0.1)
        if ok:
            r.update(status="done", error="", progress=100, uploaded=time.time())
            log(f"Đã lên Drive: {name} (mất {mmss(dt)}, {r['size'] / 1e6 / dt:.1f} MB/s)")
            if CFG["delete_after_upload"]:
                path.unlink(missing_ok=True)
                r["deleted"] = True
                log(f"Đã xoá trên máy sau khi tải lên: {name}")
        else:
            if "didn't find section" in msg:
                msg = "Chưa kết nối Google Drive — bấm 'Kết nối Google Drive' ở mục Cài đặt ban đầu"
            r.update(status="error", error=msg or "rclone lỗi")
            log(f"Tải lên lỗi {name}: {r['error']}", "error")
        save_db()


def slot_for_filename(name):
    m = re.search(r"_(\d{8}_\d{6})\.mp4$", name)
    if not m:
        return None
    ts = datetime.strptime(m[1], "%Y%m%d_%H%M%S").replace(tzinfo=LONDON).timestamp()  # ffmpeg names files in London time
    hit = active_slot(CFG["slots"], ts, only_enabled=False)
    return hit[0] if hit else None


async def sweep():
    """Register unknown files and queue everything not yet on Drive."""
    # only the root and the slot folders: the root may be e.g. ~ and must not be scanned recursively
    dirs = [(slot_dir(s["name"]), s) for s in CFG["slots"]] + [(Path(CFG["output_dir"]), None)]
    n = 0
    for d, slot in dirs:
        for p in sorted(d.glob("*.mp4")) if d.is_dir() else []:
            if rec.task and time.time() - p.stat().st_mtime < 30:
                continue  # file ffmpeg is still writing
            r = FILES.get(p.name)
            if r is None:
                s = slot or slot_for_filename(p.name)
                r = await register(p, s["name"] if s else "?", s["folder_id"] if s else None)
            if r["status"] in ("pending", "error") and r.get("folder_id") and enqueue(p.name):
                n += 1
    if n:
        log(f"Quét lại: {n} file chờ tải lên Drive")


# ---------- ghi ----------
class Recorder:
    def __init__(self):
        self.task = None
        self.proc = None
        self.session = None
        self.stop_event = asyncio.Event()
        self.status = "idle"
        self.error = ""
        self.url_error = ""
        self.fails = 0
        self.skip_until = 0.0
        self.media_time = self.seg_mark = 0.0
        self.speed = ""

    def start(self, session):
        if self.task:
            return False
        self.stop_event = asyncio.Event()
        self.status, self.error = "starting", ""
        self.session = {**session, "started": time.time(), "done": 0}
        self.task = asyncio.create_task(self._run(self.session))
        return True

    async def stop(self):
        task = self.task
        if not task:
            return
        self.stop_event.set()
        p = self.proc
        if p and p.returncode is None:
            p.send_signal(signal.SIGINT)  # ffmpeg finalizes the current mp4 on SIGINT
            try:
                await asyncio.wait_for(p.wait(), 15)
            except TimeoutError:
                p.kill()
        await task

    def full(self, sess):
        return sess["videos"] and sess["done"] >= sess["videos"]

    async def _finish_file(self, path, sess):
        if self.full(sess):  # the few seconds ffmpeg writes between the last video and the stop
            path.unlink(missing_ok=True)
            return
        sess["done"] += 1
        self.fails = 0
        r = await register(path, sess["name"], sess["folder_id"])
        total = f"/{sess['videos']}" if sess["videos"] else ""
        log(f"Xong video {sess['done']}{total}: {path.name} ({r['size'] / 1e6:.1f} MB, {mmss(r['duration'] or 0)})"
            + (f" → đã lưu vào {path.parent}" if r["status"] == "saved" else ""))
        enqueue(path.name)
        if self.full(sess) and not self.stop_event.is_set():
            log(f"Đã đủ {sess['videos']} video: {sess['name']}")
            if sess["kind"] == "slot":
                self.skip_until = sess["end"]  # don't restart this slot occurrence
            self.stop_event.set()
            if self.proc and self.proc.returncode is None:
                self.proc.send_signal(signal.SIGINT)

    async def _run(self, sess):
        plan = f"{sess['videos']} video" if sess["videos"] else "không giới hạn số video"
        until = f", đến {datetime.fromtimestamp(sess['end'], LONDON):%H:%M} London" if sess["end"] else ", đến khi bấm Dừng"
        log(f"Bắt đầu ghi: {sess['name']} — {plan}, mỗi video {mmss(sess['seg'])}{until}")
        log(f"Lưu vào thư mục: {sess['dir']}" + (" (rồi tải lên Google Drive)" if CFG["storage"] == "gdrive" else ""))
        self.fails = 0
        await refresh_url("trước khung giờ")  # on failure keep the old URL and try anyway
        try:
            while not self.stop_event.is_set():
                if not tool("ffmpeg"):
                    self.status, self.error = "error", "Chưa cài ffmpeg — bấm 'Cài phần phụ thuộc' ở mục Cài đặt ban đầu"
                    tail = ""
                else:
                    tail = await self._ffmpeg_once(sess)
                    if self.stop_event.is_set():
                        break
                    self.fails += 1
                    self.status = "error"
                    self.error = f"ffmpeg dừng (lần {self.fails} liên tiếp): {tail or 'mất kết nối'}"
                log(self.error + " — thử lại sau 5 giây", "error")
                if self.fails >= 3:
                    self.fails = 0
                    await refresh_url("ffmpeg lỗi 3 lần liền")
                try:
                    await asyncio.wait_for(self.stop_event.wait(), 5)
                except TimeoutError:
                    pass
        finally:
            out = Path(sess["dir"])
            for p in sorted(out.glob("*.mp4")) if out.is_dir() else []:  # last segment may not reach the list on stop
                if p.name not in FILES and p.stat().st_mtime >= sess["started"]:
                    await self._finish_file(p, sess)
            self.proc = self.session = self.task = None
            self.status, self.error = "idle", ""
            log(f"Đã dừng ghi: {sess['name']} — ghi được {sess['done']} video")
            asyncio.create_task(sweep())

    async def _ffmpeg_once(self, sess):
        out = Path(sess["dir"])
        out.mkdir(parents=True, exist_ok=True)
        args = [tool("ffmpeg"), "-hide_banner", "-nostdin", "-loglevel", "warning", "-stats", "-rw_timeout", "15000000",
                "-headers", f"Referer: {REFERER}\r\n", "-user_agent", UA, "-i", CFG["stream_url"],
                "-c", "copy", "-f", "segment", "-segment_time", str(sess["seg"]), "-reset_timestamps", "1",
                "-strftime", "1", "-segment_list", "pipe:1", "-segment_list_type", "flat",
                f"{CFG['prefix']}_%Y%m%d_%H%M%S.mp4"]
        log("Đang mở luồng bằng ffmpeg…")
        try:
            self.proc = p = await asyncio.create_subprocess_exec(*args, cwd=out, env={**os.environ, "TZ": "Europe/London"},
                                                                 stdout=PIPE, stderr=PIPE)
        except OSError as e:
            return str(e)
        self.media_time = self.seg_mark = 0.0
        tail: deque = deque(maxlen=5)
        last = [time.monotonic()]

        async def read_list():
            while line := await p.stdout.readline():
                name = line.decode().strip()
                if name and (out / name).exists():
                    self.seg_mark = self.media_time
                    await self._finish_file(out / name, sess)

        async def read_err():
            while chunk := await p.stderr.read(4096):
                last[0] = time.monotonic()
                text = chunk.decode(errors="replace")
                if times := re.findall(r"time=(\d+):(\d+):([\d.]+)", text):
                    h, m, s = times[-1]
                    self.media_time = int(h) * 3600 + int(m) * 60 + float(s)
                    if self.status != "recording":
                        self.status, self.error = "recording", ""
                        log(f"Đã kết nối luồng, đang ghi video {sess['done'] + 1}")
                if sp := re.findall(r"speed=\s*([\d.]+x)", text):
                    self.speed = sp[-1]
                for ln in re.split(r"[\r\n]+", text):
                    if ln.strip() and not ln.lstrip().startswith(("frame=", "size=")):
                        tail.append(ln.strip())

        async def watchdog():
            # shortcut: stall = no ffmpeg stderr output for 60s; upgrade to checking file growth if it misfires
            while p.returncode is None:
                await asyncio.sleep(5)
                if p.returncode is None and time.monotonic() - last[0] > 60:
                    log("Không nhận được dữ liệu 60 giây, khởi động lại ffmpeg", "warn")
                    p.terminate()
                    return

        wd = asyncio.create_task(watchdog())
        await asyncio.gather(read_list(), read_err())
        await p.wait()
        wd.cancel()
        return " | ".join(tail)[-400:]

    def progress(self):
        s = self.session
        if not s:
            return None
        out = Path(s["dir"])
        cur = max(out.glob("*.mp4"), key=lambda p: p.stat().st_mtime, default=None) if out.is_dir() else None
        size = cur.stat().st_size if cur and time.time() - cur.stat().st_mtime < 15 else 0
        total = s["videos"] or (math.ceil((s["end"] - s["started"]) / s["seg"]) if s["end"] else None)
        return {"index": min(s["done"] + 1, total) if s["videos"] else s["done"] + 1, "total": total, "total_exact": bool(s["videos"]), "file_time": max(0, self.media_time - self.seg_mark),
                "seg": s["seg"], "speed": self.speed, "size": size}


rec = Recorder()


async def scheduler():
    while True:
        try:
            now = time.time()
            s = rec.session
            if s and s["end"] and now >= s["end"]:
                log(f"Hết giờ: {s['name']}")
                await rec.stop()
            elif not rec.task and CFG["schedule_enabled"] and now >= rec.skip_until:
                act = active_slot(CFG["slots"], now)
                if act:
                    slot, st, en = act
                    rec.start({"kind": "slot", "name": slot["name"], "folder_id": slot["folder_id"], "end": en, "dir": str(slot_dir(slot["name"])),
                               "videos": slot["videos"], "seg": seg_len(slot["segment_seconds"], slot["videos"], en - st)})
        except Exception as e:
            log(f"Lỗi bộ lập lịch: {e!r}", "error")
        await asyncio.sleep(2)


# ---------- cài đặt ban đầu: phần phụ thuộc + Google Drive ----------
INSTALL = {"running": False, "step": "", "pct": None, "error": ""}
AUTH = {"running": False, "url": "", "error": "", "proc": None}
ARCH = {"x86_64": ("amd64", "linux64"), "aarch64": ("arm64", "linuxarm64")}


async def run(*args, timeout=60):
    p = await asyncio.create_subprocess_exec(*args, stdout=PIPE, stderr=STDOUT)
    try:
        out, _ = await asyncio.wait_for(p.communicate(), timeout)
    except TimeoutError:
        p.kill()
        return 1, "Quá thời gian chờ"
    return p.returncode, out.decode(errors="replace").strip()


async def gdrive_connected():
    if not tool("rclone"):
        return False
    code, out = await run(tool("rclone"), "config", "dump")
    try:
        return code == 0 and bool(json.loads(out).get("gdrive", {}).get("token"))
    except ValueError:
        return False


async def download(url, dest: Path, label):
    async with http.stream("GET", url, timeout=httpx.Timeout(30, read=120)) as r:
        r.raise_for_status()
        total, got, shown = int(r.headers.get("content-length", 0)), 0, -1
        with dest.open("wb") as f:
            async for chunk in r.aiter_bytes(1 << 20):
                f.write(chunk)
                got += len(chunk)
                if total:
                    INSTALL["pct"] = got * 100 // total
                    if INSTALL["pct"] // 10 > shown:
                        shown = INSTALL["pct"] // 10
                        log(f"{label}: {INSTALL['pct']}% ({got / 1e6:.0f}/{total / 1e6:.0f} MB)")


def _extract(archive: Path, wanted: dict):
    """Copy archive members whose path ends with each key to BIN/<value>, atomically."""
    if archive.suffix == ".zip":
        z = zipfile.ZipFile(archive)
        members = {n: (lambda n=n: z.open(n)) for n in z.namelist()}
    else:
        t = tarfile.open(archive)
        members = {m.name: (lambda m=m: t.extractfile(m)) for m in t.getmembers() if m.isfile()}
    for suffix, name in wanted.items():
        src = next((n for n in members if n.endswith(suffix)), None)
        if not src:
            raise RuntimeError(f"không thấy {suffix} trong gói tải về")
        tmp = BIN / f".{name}.new"
        with members[src]() as fin, tmp.open("wb") as fout:
            shutil.copyfileobj(fin, fout)
        tmp.chmod(0o755)
        tmp.replace(BIN / name)  # replacing a running binary is safe on Linux


async def install_deps(force):
    arch = ARCH.get(platform.machine())
    INSTALL.update(running=True, error="", pct=None)
    try:
        if not arch:
            raise RuntimeError(f"chưa hỗ trợ kiến trúc máy {platform.machine()}")
        BIN.mkdir(exist_ok=True)
        jobs = []
        if force or not tool("rclone"):
            jobs.append(("rclone", f"https://downloads.rclone.org/rclone-current-linux-{arch[0]}.zip", {"/rclone": "rclone"}))
        if force or not (tool("ffmpeg") and tool("ffprobe")):
            # BtbN builds link glibc dynamically, so DNS works (fully static builds can't resolve hostnames)
            jobs.append(("ffmpeg + ffprobe", f"https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-{arch[1]}-gpl.tar.xz",
                         {"/bin/ffmpeg": "ffmpeg", "/bin/ffprobe": "ffprobe"}))
        if not jobs:
            log("Đã có đủ ffmpeg, ffprobe, rclone — không cần cài")
        for label, url, wanted in jobs:
            archive = BIN / (".download.zip" if url.endswith(".zip") else ".download.tar.xz")
            INSTALL.update(step=f"Đang tải {label}", pct=0)
            log(f"Bắt đầu tải {label} từ {urlparse(url).hostname}")
            await download(url, archive, f"Tải {label}")
            INSTALL.update(step=f"Đang giải nén {label}", pct=None)
            log(f"Đang giải nén {label}…")
            await asyncio.to_thread(_extract, archive, wanted)
            archive.unlink(missing_ok=True)
            for name in wanted.values():
                code, out = await run(str(BIN / name), "version" if name == "rclone" else "-version")
                if code != 0:
                    raise RuntimeError(f"{name} cài xong nhưng không chạy được: {out[-200:]}")
                log(f"Đã cài {name}: {out.splitlines()[0]}")
        INSTALL["step"] = "Xong"
    except Exception as e:
        INSTALL.update(step="Lỗi", error=str(e) or repr(e))
        log(f"Cài phần phụ thuộc lỗi: {INSTALL['error']}", "error")
    finally:
        INSTALL.update(running=False, pct=None)


async def gdrive_connect():
    rc = tool("rclone")
    AUTH.update(running=True, url="", error="")
    try:
        if await gdrive_connected():  # reconnect: refresh the token, keep the working remote if cancelled
            cmd = [rc, "config", "update", "gdrive", "scope=drive"]
        else:
            await run(rc, "config", "delete", "gdrive")  # clear a half-finished remote from an earlier attempt
            cmd = [rc, "config", "create", "gdrive", "drive", "scope=drive"]
        log("Đang chuẩn bị đăng nhập Google Drive…")
        p = AUTH["proc"] = await asyncio.create_subprocess_exec(*cmd, stdout=PIPE, stderr=STDOUT)

        async def read():
            while line := await p.stdout.readline():
                if m := re.search(r"(http://127\.0\.0\.1:53682/auth\?state=\S+)", line.decode(errors="replace")):
                    AUTH["url"] = m[1]
                    log("Hãy bấm link 'Đăng nhập Google' trên trang để cho phép rclone truy cập Drive")
            await p.wait()

        try:
            await asyncio.wait_for(read(), 600)
        except TimeoutError:
            p.kill()
            raise RuntimeError("quá 10 phút chưa đăng nhập xong")
        if not await gdrive_connected():
            raise RuntimeError("đã huỷ hoặc chưa cho phép rclone truy cập Drive")
        log("Đã kết nối Google Drive (remote 'gdrive')")
        await sweep()  # files that failed for lack of a remote can go up now
    except Exception as e:
        AUTH["error"] = str(e)
        log(f"Kết nối Google Drive lỗi: {e}", "error")
        if not await gdrive_connected():
            await run(rc, "config", "delete", "gdrive")
    finally:
        AUTH.update(running=False, url="", proc=None)


# ---------- API ----------
@asynccontextmanager
async def lifespan(_):
    for r in FILES.values():  # an upload cut off by a restart is retried
        if r["status"] == "uploading":
            r["status"] = "pending"
    save_db()
    if not CFG["stream_url"] or not (await check_url(CFG["stream_url"]))[0]:
        await refresh_url("khởi động")
    try:
        ensure_slot_dirs()
    except OSError as e:
        log(f"Không tạo được thư mục khung giờ trong {CFG['output_dir']}: {e}", "error")
    tasks = [asyncio.create_task(scheduler()), asyncio.create_task(upload_worker())]
    await sweep()
    log("App đã khởi động")
    yield
    if AUTH["proc"] and AUTH["proc"].returncode is None:
        AUTH["proc"].kill()
    await rec.stop()
    for t in tasks:
        t.cancel()


app = FastAPI(lifespan=lifespan)
HM = r"^([01]\d|2[0-3]):[0-5]\d$"
FOLDER = r"^([A-Za-z0-9_-]{10,100})?$"  # optional: only needed in Google Drive mode


class SlotIn(BaseModel):
    name: str = Field(min_length=1, max_length=60)
    start: str = Field(pattern=HM)
    end: str = Field(pattern=HM)
    folder_id: str = Field("", pattern=FOLDER)
    enabled: bool = True
    repeat: Literal["daily", "weekly", "once"] = "daily"
    days: list[int] = Field(default_factory=lambda: list(range(7)))
    date: str = ""
    tz: Literal["Europe/London", "Asia/Ho_Chi_Minh"] = "Europe/London"
    videos: int = Field(0, ge=0, le=1000)
    segment_seconds: int = Field(0, ge=0, le=36000)


class SettingsIn(BaseModel):
    segment_seconds: int = Field(ge=10, le=3600)
    prefix: str = Field(pattern=r"^[A-Za-z0-9_-]{1,40}$")
    delete_after_upload: bool


class StorageIn(BaseModel):
    mode: Literal["local", "gdrive"]
    root: str = Field(min_length=1)


class MkdirIn(BaseModel):
    path: str
    name: str = Field(min_length=1, max_length=100)


class UrlIn(BaseModel):
    url: str


class StartIn(BaseModel):
    slot_id: str
    minutes: float = Field(0, ge=0, le=1440)
    videos: int = Field(0, ge=0, le=1000)
    segment_seconds: int = Field(0, ge=0, le=36000)


@app.get("/")
async def index():
    return FileResponse(BASE / "index.html")


@app.get("/api/status")
async def status():
    now = time.time()
    act, nxt = active_slot(CFG["slots"], now), next_slot(CFG["slots"], now)
    files = sorted(FILES.values(), key=lambda r: r["created"], reverse=True)
    for r in files:
        r["exists"] = (Path(r["dir"]) / r["name"]).exists()
    return {"status": rec.status if rec.task else "idle", "error": rec.error, "url_error": rec.url_error,
            "session": rec.session, "progress": rec.progress(),
            "active": act and {"name": act[0]["name"], "start": act[1], "end": act[2]},
            "next": nxt and {"name": nxt[0]["name"], "start": nxt[1], "end": nxt[2]},
            "slot_next": {s["id"]: slot_next(s, now) for s in CFG["slots"]},
            "skip_until": rec.skip_until if rec.skip_until > now else None,
            "slot_dirs": [{"id": s["id"], "path": str(d), "exists": d.is_dir(), "count": len(list(d.glob("*.mp4"))) if d.is_dir() else 0}
                          for s in CFG["slots"] for d in [slot_dir(s["name"])]],
            "install": INSTALL, "auth": {k: v for k, v in AUTH.items() if k != "proc"},
            "stream_td": url_td(CFG["stream_url"]) if CFG["stream_url"] else None, "config": CFG, "files": files}


@app.get("/api/setup")
async def setup():
    tools = {}
    for name in ("ffmpeg", "ffprobe", "rclone"):
        path = tool(name)
        ver = ""
        if path:
            code, out = await run(path, "version" if name == "rclone" else "-version", timeout=10)
            ver = out.splitlines()[0] if code == 0 and out else "không chạy được"
        tools[name] = {"path": path, "version": ver}
    return {"tools": tools, "gdrive": await gdrive_connected(), "arch": platform.machine()}


@app.post("/api/setup/install")
async def setup_install(force: bool = False):
    if INSTALL["running"]:
        raise HTTPException(409, "Đang cài rồi")
    asyncio.create_task(install_deps(force))
    return {"ok": True}


@app.post("/api/setup/gdrive")
async def setup_gdrive():
    if not tool("rclone"):
        raise HTTPException(400, "Chưa có rclone — bấm 'Cài phần phụ thuộc' trước")
    if AUTH["running"]:
        raise HTTPException(409, "Đang chờ đăng nhập")
    asyncio.create_task(gdrive_connect())
    return {"ok": True}


@app.post("/api/setup/gdrive/cancel")
async def setup_gdrive_cancel():
    if AUTH["proc"] and AUTH["proc"].returncode is None:
        AUTH["proc"].kill()
    return {"ok": True}


@app.post("/api/setup/folders")
async def setup_folders():
    if not tool("rclone"):
        raise HTTPException(400, "Chưa có rclone")

    async def one(s):
        code, out = await run(tool("rclone"), "lsf", "gdrive:", "--drive-root-folder-id", s["folder_id"], "--max-depth", "1")
        lines = [ln for ln in out.splitlines() if "Config file" not in ln]
        msg = f"OK, đang có {len(lines)} mục" if code == 0 else (lines[-1] if lines else "lỗi")[-200:]
        log(f"Kiểm tra thư mục '{s['name']}': {msg}", "info" if code == 0 else "error")
        return {"id": s["id"], "name": s["name"], "ok": code == 0, "msg": msg}

    return await asyncio.gather(*(one(s) for s in CFG["slots"]))


@app.post("/api/record/start")
async def record_start(body: StartIn):
    slot = next((s for s in CFG["slots"] if s["id"] == body.slot_id), None)
    if not slot:
        raise HTTPException(404, "Không có khung giờ này")
    dur = body.minutes * 60
    sess = {"kind": "manual", "name": f"Ghi tay → {slot['name']}", "folder_id": slot["folder_id"], "dir": str(slot_dir(slot["name"])),
            "end": time.time() + dur if dur else None, "videos": body.videos, "seg": seg_len(body.segment_seconds, body.videos, dur)}
    if not rec.start(sess):
        raise HTTPException(409, "Đang ghi rồi")
    return {"ok": True}


@app.post("/api/record/stop")
async def record_stop():
    act = active_slot(CFG["slots"], time.time())
    if act:  # don't let the scheduler restart the slot the user just stopped
        rec.skip_until = act[2]
    await rec.stop()
    return {"ok": True}


@app.put("/api/schedule")
async def schedule_toggle(body: dict):
    CFG["schedule_enabled"] = bool(body.get("enabled"))
    if CFG["schedule_enabled"]:
        rec.skip_until = 0
    save_cfg()
    log("Đã bật lịch" if CFG["schedule_enabled"] else "Đã tạm tắt lịch")
    return {"ok": True}


def _check_slot(body: SlotIn):
    if body.start == body.end:
        raise HTTPException(400, "Giờ bắt đầu và kết thúc phải khác nhau")
    if body.repeat == "weekly" and not body.days:
        raise HTTPException(400, "Chọn ít nhất một thứ trong tuần")
    if any(d not in range(7) for d in body.days):
        raise HTTPException(400, "Thứ không hợp lệ")
    if body.repeat == "once":
        try:
            date.fromisoformat(body.date)
        except ValueError:
            raise HTTPException(400, "Chọn ngày cho lịch một lần")
    if 0 < body.segment_seconds < 10:
        raise HTTPException(400, "Mỗi video phải dài ít nhất 10 giây")


@app.post("/api/slots")
async def slot_add(body: SlotIn):
    _check_slot(body)
    CFG["slots"].append({"id": uuid.uuid4().hex[:8], **body.model_dump()})
    save_cfg()
    slot_dir(body.name).mkdir(parents=True, exist_ok=True)
    log(f"Thêm khung giờ {body.name} {body.start}–{body.end}")
    return {"ok": True}


@app.put("/api/slots/{sid}")
async def slot_edit(sid: str, body: SlotIn):
    _check_slot(body)
    slot = next((s for s in CFG["slots"] if s["id"] == sid), None)
    if not slot:
        raise HTTPException(404)
    slot.update(body.model_dump())
    save_cfg()
    slot_dir(body.name).mkdir(parents=True, exist_ok=True)
    log(f"Sửa khung giờ {body.name} {body.start}–{body.end}")
    return {"ok": True}


@app.delete("/api/slots/{sid}")
async def slot_delete(sid: str):
    slot = next((s for s in CFG["slots"] if s["id"] == sid), None)
    if not slot:
        raise HTTPException(404)
    CFG["slots"].remove(slot)
    save_cfg()
    log(f"Đã xoá khung giờ {slot['name']}")
    return {"ok": True}


@app.put("/api/settings")
async def settings(body: SettingsIn):
    CFG.update(body.model_dump())
    save_cfg()
    log("Đã lưu cài đặt (áp dụng từ lần ghi tiếp theo)")
    return {"ok": True}


@app.put("/api/storage")
async def storage(body: StorageIn):
    d = Path(body.root).expanduser()
    if not d.is_absolute():
        raise HTTPException(400, "Thư mục gốc phải là đường dẫn tuyệt đối")
    try:
        d.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise HTTPException(400, f"Không tạo được thư mục: {e}")
    if not os.access(d, os.W_OK):
        raise HTTPException(400, "Không có quyền ghi vào thư mục này")
    old_root = CFG["output_dir"]
    CFG.update(storage=body.mode, output_dir=str(d))
    try:
        ensure_slot_dirs()
    except OSError as e:
        CFG["output_dir"] = old_root
        raise HTTPException(400, f"Không tạo được thư mục khung giờ: {e}")
    save_cfg()
    later = " (lần ghi đang chạy vẫn lưu chỗ cũ)" if rec.task else ""
    log(f"Nơi lưu: {'trên máy' if body.mode == 'local' else 'Google Drive'} · thư mục gốc {d}{later}")
    if body.mode == "gdrive":
        await sweep()
    return {"ok": True}


@app.get("/api/fs")
async def fs_list(path: str = ""):
    """Folder browser for the root picker: a web page cannot learn a local path by itself."""
    p = Path(path or CFG["output_dir"]).expanduser()
    if not p.is_absolute():
        raise HTTPException(400, "Cần đường dẫn tuyệt đối, ví dụ /home/ten/Videos")
    while not p.is_dir() and p != p.parent:  # typed a missing path: show its nearest existing parent
        p = p.parent
    try:
        dirs = sorted((c.name for c in p.iterdir() if not c.name.startswith(".") and c.is_dir()), key=str.lower)
    except PermissionError:
        raise HTTPException(403, "Không có quyền đọc thư mục này")
    return {"path": str(p), "parent": str(p.parent) if p != p.parent else None, "dirs": dirs,
            "home": str(Path.home()), "writable": os.access(p, os.W_OK)}


@app.post("/api/fs/mkdir")
async def fs_mkdir(body: MkdirIn):
    name = body.name.strip()
    if name in (".", "..") or "/" in name or "\x00" in name:
        raise HTTPException(400, "Tên thư mục không hợp lệ")
    p = Path(body.path) / name
    try:
        p.mkdir(exist_ok=True)
    except OSError as e:
        raise HTTPException(400, f"Không tạo được thư mục: {e}")
    return {"path": str(p)}


@app.post("/api/stream/check")
async def stream_check(body: UrlIn):
    ok, msg = await check_url(body.url.strip())
    return {"ok": ok, "msg": msg}


@app.put("/api/stream")
async def stream_set(body: UrlIn):
    url = body.url.strip()
    if not is_earthcam(url):
        raise HTTPException(400, "Địa chỉ phải là https://…earthcam.com/…")
    CFG["stream_url"] = url
    save_cfg()
    rec.url_error = ""
    log(f"Đã lưu địa chỉ luồng dán tay, td={url_td(url)}")
    return {"ok": True}


@app.post("/api/stream/refresh")
async def stream_refresh():
    return {"ok": await refresh_url("bấm tay"), "msg": rec.url_error}


def _file(name):
    r = FILES.get(name)
    if not r:
        raise HTTPException(404, "Không có file này")
    return r, Path(r["dir"]) / r["name"]


@app.get("/api/files/{name}")
async def file_get(name: str):
    _, p = _file(name)
    if not p.exists():
        raise HTTPException(404, "File đã bị xoá trên máy")
    return FileResponse(p, media_type="video/mp4")


@app.post("/api/files/{name}/retry")
async def file_retry(name: str):
    r, _ = _file(name)
    if r["status"] == "uploading":
        raise HTTPException(409, "Đang tải lên")
    r.update(status="pending", error="")
    save_db()
    enqueue(name)
    return {"ok": True}


@app.delete("/api/files/{name}")
async def file_delete(name: str):
    r, p = _file(name)
    if r["status"] == "uploading":
        raise HTTPException(409, "Đang tải lên, chờ xong rồi xoá")
    p.unlink(missing_ok=True)
    if r["status"] == "done":
        r["deleted"] = True
    else:
        del FILES[name]
    save_db()
    log(f"Đã xoá trên máy: {name}")
    return {"ok": True}


@app.get("/api/events")
async def events(request: Request):
    q: asyncio.Queue = asyncio.Queue()
    SUBS.add(q)

    async def gen():
        try:
            for e in list(LOGS):
                yield f"data: {json.dumps(e, ensure_ascii=False)}\n\n"
            while not await request.is_disconnected():
                try:
                    e = await asyncio.wait_for(q.get(), 15)
                    yield f"data: {json.dumps(e, ensure_ascii=False)}\n\n"
                except TimeoutError:
                    yield ": ping\n\n"
        finally:
            SUBS.discard(q)

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.get("/proxy")
async def proxy(u: str):
    """Live view: hls.js can't set Referer, so the backend fetches and rewrites playlist URLs to come back here."""
    if not is_earthcam(u):
        raise HTTPException(400, "Chỉ proxy earthcam.com")
    try:
        r = await http.get(u, headers=HDRS)
    except httpx.HTTPError as e:
        raise HTTPException(502, repr(e))
    ct = r.headers.get("content-type", "")
    if ".m3u8" in urlparse(u).path or "mpegurl" in ct.lower():
        base = str(r.url)
        wrap = lambda x: "/proxy?u=" + quote(urljoin(base, x), safe="")
        lines = [wrap(ln) if ln and not ln.startswith("#") else re.sub(r'URI="([^"]+)"', lambda m: f'URI="{wrap(m[1])}"', ln)
                 for ln in r.text.splitlines()]
        return Response("\n".join(lines), r.status_code, media_type="application/vnd.apple.mpegurl")
    return Response(r.content, r.status_code, media_type=ct or "video/mp2t")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8080, timeout_graceful_shutdown=5)  # localhost only
