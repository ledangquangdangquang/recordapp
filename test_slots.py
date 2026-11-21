# Run: .venv/bin/python test_slots.py
from datetime import datetime
from zoneinfo import ZoneInfo
from app import LONDON, SLOT_DEFAULTS, active_slot, next_slot, safe_name, seg_len, slot_for_filename, slot_next

VN = ZoneInfo("Asia/Ho_Chi_Minh")
ts = lambda *a, tz=LONDON: datetime(*a, tzinfo=tz).timestamp()
mk = lambda i, st, en, **kw: {"id": i, "name": i, "start": st, "end": en, "folder_id": "x", "enabled": True, **SLOT_DEFAULTS, **kw}
S = [mk("a", "07:00", "08:00"), mk("n", "23:30", "00:30")]

assert active_slot(S, ts(2026, 7, 1, 7, 30))[0]["id"] == "a"          # summer (BST)
assert active_slot(S, ts(2026, 12, 1, 7, 0))[0]["id"] == "a"          # winter (GMT), start inclusive
assert active_slot(S, ts(2026, 12, 1, 8, 0)) is None                  # end exclusive
assert active_slot(S, ts(2026, 12, 2, 0, 15))[0]["id"] == "n"         # crosses midnight
s, st, en = next_slot(S, ts(2026, 3, 28, 9, 0))                       # day before clocks go forward
assert s["id"] == "n" and en - st == 3600
s, st, _ = next_slot(S, ts(2026, 3, 29, 1, 0))                        # 01:00 GMT == 02:00 BST on DST day
assert s["id"] == "a" and st == ts(2026, 3, 29, 7, 0) and st - ts(2026, 3, 29, 1, 0) == 5 * 3600

# weekly: Mon+Wed only (2026-12-07 is a Monday)
w = [mk("w", "09:00", "10:00", repeat="weekly", days=[0, 2])]
assert active_slot(w, ts(2026, 12, 7, 9, 30)) and not active_slot(w, ts(2026, 12, 8, 9, 30))
assert slot_next(w[0], ts(2026, 12, 7, 11, 0))[0] == ts(2026, 12, 9, 9, 0)
# once, far in the future, in Vietnam time
o = mk("o", "20:00", "20:30", repeat="once", date="2027-02-01", tz="Asia/Ho_Chi_Minh")
assert slot_next(o, ts(2026, 12, 1, 0, 0)) == (ts(2027, 2, 1, 20, 0, tz=VN), ts(2027, 2, 1, 20, 30, tz=VN))
assert active_slot([o], ts(2027, 2, 1, 13, 10))                       # 20:10 VN == 13:10 London (GMT)
assert slot_next(o, ts(2027, 2, 2, 0, 0)) is None                     # past one-off never comes back

assert seg_len(0, 30, 3600) == 120 and seg_len(300, 30, 3600) == 300 and seg_len(0, 0, 3600) == 120
assert slot_for_filename("abbeyroad_20261201_070512.mp4")["start"] == "07:00"
assert slot_for_filename("abbeyroad_20261201_100512.mp4") is None
assert [safe_name(n) for n in ["Sáng 7-8", "a/b", "../x", ".", ""]] == ["Sáng 7-8", "a-b", "-x", "khac", "khac"]  # no escaping the root
print("ok")
