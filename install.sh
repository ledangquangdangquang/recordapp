#!/usr/bin/env bash
# Cài app trên Ubuntu/Debian (chỉ cần apt). Chạy:  bash install.sh
# ffmpeg và rclone thì cài bằng nút "Cài phần phụ thuộc" trên web, không cần sudo.
set -euo pipefail
cd "$(dirname "$0")"
DIR=$PWD

if ! python3 -c 'import venv, ensurepip' 2>/dev/null; then
  echo ">> Cần python3-venv, cài bằng apt (sẽ hỏi mật khẩu sudo)…"
  sudo apt-get update && sudo apt-get install -y python3 python3-venv
fi

[ -x .venv/bin/python ] || python3 -m venv .venv
.venv/bin/python -m pip --version >/dev/null 2>&1 || .venv/bin/python -m ensurepip --upgrade >/dev/null
echo ">> Cài thư viện Python…"
.venv/bin/python -m pip install -q --disable-pip-version-check -r requirements.txt

echo ">> Cài dịch vụ tự chạy khi bật máy (systemd user service)…"
mkdir -p ~/.config/systemd/user
cat > ~/.config/systemd/user/recordapp.service <<EOF
[Unit]
Description=Ghi cam EarthCam Abbey Road -> Google Drive

[Service]
WorkingDirectory=$DIR
Environment=PATH=$DIR/bin:$HOME/.local/bin:$HOME/.nix-profile/bin:/usr/local/bin:/usr/bin:/bin
Environment=PYTHONUNBUFFERED=1
ExecStart=$DIR/.venv/bin/python app.py
# SIGINT lets ffmpeg finish the current mp4 cleanly
KillSignal=SIGINT
TimeoutStopSec=30
Restart=always
RestartSec=5

[Install]
WantedBy=default.target
EOF

# linger: the service keeps running after logout and starts at boot without logging in
loginctl enable-linger "$USER" 2>/dev/null || sudo loginctl enable-linger "$USER"
systemctl --user daemon-reload
systemctl --user enable recordapp.service >/dev/null 2>&1
systemctl --user restart recordapp.service
sleep 3
systemctl --user is-active --quiet recordapp.service && echo ">> Xong! Mở http://localhost:8080" \
  || { echo ">> Dịch vụ chưa chạy được, xem log:"; journalctl --user -u recordapp -n 30 --no-pager; exit 1; }
