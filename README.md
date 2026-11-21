# Ghi cam EarthCam Abbey Road

Web app ghi cam EarthCam Abbey Road theo lịch (giờ London hoặc giờ Việt Nam), lưu mỗi khung giờ vào một thư mục riêng trên máy, có thể tải lên Google Drive.

## Cài (Ubuntu/Debian)

```bash
bash install.sh
```

Script cài Python venv bằng apt, cài thư viện và tạo systemd user service tự chạy khi bật máy. Sau đó mở http://localhost:8080 rồi:

1. **Cài phần phụ thuộc**: tải ffmpeg và rclone vào `bin/`, không cần sudo.
2. **Chọn thư mục gốc**: app tạo trong đó một thư mục cho mỗi khung giờ.
3. (Tuỳ chọn) chọn **Google Drive** rồi bấm **Kết nối Google Drive**.

## Quản lý

| Việc | Lệnh |
|---|---|
| Xem log | `journalctl --user -u recordapp -f` |
| Dừng / chạy lại | `systemctl --user stop recordapp` / `systemctl --user restart recordapp` |
| Chạy tay (dừng service trước) | `.venv/bin/python app.py` |
| Kiểm tra lịch | `.venv/bin/python test_slots.py` |

App chỉ nghe ở `127.0.0.1:8080`, không mở ra mạng ngoài.
