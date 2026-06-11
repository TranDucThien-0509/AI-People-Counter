import cv2
import base64
import time
import json
import os
import redis

# 1. Kết nối tới Redis — đọc host từ env để chạy được cả local lẫn Docker
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# Retry loop: đợi Redis sẵn sàng (quan trọng khi dùng Docker depends_on)
for attempt in range(10):
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r.ping()
        print(f"✅ Kết nối tới Redis thành công! ({REDIS_HOST}:{REDIS_PORT})")
        break
    except redis.ConnectionError:
        print(f"⏳ Redis chưa sẵn sàng, thử lại ({attempt+1}/10)...")
        time.sleep(3)
else:
    print("❌ Lỗi: Không thể kết nối tới Redis sau nhiều lần thử.")
    exit(1)

# Tên của Stream trong Redis (tương đương với Topic trong Kafka)
STREAM_NAME = "camera:stream"

# 2. Cấu hình nguồn Video (Thay 'video.mp4' bằng số 0 nếu muốn dùng Webcam)
video_source = 'video/3.mp4'
cap = cv2.VideoCapture(video_source)

if not cap.isOpened():
    print(f"❌ Lỗi: Không thể mở nguồn video: {video_source}")
    exit()

print("🚀 Ingestion Server đang khởi động luồng truyền dữ liệu...")

frame_id = 0

while cap.isOpened():
    ret, frame = cap.read()
    if not ret:
        print("🎯 Đã đọc hết file video hoặc luồng bị ngắt.")
        break
        
    frame_id += 1
    
    # 3. Tối ưu hóa Big Data: Giảm độ phân giải về 640x480 để truyền tải nhẹ và nhanh hơn
    frame = cv2.resize(frame, (640, 480))
    
    # 4. Mã hóa (Encode) khung hình thành chuỗi văn bản Base64
    # Chuyển ảnh OpenCV (numpy array) sang định dạng JPEG trong bộ nhớ
    _, buffer = cv2.imencode('.jpg', frame)
    # Biến byte thành chuỗi chuỗi chuỗi Base64 string để đóng gói JSON
    img_base64 = base64.b64encode(buffer).decode('utf-8')
    
    # 5. Tạo Payload chứa Metadata dữ liệu lớn
    payload = {
        "frame_id": frame_id,
        "timestamp": time.time(),
        "image": img_base64
    }
    
    # 6. Đẩy dữ liệu vào Redis Streams
    # Lệnh xadd() tương đương với việc Producer gửi tin nhắn vào Kafka
    # Dấu '*' nghĩa là cho phép Redis tự động sinh ID tăng dần dựa theo timestamp
    r.xadd(STREAM_NAME, {"data": json.dumps(payload)})
    
    print(f"📤 Đã push Frame {frame_id} vào Redis Stream.")
    
    # Giới hạn tốc độ đọc một chút (~20 FPS) để tránh đẩy quá nhanh khi test local
    time.sleep(0.05)

# Giải phóng tài nguyên
cap.release()
print("🏁 Hoàn thành luồng Ingestion.")