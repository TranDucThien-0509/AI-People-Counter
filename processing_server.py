import os

import json
import base64
import time
import cv2
import numpy as np
import redis
from pymongo import MongoClient
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import contextlib
import threading
import asyncio

# FIX PyTorch 2.6+: patch torch.load trước khi ultralytics dùng nó
import torch
_original_torch_load = torch.load
def _patched_torch_load(f, *args, **kwargs):
    kwargs.setdefault("weights_only", False)
    return _original_torch_load(f, *args, **kwargs)
torch.load = _patched_torch_load

from ultralytics import YOLO

# Khởi tạo mô hình YOLOv8
model = YOLO('yolov8n.pt')

# Kết nối hạ tầng — đọc host từ env để chạy được cả local lẫn Docker
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")

# Retry Redis
for attempt in range(15):
    try:
        r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
        r.ping()
        print(f"✅ Redis connected ({REDIS_HOST})")
        break
    except redis.ConnectionError:
        print(f"⏳ Redis chưa sẵn sàng ({attempt+1}/15)...")
        time.sleep(3)
else:
    raise RuntimeError("Không thể kết nối Redis")

# FIX 1: Giới hạn connection pool để tránh MongoDB bị flood connection
mongo_client = MongoClient(
    f'mongodb://{MONGO_HOST}:27017/',
    maxPoolSize=5,
    serverSelectionTimeoutMS=5000
)
db = mongo_client['camera_analytics']
collection = db['people_counting']

STREAM_NAME = "camera:stream"
GROUP_NAME = "processing_group"
CONSUMER_NAME = "processor_1"

# FIX 2: Dùng id="$" thay vì id="0"
# id="0" → đọc lại TẤT CẢ message từ đầu stream mỗi khi group bị tạo lại
# id="$" → chỉ đọc message MỚI phát sinh sau thời điểm tạo group
try:
    r.xgroup_create(STREAM_NAME, GROUP_NAME, id="$", mkstream=True)
    print(f"✅ Tạo Consumer Group '{GROUP_NAME}' thành công.")
except redis.exceptions.ResponseError:
    print(f"ℹ️ Consumer Group '{GROUP_NAME}' đã tồn tại, tiếp tục.")

is_running = True

def consume_and_process():
    """Chạy trong background thread — tách biệt hoàn toàn khỏi async event loop."""
    print("🧠 Processing Server bắt đầu quét dữ liệu từ Redis Streams...")

    # FIX 3: Đếm số lần liên tiếp không có message để tránh spam log
    idle_streak = 0
    IDLE_LOG_THRESHOLD = 10  # Log mỗi 10 lần idle (~10 giây)

    while is_running:
        # '>' = chỉ đọc message mới chưa ai xử lý
        # block=1000ms: chờ tối đa 1s nếu stream rỗng (không busy-wait)
        messages = r.xreadgroup(
            GROUP_NAME, CONSUMER_NAME,
            {STREAM_NAME: ">"},
            count=1, block=1000
        )

        if not messages:
            # FIX 4: Không spam log khi stream rỗng, chỉ log định kỳ
            idle_streak += 1
            if idle_streak % IDLE_LOG_THRESHOLD == 1:
                print("⏸️ Đang chờ dữ liệu mới từ Redis Stream...")
            continue

        # Reset idle khi có message
        idle_streak = 0

        for stream, payload_list in messages:
            for message_id, message_data in payload_list:
                try:
                    data = json.loads(message_data['data'])
                    frame_id = data['frame_id']
                    timestamp = data['timestamp']

                    # Decode ảnh từ Base64 → numpy array
                    img_bytes = base64.b64decode(data['image'])
                    np_arr = np.frombuffer(img_bytes, dtype=np.uint8)
                    frame = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

                    # Dự đoán bằng YOLOv8 (chỉ class 0 = person)
                    results = model(frame, verbose=False, device='cpu', classes=[0])[0]

                    bounding_boxes = []
                    person_count = 0

                    for box in results.boxes:
                        person_count += 1
                        coords = box.xyxy[0].tolist()
                        confidence = float(box.conf[0])
                        bounding_boxes.append({
                            'box': [round(x, 2) for x in coords],
                            'confidence': round(confidence, 2)
                        })

                    # Lưu vào MongoDB
                    result_payload = {
                        'frame_id': frame_id,
                        'timestamp': timestamp,
                        'person_count': person_count,
                        'bounding_boxes': bounding_boxes
                    }
                    collection.insert_one(result_payload)
                    print(f"💾 Frame {frame_id}: {person_count} người.")

                    # Acknowledge — báo Redis đã xử lý xong
                    r.xack(STREAM_NAME, GROUP_NAME, message_id)

                except Exception as e:
                    # FIX 5: Bắt lỗi từng message để 1 frame lỗi không crash cả thread
                    print(f"❌ Lỗi xử lý message {message_id}: {e}")
                    r.xack(STREAM_NAME, GROUP_NAME, message_id)  # ACK để không xử lý lại

    print("🛑 Consumer thread đã dừng.")

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    t = threading.Thread(target=consume_and_process, daemon=True)
    t.start()
    yield
    global is_running
    is_running = False

# Khởi tạo FastAPI
app = FastAPI(lifespan=lifespan, title="AI Processing Server")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def index():
    return {"status": "Running", "info": "YOLOv8 Processing Server"}

@app.get("/latest")
def get_latest_count():
    latest_record = collection.find_one(sort=[('frame_id', -1)])
    if latest_record:
        latest_record.pop('_id')
        return latest_record
    return {"message": "Chưa có dữ liệu"}

# FIX 6: Thêm endpoint xem tổng số frame đã xử lý
@app.get("/stats")
def get_stats():
    total = collection.count_documents({})
    return {"total_frames_processed": total}

@app.get("/history")
def get_history(limit: int = 60):
    """Trả về lịch sử person_count của N frame gần nhất."""
    records = list(collection.find(
        {}, {'_id': 0, 'frame_id': 1, 'person_count': 1, 'timestamp': 1}
    ).sort('frame_id', -1).limit(limit))
    records.reverse()
    return records

@app.get("/stats/stream")
async def stats_stream():
    """Server-Sent Events — push dữ liệu mới nhất mỗi giây."""
    async def event_generator():
        last_frame_id = -1
        while True:
            try:
                latest = collection.find_one(sort=[('frame_id', -1)])
                if latest and latest.get('frame_id') != last_frame_id:
                    last_frame_id = latest['frame_id']
                    latest.pop('_id', None)
                    total = collection.count_documents({})
                    latest['total_frames_processed'] = total
                    yield f"data: {json.dumps(latest)}\n\n"
            except Exception as e:
                yield f"data: {json.dumps({'error': str(e)})}\n\n"
            await asyncio.sleep(1)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        }
    )