# AI People Counter

A real-time people counting system using **YOLOv8**, **Redis Streams**, **MongoDB**, and **Docker**. Video frames are ingested, streamed through Redis, processed by an AI model, and stored for querying via a REST API.

---

## Architecture

```
[Ingestion Server] ──► [Redis Streams] ──► [Processing Server] ──► [MongoDB]
  (OpenCV + video)                          (YOLOv8 detection)      (REST API)
```

| Component | Role |
|---|---|
| **Ingestion Server** | Reads video frames, encodes to Base64, pushes to Redis Stream |
| **Redis Streams** | Message queue (acts like a lightweight Kafka topic) |
| **Processing Server** | Consumes frames, runs YOLOv8 person detection, saves results |
| **MongoDB** | Stores detection results per frame |
| **FastAPI** | Exposes REST + SSE endpoints to query results |

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (includes Docker Compose)
- Git

> No Python or pip installation needed — everything runs inside Docker containers.

---

## Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/TranDucThien-0509/AI-People-Counter.git
cd AI-People-Counter
```

### 2. Add your video files

Place your `.mp4` video files inside the `video/` folder:

```
AI-People-Counter/
├── video/
│   ├── 1.mp4
│   └── 2.mp4
├── ingestion_server.py
├── processing_server.py
├── docker-compose.yml
├── Dockerfile.ingestion
├── Dockerfile.processing
└── yolov8n.pt
```

### 3. Configure which video to use

Open `docker-compose.yml` and set the `VIDEO_SOURCE` environment variable under `ingestion-server`:

```yaml
ingestion-server:
  environment:
    - REDIS_HOST=redis
    - VIDEO_SOURCE=/videos/2.mp4  # ← change filename here
  volumes:
    - ./video:/videos
```

### 4. Build and run

```bash
docker compose up --build
```

This starts all 4 services: Redis, MongoDB, Processing Server, and Ingestion Server.

To run in the background:

```bash
docker compose up --build -d
```

---

## Switching Videos (No Rebuild Needed)

After the first build, to switch to a different video:

1. Edit `VIDEO_SOURCE` in `docker-compose.yml`
2. Clear old data (optional but recommended):

```bash
# Clear Redis Stream
docker exec -it bigdata_redis redis-cli DEL camera:stream

# Clear MongoDB results
docker exec -it bigdata_mongodb mongosh --eval \
  "db.getSiblingDB('camera_analytics').people_counting.deleteMany({})"
```

3. Restart only the ingestion container:

```bash
docker compose restart ingestion-server
```

---

## 📡 API Endpoints

The Processing Server exposes a REST API at `http://localhost:8000`.

| Endpoint | Method | Description |
|---|---|---|
| `/` | GET | Server health check |
| `/latest` | GET | Latest detected frame result |
| `/history?limit=60` | GET | Last N frames of person counts |
| `/stats` | GET | Total number of frames processed |
| `/stats/stream` | GET | Server-Sent Events — live updates every second |

### Example

```bash
# Check how many frames have been processed
curl http://localhost:8000/stats

# Get the latest detection result
curl http://localhost:8000/latest
```

---

## Tech Stack

| Technology | Version | Purpose |
|---|---|---|
| Python | 3.12-slim | Runtime for both servers |
| YOLOv8 (Ultralytics) | `yolov8n.pt` | Person detection model |
| OpenCV | latest | Video decoding and frame processing |
| Redis | 7 Alpine | Stream-based message queue |
| MongoDB | 7.0 | Persistent result storage |
| FastAPI + Uvicorn | latest | REST API server |
| Docker Compose | v3.9 | Container orchestration |

---

## Project Structure

```
├── ingestion_server.py      # Reads video, pushes frames to Redis
├── processing_server.py     # Consumes frames, runs YOLO, exposes API
├── Dockerfile.ingestion     # Docker image for ingestion server
├── Dockerfile.processing    # Docker image for processing server
├── docker-compose.yml       # Orchestrates all services
├── yolov8n.pt               # YOLOv8 nano model weights
└── video/                   # Put your .mp4 files here (not committed to git)
```

---

## Notes

- The ingestion server **stops automatically** after the video ends — this is expected behavior.
- On first run, YOLOv8 may take a moment to initialize before processing begins.
- The `video/` folder is excluded from git (see `.gitignore`). Add your own video files manually after cloning.
