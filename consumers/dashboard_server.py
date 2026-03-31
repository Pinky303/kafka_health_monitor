"""
Dashboard WebSocket Server
==========================
FastAPI server that reads from Kafka and streams vitals to all
connected browser clients via WebSocket in real time.

Run: uvicorn consumers.dashboard_server:app --reload --port 8000
"""

import json
import asyncio
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [DASHBOARD] %(levelname)s — %(message)s",
)
logger = logging.getLogger("DashboardServer")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
VITALS_TOPIC    = "patient-vitals"
ALERTS_TOPIC    = "patient-alerts"
DASHBOARD_DIR   = Path(__file__).parent.parent / "dashboard"

app = FastAPI(
    title="ICU Health Monitor Dashboard",
    description="Real-time patient vitals streaming via Kafka + WebSockets",
    version="1.0.0",
)

# ── Shared state ──────────────────────────────────────────────────────────────

class ConnectionManager:
    def __init__(self):
        self.active: set[WebSocket] = set()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.add(ws)
        logger.info(f"🔌 Client connected. Active: {len(self.active)}")

    def disconnect(self, ws: WebSocket):
        self.active.discard(ws)
        logger.info(f"🔌 Client left. Active: {len(self.active)}")

    async def broadcast(self, payload: dict):
        dead = set()
        for ws in self.active:
            try:
                await ws.send_json(payload)
            except Exception:
                dead.add(ws)
        self.active -= dead


vitals_manager = ConnectionManager()
alerts_manager = ConnectionManager()

latest_vitals: dict[str, dict] = {}   # patient_id → latest reading
recent_alerts: list[dict]      = []   # rolling last 50 alerts
vitals_queue: asyncio.Queue    = None
alerts_queue:  asyncio.Queue   = None


# ── Kafka consumer threads ────────────────────────────────────────────────────

def vitals_consumer_thread(loop: asyncio.AbstractEventLoop, queue: asyncio.Queue):
    consumer = KafkaConsumer(
        VITALS_TOPIC,
        bootstrap_servers  = KAFKA_BOOTSTRAP,
        group_id           = "dashboard-vitals-group",
        auto_offset_reset  = "latest",
        value_deserializer = lambda m: json.loads(m.decode()),
        enable_auto_commit = True,
    )
    logger.info("📡 Vitals Kafka consumer started")
    for msg in consumer:
        data = msg.value
        latest_vitals[data["patient_id"]] = data
        asyncio.run_coroutine_threadsafe(queue.put({"type": "vitals", "data": data}), loop)


def alerts_consumer_thread(loop: asyncio.AbstractEventLoop, queue: asyncio.Queue):
    consumer = KafkaConsumer(
        ALERTS_TOPIC,
        bootstrap_servers  = KAFKA_BOOTSTRAP,
        group_id           = "dashboard-alerts-group",
        auto_offset_reset  = "latest",
        value_deserializer = lambda m: json.loads(m.decode()),
        enable_auto_commit = True,
    )
    logger.info("🚨 Alerts Kafka consumer started")
    for msg in consumer:
        data = msg.value
        recent_alerts.insert(0, data)
        del recent_alerts[50:]   # keep last 50
        asyncio.run_coroutine_threadsafe(queue.put({"type": "alert", "data": data}), loop)


# ── Broadcast tasks ───────────────────────────────────────────────────────────

async def broadcast_loop():
    while True:
        msg = await vitals_queue.get()
        if msg["type"] == "vitals":
            await vitals_manager.broadcast(msg)
        elif msg["type"] == "alert":
            await alerts_manager.broadcast(msg)
            await vitals_manager.broadcast(msg)   # also push alerts to main feed


# ── Startup ───────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    global vitals_queue, alerts_queue
    vitals_queue = asyncio.Queue(maxsize=1000)
    alerts_queue = asyncio.Queue(maxsize=200)
    loop = asyncio.get_event_loop()

    threading.Thread(
        target=vitals_consumer_thread,
        args=(loop, vitals_queue),
        daemon=True,
    ).start()

    threading.Thread(
        target=alerts_consumer_thread,
        args=(loop, vitals_queue),   # share single queue, typed by "type" field
        daemon=True,
    ).start()

    asyncio.create_task(broadcast_loop())
    logger.info("🖥  Dashboard server ready at http://localhost:8000")


# ── WebSocket endpoints ───────────────────────────────────────────────────────

@app.websocket("/ws/vitals")
async def vitals_ws(ws: WebSocket):
    await vitals_manager.connect(ws)

    # Send snapshot of all current patients immediately on connect
    await ws.send_json({
        "type":    "snapshot",
        "data":    list(latest_vitals.values()),
        "alerts":  recent_alerts[:10],
        "ts":      datetime.now(timezone.utc).isoformat(),
    })

    try:
        while True:
            await ws.receive_text()   # keepalive ping
    except WebSocketDisconnect:
        vitals_manager.disconnect(ws)


@app.websocket("/ws/alerts")
async def alerts_ws(ws: WebSocket):
    await alerts_manager.connect(ws)
    await ws.send_json({"type": "snapshot_alerts", "data": recent_alerts})
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        alerts_manager.disconnect(ws)


# ── REST endpoints ────────────────────────────────────────────────────────────

@app.get("/api/patients")
async def get_patients():
    return JSONResponse({"count": len(latest_vitals), "patients": list(latest_vitals.values())})


@app.get("/api/alerts")
async def get_alerts():
    return JSONResponse({"count": len(recent_alerts), "alerts": recent_alerts})


@app.get("/api/health")
async def health_check():
    return {"status": "ok", "patients_tracked": len(latest_vitals)}


@app.get("/")
async def root():
    index = DASHBOARD_DIR / "index.html"
    if index.exists():
        return FileResponse(index)
    return JSONResponse({"message": "Dashboard not found — place index.html in dashboard/"})
