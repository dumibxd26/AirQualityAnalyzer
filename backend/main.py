"""FastAPI WebSocket bridge: Kafka -> Browser.

Subscribes to the topics produced by Flink + the raw stream and fans-out
JSON messages to every connected websocket client. Also exposes a REST
snapshot of recent state so the UI can hydrate on page load.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from collections import deque
from contextlib import asynccontextmanager
from typing import Any

from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] api: %(message)s",
)
log = logging.getLogger("api")

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# topic -> message type emitted to the UI
TOPICS = {
    "raw-air-quality":   "reading",
    "enriched-readings": "enriched",
    "pollution-alerts":  "alert",
    "critical-alerts":   "critical",
}

# Rolling in-memory state for hydration / dashboard snapshots.
RECENT_READINGS: deque[dict[str, Any]] = deque(maxlen=2000)
RECENT_ALERTS:   deque[dict[str, Any]] = deque(maxlen=200)
RECENT_CRITICAL: deque[dict[str, Any]] = deque(maxlen=100)
STATIONS: dict[str, dict[str, Any]] = {}  # location -> {lat, lon, last_value, ...}

CLIENTS: set[WebSocket] = set()


async def broadcast(msg: dict[str, Any]) -> None:
    if not CLIENTS:
        return
    payload = json.dumps(msg, default=str)
    dead = []
    for ws in CLIENTS:
        try:
            await ws.send_text(payload)
        except Exception:  # noqa: BLE001
            dead.append(ws)
    for ws in dead:
        CLIENTS.discard(ws)


def _update_state(kind: str, value: dict[str, Any]) -> None:
    if kind == "reading":
        RECENT_READINGS.append(value)
        loc = value.get("location")
        if loc:
            s = STATIONS.setdefault(loc, {"location": loc})
            if value.get("lat") is not None:
                s["lat"] = value["lat"]
                s["lon"] = value["lon"]
            s.setdefault("readings", {})[value.get("parameter")] = value.get("value")
            s["last_seen"] = value.get("timestamp")
    elif kind == "alert":
        RECENT_ALERTS.append(value)
    elif kind == "critical":
        RECENT_CRITICAL.append(value)
    elif kind == "enriched":
        loc = value.get("location")
        if loc and loc in STATIONS:
            STATIONS[loc]["temperature"] = value.get("temperature")
            STATIONS[loc]["wind_speed"] = value.get("wind_speed")
            STATIONS[loc]["humidity"] = value.get("humidity")


async def consume_topic(topic: str, kind: str) -> None:
    while True:
        try:
            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=BOOTSTRAP,
                group_id=f"ui-bridge-{topic}",
                auto_offset_reset="latest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            await consumer.start()
            log.info("Subscribed to %s", topic)
            try:
                async for msg in consumer:
                    value = msg.value
                    _update_state(kind, value)
                    await broadcast({"type": kind, "data": value})
            finally:
                await consumer.stop()
        except Exception as e:  # noqa: BLE001
            log.warning("Consumer %s failed (%s), retrying in 3s", topic, e)
            await asyncio.sleep(3)


@asynccontextmanager
async def lifespan(app: FastAPI):
    tasks = [asyncio.create_task(consume_topic(t, k)) for t, k in TOPICS.items()]
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()


app = FastAPI(title="AirQualityAnalyzer API", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "ok": True,
        "clients": len(CLIENTS),
        "stations": len(STATIONS),
        "recent_readings": len(RECENT_READINGS),
        "recent_alerts": len(RECENT_ALERTS),
    }


@app.get("/snapshot")
async def snapshot() -> dict[str, Any]:
    return {
        "stations": list(STATIONS.values()),
        "alerts":   list(RECENT_ALERTS),
        "critical": list(RECENT_CRITICAL),
        "readings": list(RECENT_READINGS)[-500:],
    }


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    await ws.accept()
    CLIENTS.add(ws)
    log.info("Client connected (%d total)", len(CLIENTS))
    try:
        # Push a hydration snapshot first.
        await ws.send_text(json.dumps({
            "type": "snapshot",
            "data": {
                "stations": list(STATIONS.values()),
                "alerts":   list(RECENT_ALERTS),
                "critical": list(RECENT_CRITICAL),
                "readings": list(RECENT_READINGS)[-500:],
            },
        }, default=str))
        while True:
            # We don't expect client messages; this just keeps the socket alive.
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        CLIENTS.discard(ws)
        log.info("Client disconnected (%d remain)", len(CLIENTS))
