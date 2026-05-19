# weather_producer.py
#
# Subscribes to `raw-air-quality` to learn which stations exist (with lat/lon),
# then periodically polls Open-Meteo's free API for current weather at each
# unique station and publishes to the `weather-stream` Kafka topic.
#
# This is the second source that lets the Flink job perform a stream-stream
# join (pollution x weather) -- the kind of work Flink was actually built for.
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] weather: %(message)s",
)
log = logging.getLogger("weather")

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
IN_TOPIC = "raw-air-quality"
OUT_TOPIC = "weather-stream"
POLL_INTERVAL_SEC = int(os.environ.get("WEATHER_POLL_SEC", "60"))
MAX_STATIONS = int(os.environ.get("WEATHER_MAX_STATIONS", "100"))

# location -> (lat, lon)
STATIONS: dict[str, tuple[float, float]] = {}
LOCK = threading.Lock()


def make_consumer() -> KafkaConsumer:
    while True:
        try:
            return KafkaConsumer(
                IN_TOPIC,
                bootstrap_servers=BOOTSTRAP.split(","),
                group_id="weather-station-discovery",
                auto_offset_reset="latest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                enable_auto_commit=True,
            )
        except Exception as e:  # noqa: BLE001
            log.warning("Kafka not ready (%s), retrying...", e)
            time.sleep(3)


def make_producer() -> KafkaProducer:
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
            )
        except Exception as e:  # noqa: BLE001
            log.warning("Kafka not ready (%s), retrying...", e)
            time.sleep(3)


def discover_stations() -> None:
    """Background thread: learn (location, lat, lon) from raw-air-quality."""
    consumer = make_consumer()
    log.info("Discovering stations from %s ...", IN_TOPIC)
    for msg in consumer:
        rec = msg.value or {}
        loc = rec.get("location")
        lat = rec.get("lat")
        lon = rec.get("lon")
        if not loc or lat is None or lon is None:
            continue
        with LOCK:
            if loc not in STATIONS and len(STATIONS) < MAX_STATIONS:
                STATIONS[loc] = (float(lat), float(lon))
                log.info("Learned station %s @ (%.3f, %.3f) [%d total]",
                         loc, lat, lon, len(STATIONS))


def fetch_weather(lat: float, lon: float):
    url = "https://api.open-meteo.com/v1/forecast"
    try:
        r = requests.get(
            url,
            params={
                "latitude": lat,
                "longitude": lon,
                "current": "temperature_2m,wind_speed_10m,wind_direction_10m,relative_humidity_2m,precipitation",
            },
            timeout=10,
        )
        r.raise_for_status()
        return r.json().get("current") or {}
    except Exception as e:  # noqa: BLE001
        log.warning("Open-Meteo fetch failed for (%.3f, %.3f): %s", lat, lon, e)
        return {}


def main() -> None:
    threading.Thread(target=discover_stations, daemon=True).start()
    producer = make_producer()
    log.info("Weather producer ready, publishing to '%s' every %ds",
             OUT_TOPIC, POLL_INTERVAL_SEC)

    while True:
        with LOCK:
            snapshot = list(STATIONS.items())
        if not snapshot:
            log.info("No stations learned yet, waiting...")
            time.sleep(5)
            continue

        sent = 0
        for loc, (lat, lon) in snapshot:
            cur = fetch_weather(lat, lon)
            if not cur:
                continue
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            payload = {
                "location": loc,
                "lat": lat,
                "lon": lon,
                "temperature": cur.get("temperature_2m"),
                "wind_speed": cur.get("wind_speed_10m"),
                "wind_direction": cur.get("wind_direction_10m"),
                "humidity": cur.get("relative_humidity_2m"),
                "precipitation": cur.get("precipitation"),
                "timestamp": ts,
            }
            producer.send(OUT_TOPIC, key=loc, value=payload)
            sent += 1
        producer.flush()
        log.info("Weather tick: %d / %d stations published", sent, len(snapshot))
        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main()
