# producer.py
#
# OpenAQ -> Kafka producer.
#
# Improvements over the original:
#   * Polls MULTIPLE pollutants (pm25, pm10, no2, o3, so2, co) concurrently
#     so the topic actually has enough volume for Flink to do real work.
#   * Keys each Kafka record by `location` so records are partitioned
#     deterministically -> true parallelism across Flink subtasks.
#   * Captures lat/lon coordinates for the weather join and map UI.
#   * Reconnects on failure and logs structured progress.
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor

import requests
from kafka import KafkaProducer

import env

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] producer: %(message)s",
)
log = logging.getLogger("producer")

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "raw-air-quality"

# OpenAQ v3 parameter ids.
PARAMETERS = {
    2: "pm25",
    1: "pm10",
    7: "no2",
    10: "o3",
    9: "so2",
    8: "co",
}

POLL_INTERVAL_SEC = int(os.environ.get("POLL_INTERVAL_SEC", "15"))
LIMIT_PER_PARAM = int(os.environ.get("LIMIT_PER_PARAM", "200"))


def make_producer() -> KafkaProducer:
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                linger_ms=50,
                acks="all",
                retries=5,
            )
        except Exception as e:  # noqa: BLE001
            log.warning("Kafka not ready (%s), retrying in 3s...", e)
            time.sleep(3)


def fetch_param(param_id: int, param_name: str):
    url = f"https://api.openaq.org/v3/parameters/{param_id}/latest"
    headers = {"X-API-Key": env.API_KEY} if env.API_KEY else {}
    try:
        r = requests.get(
            url,
            params={"limit": LIMIT_PER_PARAM},
            headers=headers,
            timeout=15,
        )
        r.raise_for_status()
        return param_name, r.json().get("results", [])
    except Exception as e:  # noqa: BLE001
        log.warning("Fetch %s failed: %s", param_name, e)
        return param_name, []


def to_payload(item: dict, param_name: str):
    raw_time = (item.get("datetime") or {}).get("utc", "")
    if not raw_time:
        return None
    flink_time = raw_time.replace("T", " ").replace("Z", "")
    coords = item.get("coordinates") or {}
    loc_id = item.get("locationsId", "Unknown")
    value = item.get("value")
    if value is None or value < 0:  # OpenAQ "no-data" sentinel
        return None
    return {
        "location": f"Station-{loc_id}",
        "city": "Global-Network",
        "parameter": param_name,
        "value": float(value),
        "lat": coords.get("latitude"),
        "lon": coords.get("longitude"),
        "timestamp": flink_time,
    }


def main() -> None:
    producer = make_producer()
    log.info("Connected to %s, publishing to '%s'", BOOTSTRAP, TOPIC)

    with ThreadPoolExecutor(max_workers=len(PARAMETERS)) as pool:
        while True:
            sent = 0
            futures = [pool.submit(fetch_param, pid, pname)
                       for pid, pname in PARAMETERS.items()]
            for f in futures:
                param_name, results = f.result()
                for item in results:
                    payload = to_payload(item, param_name)
                    if not payload:
                        continue
                    producer.send(TOPIC, key=payload["location"], value=payload)
                    sent += 1
            producer.flush()
            log.info("Tick: %d records across %d parameters", sent, len(PARAMETERS))
            time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main()
