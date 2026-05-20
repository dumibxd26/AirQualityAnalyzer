# producer.py
#
# OpenAQ -> Kafka producer.
#
# Improvements over the original:
#   * Polls MULTIPLE pollutants concurrently for volume.
#   * Keys each Kafka record by `location` for deterministic partitioning.
#   * Captures lat/lon for the weather join + map UI.
#   * **De-duplicates** by (location, parameter, timestamp) so we don't
#     republish the same OpenAQ reading every poll cycle.
#   * **Drops stale readings** whose original timestamp is older than
#     MAX_AGE_HOURS -- OpenAQ /latest can return values years old from
#     defunct stations, which previously poisoned Flink's watermark.
#   * **Rewrites the Kafka-record `timestamp` to ingest time** so Flink's
#     event-time windows close promptly and the dashboard feels live.
#     The original event time is preserved as `event_time`.
#   * Catches send-buffer overflow instead of crashing.
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError

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
MAX_AGE_HOURS = int(os.environ.get("MAX_AGE_HOURS", "6"))

# How long the same (location, parameter, event_time) is suppressed before we
# allow it to be republished with a fresh ingest timestamp. This keeps Flink
# windows continuously fed (so the dashboard stays "live") while still
# preventing the original ~1190x/min republish firehose. Set to 0 to disable
# republishing entirely.
DEDUP_TTL_SEC = int(os.environ.get("DEDUP_TTL_SEC", "90"))

# In-memory dedup: (location, parameter) -> (event_time_str, last_published_at).
LAST_SEEN: dict[tuple[str, str], tuple[str, float]] = {}


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
                max_block_ms=5_000,  # don't block forever if broker is sick
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


def to_payload(item: dict, param_name: str, now_iso: str, max_age: timedelta):
    raw_time = (item.get("datetime") or {}).get("utc", "")
    if not raw_time:
        return None, "no_time"

    # OpenAQ format: "2026-05-20T13:00:00Z"
    try:
        event_dt = datetime.strptime(raw_time, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        return None, "bad_time"

    # Drop records whose true event time is older than MAX_AGE_HOURS.
    if datetime.now(timezone.utc) - event_dt > max_age:
        return None, "stale"

    coords = item.get("coordinates") or {}
    loc_id = item.get("locationsId", "Unknown")
    value = item.get("value")
    if value is None or value < 0:  # OpenAQ "no-data" sentinel
        return None, "no_value"

    location = f"Station-{loc_id}"
    event_time_str = event_dt.strftime("%Y-%m-%d %H:%M:%S")

    # Dedup with TTL: suppress republishing the same (location, parameter,
    # event_time) for DEDUP_TTL_SEC. After that we allow it through again
    # with a fresh ingest timestamp so Flink windows keep firing.
    key = (location, param_name)
    now_mono = time.monotonic()
    last = LAST_SEEN.get(key)
    if last is not None:
        last_event, last_published = last
        if last_event == event_time_str and (now_mono - last_published) < DEDUP_TTL_SEC:
            return None, "dup"
    LAST_SEEN[key] = (event_time_str, now_mono)

    return {
        "location": location,
        "city": "Global-Network",
        "parameter": param_name,
        "value": float(value),
        "lat": coords.get("latitude"),
        "lon": coords.get("longitude"),
        # `timestamp` is the ingest time so Flink event-time windows close
        # promptly; `event_time` keeps the original sensor timestamp.
        "timestamp": now_iso,
        "event_time": event_time_str,
    }, "ok"


def main() -> None:
    producer = make_producer()
    log.info(
        "Connected to %s, publishing to '%s' (max_age=%dh)",
        BOOTSTRAP, TOPIC, MAX_AGE_HOURS,
    )

    max_age = timedelta(hours=MAX_AGE_HOURS)

    with ThreadPoolExecutor(max_workers=len(PARAMETERS)) as pool:
        while True:
            now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            stats = {"ok": 0, "dup": 0, "stale": 0, "no_value": 0, "no_time": 0, "bad_time": 0}
            futures = [pool.submit(fetch_param, pid, pname)
                       for pid, pname in PARAMETERS.items()]
            for f in futures:
                param_name, results = f.result()
                for item in results:
                    payload, reason = to_payload(item, param_name, now_iso, max_age)
                    stats[reason] = stats.get(reason, 0) + 1
                    if not payload:
                        continue
                    try:
                        producer.send(TOPIC, key=payload["location"], value=payload)
                    except (KafkaTimeoutError, BufferError) as e:
                        log.warning("send buffer pressure (%s); skipping record", e)
                        continue
            try:
                producer.flush(timeout=10)
            except KafkaTimeoutError:
                log.warning("flush timed out; broker may be slow")
            log.info(
                "Tick: published=%d dup=%d stale=%d empty=%d (last_seen=%d)",
                stats["ok"], stats["dup"], stats["stale"],
                stats["no_value"] + stats["no_time"] + stats["bad_time"],
                len(LAST_SEEN),
            )
            time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main()

