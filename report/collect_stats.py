"""Statistics collector for the AirQualityAnalyzer report.

Runs alongside the live Docker stack and records time-series + event-level
data for ~1 hour (configurable). Produces three artefacts in an output dir:

  throughput.csv      - cumulative Kafka offsets sampled every INTERVAL s
  alerts.jsonl        - every pollution-alert message (compact + recv_ts)
  critical.jsonl      - every critical-alert message (compact + recv_ts)
  enriched.jsonl      - compact enriched-reading records (concentration + weather)

Connects to Kafka via the external listener (localhost:9092).

Usage:
  python collect_stats.py --minutes 60 --interval 30
"""
from __future__ import annotations

import argparse
import json
import signal
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaConsumer, TopicPartition

BOOTSTRAP = "localhost:9092"
OFFSET_TOPICS = [
    "raw-air-quality",
    "weather-stream",
    "enriched-readings",
    "pollution-alerts",
    "critical-alerts",
]

_STOP = threading.Event()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def total_end_offsets(consumer: KafkaConsumer, topic: str) -> int:
    """Sum of end offsets across all partitions of a topic."""
    parts = consumer.partitions_for_topic(topic)
    if not parts:
        return 0
    tps = [TopicPartition(topic, p) for p in parts]
    end = consumer.end_offsets(tps)
    return int(sum(end.values()))


def offset_sampler(out_dir: Path, interval: int) -> None:
    """Sample cumulative offsets every `interval` seconds."""
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP,
        consumer_timeout_ms=5000,
        request_timeout_ms=8000,
    )
    csv_path = out_dir / "throughput.csv"
    with csv_path.open("w", encoding="utf-8") as f:
        f.write("ts_iso,elapsed_s," + ",".join(OFFSET_TOPICS) + "\n")
        start = time.time()
        while not _STOP.is_set():
            row_start = time.time()
            vals = []
            for t in OFFSET_TOPICS:
                try:
                    vals.append(total_end_offsets(consumer, t))
                except Exception:
                    vals.append("")
            elapsed = int(time.time() - start)
            f.write(f"{_now_iso()},{elapsed}," + ",".join(str(v) for v in vals) + "\n")
            f.flush()
            # sleep the remainder of the interval, but stay responsive to stop
            wait = max(0.0, interval - (time.time() - row_start))
            _STOP.wait(wait)
    consumer.close()


def topic_recorder(topic: str, out_file: Path, project) -> None:
    """Consume a topic from 'latest' and append projected records as JSONL."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP,
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=2000,
        group_id=None,
    )
    with out_file.open("w", encoding="utf-8") as f:
        while not _STOP.is_set():
            batch = consumer.poll(timeout_ms=1000)
            for _tp, msgs in batch.items():
                for m in msgs:
                    rec = project(m.value)
                    rec["recv_ts"] = _now_iso()
                    f.write(json.dumps(rec, default=str) + "\n")
            f.flush()
    consumer.close()


def proj_alert(v: dict) -> dict:
    return {
        "location": v.get("location"),
        "parameter": v.get("parameter"),
        "value": v.get("avg_value"),
        "severity": v.get("severity"),
        "window_end": v.get("window_end"),
        "sample_count": v.get("sample_count"),
    }


def proj_critical(v: dict) -> dict:
    return {
        "location": v.get("location"),
        "parameter": v.get("parameter"),
        "peak_value": v.get("peak_value"),
        "breach_count": v.get("breach_count"),
        "first_window_end": v.get("first_window_end"),
        "last_window_end": v.get("last_window_end"),
    }


def proj_enriched(v: dict) -> dict:
    return {
        "location": v.get("location"),
        "parameter": v.get("parameter"),
        "value": v.get("avg_value"),
        "severity": v.get("severity"),
        "temperature": v.get("temperature"),
        "wind_speed": v.get("wind_speed"),
        "humidity": v.get("humidity"),
        "window_end": v.get("window_end"),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=60.0)
    ap.add_argument("--interval", type=int, default=30, help="offset sample period (s)")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out) if args.out else Path(__file__).parent / "data" / f"run_{stamp}"
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"[collector] output -> {out_dir}")
    print(f"[collector] running for {args.minutes} min, offset interval {args.interval}s")

    def _handle(_sig, _frm):
        print("[collector] stop signal received")
        _STOP.set()

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)

    threads = [
        threading.Thread(target=offset_sampler, args=(out_dir, args.interval), daemon=True),
        threading.Thread(target=topic_recorder, args=("pollution-alerts", out_dir / "alerts.jsonl", proj_alert), daemon=True),
        threading.Thread(target=topic_recorder, args=("critical-alerts", out_dir / "critical.jsonl", proj_critical), daemon=True),
        threading.Thread(target=topic_recorder, args=("enriched-readings", out_dir / "enriched.jsonl", proj_enriched), daemon=True),
    ]
    for t in threads:
        t.start()

    deadline = time.time() + args.minutes * 60
    while time.time() < deadline and not _STOP.is_set():
        _STOP.wait(2)
    _STOP.set()
    print("[collector] finishing, flushing threads...")
    for t in threads:
        t.join(timeout=10)

    # write a small manifest
    (out_dir / "manifest.json").write_text(json.dumps({
        "started": stamp,
        "finished": _now_iso(),
        "minutes": args.minutes,
        "interval_s": args.interval,
        "topics": OFFSET_TOPICS,
    }, indent=2), encoding="utf-8")
    print(f"[collector] done -> {out_dir}")


if __name__ == "__main__":
    main()
