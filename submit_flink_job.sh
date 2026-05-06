#!/usr/bin/env bash
# submit_flink_job.sh — submits flink_processor.py to the dockerized Flink
# cluster so the running job appears in the Web UI at http://localhost:8081
set -euo pipefail

JM=airqualityanalyzer-flink-jobmanager-1

# The script + the Kafka SQL connector JAR are mounted into the container by
# docker-compose, so no copy step is needed.
echo "Submitting flink_processor.py to Flink JobManager..."
docker exec -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 "$JM" \
    /opt/flink/bin/flink run \
    -d \
    -py /opt/flink/usrlib/flink_processor.py

echo
echo "Submitted. Open http://localhost:8081 to see the running job."
echo "List jobs:    docker exec $JM /opt/flink/bin/flink list"
