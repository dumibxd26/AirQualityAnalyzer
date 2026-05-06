# flink_processor.py
import glob
import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

# Where to reach Kafka.
#   - Local run on host: defaults to localhost:9092
#   - Submitted to dockerized Flink: docker-compose sets KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

def run_analytics_pipeline():
    # 1. Setup Environment
    env = StreamExecutionEnvironment.get_execution_environment()

    # Dynamically load all JARs from the local ./jars folder so we don't have to
    # hard-code paths. When running inside the dockerized Flink cluster, the JAR
    # is already on the classpath via /opt/flink/lib, so we skip this step.
    jars_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "jars")
    jar_uris = [
        f"file://{os.path.abspath(p)}"
        for p in glob.glob(os.path.join(jars_dir, "*.jar"))
    ]
    if jar_uris:
        env.add_jars(*jar_uris)

    settings = EnvironmentSettings.in_streaming_mode()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # 2. Define Kafka Source Table (Consuming raw data)
    #    - Use processing-time (proctime) for windowing so the demo doesn't depend
    #      on event-time watermarks (the upstream JSON timestamps are wildly
    #      out-of-order, which would otherwise drop most records as "late").
    #    - Use 'earliest-offset' so we consume the historical backlog already in
    #      the topic instead of waiting for new messages.
    #    - Be lenient with bad JSON rather than failing the whole job.
    t_env.execute_sql(f"""
        CREATE TABLE RawAirQuality (
            location STRING,
            city STRING,
            `parameter` STRING,
            `value` DOUBLE,
            `timestamp` TIMESTAMP(3),
            proctime AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'raw-air-quality',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'properties.group.id' = 'flink-consumer-group',
            'format' = 'json',
            'json.timestamp-format.standard' = 'SQL',
            'json.ignore-parse-errors' = 'true',
            'scan.startup.mode' = 'earliest-offset'
        )
    """)

    # 3. Define Kafka Sink Table (Publishing alerts)
    t_env.execute_sql(f"""
        CREATE TABLE PollutionAlerts (
            location STRING,
            window_end TIMESTAMP(3),
            avg_pm25 DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'pollution-alerts',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'format' = 'json'
        )
    """)

    # 3b. Also mirror alerts to stdout so we can see them live in this terminal.
    t_env.execute_sql("""
        CREATE TABLE PollutionAlertsPrint (
            location STRING,
            window_end TIMESTAMP(3),
            avg_pm25 DOUBLE
        ) WITH (
            'connector' = 'print'
        )
    """)

    # 4. Windowed Aggregation & Anomaly Detection
    #    - 10-second processing-time tumbling window (fast feedback for the demo).
    #    - Filter the OpenAQ "no data" sentinel (value < 0) so it doesn't pollute
    #      the average.
    #    - Threshold 10.0 ug/m3 keeps it sensitive enough that real PM2.5 readings
    #      will frequently trigger alerts.
    aggregation_sql = """
        SELECT
            location,
            TUMBLE_END(proctime, INTERVAL '10' SECOND) AS window_end,
            AVG(`value`) AS avg_pm25
        FROM RawAirQuality
        WHERE `value` IS NOT NULL AND `value` >= 0
        GROUP BY
            TUMBLE(proctime, INTERVAL '10' SECOND),
            location
        HAVING AVG(`value`) > 10.0
    """

    # 5. Execute: write the aggregation to both Kafka and stdout in one job.
    stmt_set = t_env.create_statement_set()
    stmt_set.add_insert_sql(f"INSERT INTO PollutionAlerts {aggregation_sql}")
    stmt_set.add_insert_sql(f"INSERT INTO PollutionAlertsPrint {aggregation_sql}")
    print("Submitting Flink pipeline... alerts will appear below as windows close.")
    stmt_set.execute().wait()

if __name__ == '__main__':
    run_analytics_pipeline()