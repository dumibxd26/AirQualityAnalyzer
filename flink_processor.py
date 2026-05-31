# flink_processor.py
#
# Production-shaped streaming job:
#   * Two Kafka sources (air quality + weather) with event-time + watermarks.
#   * A windowed aggregation per (station, parameter).
#   * A stream-stream interval join enriching pollution with current weather.
#   * Multiple sinks via a StatementSet:
#       - pollution-alerts        (threshold breach by parameter)
#       - enriched-readings       (per-minute aggregates + weather, for UI)
#       - critical-alerts         (3+ consecutive breaching windows -> escalate)
#       - print sink              (live tail in the Flink TaskManager log)
#   * Checkpointing every 30s to enable exactly-once Kafka delivery.
import glob
import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

def run_analytics_pipeline():
    # 1. Environment ----------------------------------------------------------
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    env.enable_checkpointing(30_000, CheckpointingMode.EXACTLY_ONCE)

    # Dynamically load all JARs from ./jars when running on the host. Inside the
    # docker cluster the connector JAR is already on /opt/flink/lib.
    jars_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "jars")
    jar_uris = [
        f"file://{os.path.abspath(p)}"
        for p in glob.glob(os.path.join(jars_dir, "*.jar"))
    ]
    if jar_uris:
        env.add_jars(*jar_uris)

    settings = EnvironmentSettings.in_streaming_mode()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    t_env.get_config().set("pipeline.name", "AirQualityAnalyzer")
    # Mark Kafka partitions as idle after 10s without records so the global
    # watermark can still advance when only a few of the 12 partitions per
    # topic are receiving data (we have ~40 stations spread unevenly across
    # 12 partitions, so many partitions are silent for long stretches).
    t_env.get_config().set("table.exec.source.idle-timeout", "10 s")

    # 2. Sources --------------------------------------------------------------
    # 2a. raw-air-quality: event-time via the producer-supplied timestamp,
    #     with a 1-minute out-of-orderness tolerance.
    t_env.execute_sql(f"""
        CREATE TABLE RawAirQuality (
            location   STRING,
            city       STRING,
            `parameter` STRING,
            `value`    DOUBLE,
            lat        DOUBLE,
            lon        DOUBLE,
            `timestamp` TIMESTAMP(3),
            WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '60' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'raw-air-quality',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'properties.group.id' = 'flink-air-quality',
            'format' = 'json',
            'json.timestamp-format.standard' = 'SQL',
            'json.ignore-parse-errors' = 'true',
            'scan.startup.mode' = 'earliest-offset'
        )
    """)

    # 2b. weather-stream: second source for the stream-stream join.
    t_env.execute_sql(f"""
        CREATE TABLE Weather (
            location       STRING,
            lat            DOUBLE,
            lon            DOUBLE,
            temperature    DOUBLE,
            wind_speed     DOUBLE,
            wind_direction DOUBLE,
            humidity       DOUBLE,
            precipitation  DOUBLE,
            `timestamp`    TIMESTAMP(3),
            WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '60' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'weather-stream',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'properties.group.id' = 'flink-weather',
            'format' = 'json',
            'json.timestamp-format.standard' = 'SQL',
            'json.ignore-parse-errors' = 'true',
            'scan.startup.mode' = 'earliest-offset'
        )
    """)

    # 3. Sinks ----------------------------------------------------------------
    # Sinks use end-to-end exactly-once: enabled checkpointing + Kafka
    # transactional sinks. Each sink needs its own transactional-id prefix so
    # parallel sub-tasks across topics don't collide on transaction ids.
    t_env.execute_sql(f"""
        CREATE TABLE PollutionAlerts (
            location    STRING,
            `parameter` STRING,
            window_end  TIMESTAMP(3),
            avg_value   DOUBLE,
            sample_count BIGINT,
            severity    STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'pollution-alerts',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'format' = 'json',
            'sink.delivery-guarantee' = 'exactly-once',
            'sink.transactional-id-prefix' = 'aqa-pollution-alerts',
            'properties.transaction.timeout.ms' = '900000'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE EnrichedReadings (
            location     STRING,
            `parameter`  STRING,
            window_end   TIMESTAMP(3),
            avg_value    DOUBLE,
            sample_count BIGINT,
            lat          DOUBLE,
            lon          DOUBLE,
            temperature  DOUBLE,
            wind_speed   DOUBLE,
            humidity     DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'enriched-readings',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'format' = 'json',
            'sink.delivery-guarantee' = 'exactly-once',
            'sink.transactional-id-prefix' = 'aqa-enriched',
            'properties.transaction.timeout.ms' = '900000'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE CriticalAlerts (
            location          STRING,
            `parameter`       STRING,
            first_window_end  TIMESTAMP(3),
            last_window_end   TIMESTAMP(3),
            peak_value        DOUBLE,
            breach_count      BIGINT
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'critical-alerts',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'format' = 'json',
            'sink.delivery-guarantee' = 'exactly-once',
            'sink.transactional-id-prefix' = 'aqa-critical',
            'properties.transaction.timeout.ms' = '900000'
        )
    """)

    t_env.execute_sql("""
        CREATE TABLE AlertsPrint (
            location    STRING,
            `parameter` STRING,
            window_end  TIMESTAMP(3),
            avg_value   DOUBLE,
            severity    STRING
        ) WITH ('connector' = 'print')
    """)

    # 4. Logic ----------------------------------------------------------------
    # 4a. Per-parameter thresholds (WHO guideline-ish, units: ug/m3 or ppb).
    #     CASE expression mirrors the alert severity classification used by UI.
    threshold_case = """
        CASE `parameter`
            WHEN 'pm25' THEN 15.0
            WHEN 'pm10' THEN 45.0
            WHEN 'no2'  THEN 25.0
            WHEN 'o3'   THEN 60.0
            WHEN 'so2'  THEN 40.0
            WHEN 'co'   THEN 4.0
            ELSE 9999.0
        END
    """

    severity_case = f"""
        CASE
            WHEN AVG(`value`) > 3 * ({threshold_case}) THEN 'CRITICAL'
            WHEN AVG(`value`) > 2 * ({threshold_case}) THEN 'HIGH'
            ELSE 'MODERATE'
        END
    """

    # 4b. 1-minute tumbling aggregation per (station, parameter).
    #     window_rowtime is required so downstream operators (HOP, interval
    #     join) can keep treating window_end as an event-time attribute.
    t_env.execute_sql(f"""
        CREATE TEMPORARY VIEW MinuteAggregates AS
        SELECT
            location,
            `parameter`,
            TUMBLE_START(`timestamp`, INTERVAL '1' MINUTE)   AS window_start,
            TUMBLE_END(`timestamp`, INTERVAL '1' MINUTE)     AS window_end,
            TUMBLE_ROWTIME(`timestamp`, INTERVAL '1' MINUTE) AS window_rowtime,
            AVG(`value`)  AS avg_value,
            COUNT(*)      AS sample_count,
            MAX(lat)      AS lat,
            MAX(lon)      AS lon
        FROM RawAirQuality
        WHERE `value` IS NOT NULL AND `value` >= 0
        GROUP BY
            TUMBLE(`timestamp`, INTERVAL '1' MINUTE),
            location,
            `parameter`
    """)

    # 4c. Threshold-breach alerts.
    alerts_sql = f"""
        SELECT
            location,
            `parameter`,
            window_end,
            window_rowtime,
            avg_value,
            sample_count,
            CASE
                WHEN avg_value > 3 * (
                    CASE `parameter`
                        WHEN 'pm25' THEN 15.0 WHEN 'pm10' THEN 45.0
                        WHEN 'no2'  THEN 25.0 WHEN 'o3'   THEN 60.0
                        WHEN 'so2'  THEN 40.0 WHEN 'co'   THEN 4.0
                        ELSE 9999.0
                    END) THEN 'CRITICAL'
                WHEN avg_value > 2 * (
                    CASE `parameter`
                        WHEN 'pm25' THEN 15.0 WHEN 'pm10' THEN 45.0
                        WHEN 'no2'  THEN 25.0 WHEN 'o3'   THEN 60.0
                        WHEN 'so2'  THEN 40.0 WHEN 'co'   THEN 4.0
                        ELSE 9999.0
                    END) THEN 'HIGH'
                ELSE 'MODERATE'
            END AS severity
        FROM MinuteAggregates
        WHERE avg_value > (
            CASE `parameter`
                WHEN 'pm25' THEN 15.0 WHEN 'pm10' THEN 45.0
                WHEN 'no2'  THEN 25.0 WHEN 'o3'   THEN 60.0
                WHEN 'so2'  THEN 40.0 WHEN 'co'   THEN 4.0
                ELSE 9999.0
            END)
    """

    # 4d. Stream-stream INTERVAL JOIN: enrich every aggregate window with the
    #     most recent weather sample for that station (within +-5 minutes).
    #     Uses window_rowtime (an event-time attribute) so Flink recognises
    #     this as a true interval join.
    enriched_sql = """
        SELECT
            a.location,
            a.`parameter`,
            a.window_end,
            a.avg_value,
            a.sample_count,
            a.lat,
            a.lon,
            w.temperature,
            w.wind_speed,
            w.humidity
        FROM MinuteAggregates a
        LEFT JOIN Weather w
          ON a.location = w.location
         AND w.`timestamp` BETWEEN a.window_rowtime - INTERVAL '5' MINUTE
                               AND a.window_rowtime + INTERVAL '5' MINUTE
    """

    # 4e. CRITICAL escalation: 3+ breaching windows inside a 5-minute bucket.
    #     We use TUMBLE (non-overlapping) instead of HOP so a single episode
    #     produces exactly one critical-alert row, not 5 overlapping copies.
    critical_sql = f"""
        SELECT
            location,
            `parameter`,
            MIN(window_end) AS first_window_end,
            MAX(window_end) AS last_window_end,
            MAX(avg_value)  AS peak_value,
            COUNT(*)        AS breach_count
        FROM (
            {alerts_sql}
        ) breaches
        GROUP BY
            TUMBLE(window_rowtime, INTERVAL '5' MINUTE),
            location,
            `parameter`
        HAVING COUNT(*) >= 3
    """

    # 5. Execute all sinks in one job ----------------------------------------
    # The internal `alerts_sql` view carries `window_rowtime` so the critical
    # HOP window can use it as an event-time attribute. The sink tables don't
    # have that column, so each INSERT projects only the matching fields.
    stmt = t_env.create_statement_set()
    stmt.add_insert_sql(f"""
        INSERT INTO PollutionAlerts
        SELECT location, `parameter`, window_end, avg_value, sample_count, severity
        FROM ({alerts_sql})
    """)
    stmt.add_insert_sql(f"INSERT INTO EnrichedReadings {enriched_sql}")
    stmt.add_insert_sql(f"INSERT INTO CriticalAlerts {critical_sql}")
    stmt.add_insert_sql(f"""
        INSERT INTO AlertsPrint
        SELECT location, `parameter`, window_end, avg_value, severity
        FROM ({alerts_sql})
    """)
    print("Submitting Flink pipeline (alerts, enrichment, escalation)...")
    stmt.execute().wait()


if __name__ == '__main__':
    run_analytics_pipeline()
