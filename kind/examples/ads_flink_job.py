"""
Flink Ads Pipeline Job
======================
Reads ads events from Kafka → writes to:
  • ScyllaDB  — for online serving  (via JDBC connector with Cassandra-compat driver)
  • Iceberg   — for offline analysis (via Flink catalog backed by MinIO/S3)

Run inside the Flink cluster (e.g. kubectl exec into the JobManager):
    flink run -py /jobs/ads_flink_job.py

Requirements (add to Flink lib/ or pass --pyFiles):
    pip install apache-flink apache-flink-libraries

Connector JARs expected on the classpath:
    - flink-sql-connector-kafka-*.jar
    - flink-sql-connector-jdbc-*.jar      (or cassandra connector)
    - iceberg-flink-runtime-*.jar
    - hadoop-aws-*.jar  (for S3/MinIO)
    - aws-java-sdk-bundle-*.jar
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    EnvironmentSettings,
    StreamTableEnvironment,
    TableDescriptor,
    Schema,
)
from pyflink.table.types import DataTypes

# ── Environment ───────────────────────────────────────────────────────────────

env      = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(2)
env.enable_checkpointing(30_000)   # checkpoint every 30 s

settings   = EnvironmentSettings.new_instance().in_streaming_mode().build()
table_env  = StreamTableEnvironment.create(env, environment_settings=settings)

# ── Iceberg catalog (MinIO/S3-compatible) ─────────────────────────────────────

table_env.execute_sql("""
CREATE CATALOG iceberg_catalog WITH (
    'type'             = 'iceberg',
    'catalog-type'     = 'hadoop',
    'warehouse'        = 's3a://lakehouse/warehouse',
    'io-impl'          = 'org.apache.iceberg.aws.s3.S3FileIO',
    'fs.s3a.endpoint'  = 'http://minio.minio.svc.cluster.local:9000',
    'fs.s3a.access.key'= 'minioadmin',
    'fs.s3a.secret.key'= 'minioadmin',
    'fs.s3a.path.style.access' = 'true'
)
""")

table_env.execute_sql("CREATE DATABASE IF NOT EXISTS iceberg_catalog.ads")

# ── Kafka source table ────────────────────────────────────────────────────────
# Schema mirrors the Python producer's JSON payload.

table_env.execute_sql("""
CREATE TABLE IF NOT EXISTS kafka_ads_events (
    event_id          STRING,
    event_type        STRING,
    event_ts          STRING,
    epoch_ms          BIGINT,

    ad_id             STRING,
    ad_type           STRING,
    campaign_id       STRING,
    advertiser_id     STRING,
    creative_id       STRING,
    placement_id      STRING,

    user_id           STRING,
    session_id        STRING,
    device_id         STRING,
    platform          STRING,
    device_type       STRING,
    os                STRING,
    country           STRING,
    region            STRING,
    city              STRING,
    ip_hash           STRING,

    bid_floor         DOUBLE,
    winning_bid       DOUBLE,
    revenue           DOUBLE,
    currency          STRING,
    auction_id        STRING,

    app_id            STRING,
    bundle            STRING,
    page_url          STRING,
    category          STRING,
    video_duration_s  INT,
    viewability       DOUBLE,

    is_fraud          BOOLEAN,
    gdpr_consent      BOOLEAN,
    ccpa_opt_out      BOOLEAN,

    -- Flink watermark from epoch_ms (tolerate 5s late arrivals)
    row_time AS TO_TIMESTAMP_LTZ(epoch_ms, 3),
    WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'ads-events',
    'properties.bootstrap.servers'  = 'kafka.kafka.svc.cluster.local:9092',
    'properties.group.id'           = 'flink-ads-consumer',
    'scan.startup.mode'             = 'latest-offset',
    'format'                        = 'json',
    'json.fail-on-missing-field'    = 'false',
    'json.ignore-parse-errors'      = 'true'
)
""")

# ── ScyllaDB sink (via JDBC / Cassandra connector) ────────────────────────────
# Uses the Flink JDBC connector pointed at ScyllaDB's CQL-over-JDBC driver.
# Alternatively, swap to the flink-connector-cassandra if you prefer native CQL.

table_env.execute_sql("""
CREATE TABLE IF NOT EXISTS scylla_ad_events (
    event_id     STRING,
    event_type   STRING,
    event_ts     STRING,
    campaign_id  STRING,
    advertiser_id STRING,
    user_id      STRING,
    platform     STRING,
    country      STRING,
    revenue      DOUBLE,
    is_fraud     BOOLEAN,
    PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
    'connector'               = 'jdbc',
    'url'                     = 'jdbc:cassandra://scylla.scylla.svc.cluster.local:9042/ads',
    'table-name'              = 'ad_events',
    'driver'                  = 'com.datastax.oss.jdbc.CassandraDriver',
    'sink.buffer-flush.max-rows' = '1000',
    'sink.buffer-flush.interval' = '2s',
    'sink.parallelism'        = '2'
)
""")

# ── Iceberg sink table (partitioned by date + platform) ───────────────────────

table_env.execute_sql("""
CREATE TABLE IF NOT EXISTS iceberg_catalog.ads.ad_events (
    event_id      STRING,
    event_type    STRING,
    event_ts      STRING,
    epoch_ms      BIGINT,
    campaign_id   STRING,
    advertiser_id STRING,
    ad_type       STRING,
    user_id       STRING,
    platform      STRING,
    country       STRING,
    category      STRING,
    bid_floor     DOUBLE,
    winning_bid   DOUBLE,
    revenue       DOUBLE,
    viewability   DOUBLE,
    is_fraud      BOOLEAN,
    event_date    STRING   -- partition column (YYYY-MM-DD)
) PARTITIONED BY (event_date, platform)
WITH (
    'format-version' = '2',
    'write.upsert.enabled' = 'true'
)
""")

# ── Pipeline: Kafka → ScyllaDB (raw events, filtered for non-fraud) ───────────

table_env.execute_sql("""
INSERT INTO scylla_ad_events
SELECT
    event_id,
    event_type,
    event_ts,
    campaign_id,
    advertiser_id,
    user_id,
    platform,
    country,
    revenue,
    is_fraud
FROM kafka_ads_events
WHERE is_fraud = FALSE
""")

# ── Pipeline: Kafka → Iceberg (all events, enriched) ─────────────────────────

table_env.execute_sql("""
INSERT INTO iceberg_catalog.ads.ad_events
SELECT
    event_id,
    event_type,
    event_ts,
    epoch_ms,
    campaign_id,
    advertiser_id,
    ad_type,
    user_id,
    platform,
    country,
    category,
    bid_floor,
    winning_bid,
    revenue,
    viewability,
    is_fraud,
    DATE_FORMAT(row_time, 'yyyy-MM-dd') AS event_date
FROM kafka_ads_events
""")

# ── Bonus: 1-minute windowed aggregation → Iceberg ────────────────────────────
# Useful for model feature engineering without reprocessing raw events.

table_env.execute_sql("""
CREATE TABLE IF NOT EXISTS iceberg_catalog.ads.campaign_1min_agg (
    window_start  TIMESTAMP(3),
    window_end    TIMESTAMP(3),
    campaign_id   STRING,
    platform      STRING,
    impressions   BIGINT,
    clicks        BIGINT,
    conversions   BIGINT,
    total_revenue DOUBLE,
    avg_viewability DOUBLE,
    fraud_count   BIGINT
) PARTITIONED BY (campaign_id)
WITH ('format-version' = '2')
""")

table_env.execute_sql("""
INSERT INTO iceberg_catalog.ads.campaign_1min_agg
SELECT
    window_start,
    window_end,
    campaign_id,
    platform,
    COUNT(*)                                                    AS impressions,
    COUNT(*) FILTER (WHERE event_type = 'click')               AS clicks,
    COUNT(*) FILTER (WHERE event_type = 'conversion')          AS conversions,
    ROUND(SUM(revenue), 6)                                      AS total_revenue,
    ROUND(AVG(viewability), 4)                                  AS avg_viewability,
    COUNT(*) FILTER (WHERE is_fraud = TRUE)                     AS fraud_count
FROM TABLE(
    TUMBLE(TABLE kafka_ads_events, DESCRIPTOR(row_time), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, campaign_id, platform
""")

print("[flink-job] All sinks registered — job running...")
