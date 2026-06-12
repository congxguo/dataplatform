-- ScyllaDB CQL schema for the ads online serving keyspace
-- Apply via: cqlsh <scylla-host> -f scylla_schema.cql

CREATE KEYSPACE IF NOT EXISTS ads
    WITH replication = {
        'class': 'NetworkTopologyStrategy',
        'replication_factor': 1   -- bump to 3 for production
    }
    AND tablets = {'enabled': false}
    AND durable_writes = true;

USE ads;

-- ── Primary events table ──────────────────────────────────────────────────────
-- Lookup by event_id (deduplication, audit)
CREATE TABLE IF NOT EXISTS ad_events (
    event_id      UUID PRIMARY KEY,
    event_type    TEXT,
    event_ts      TIMESTAMP,
    campaign_id   TEXT,
    advertiser_id TEXT,
    user_id       TEXT,
    platform      TEXT,
    country       TEXT,
    revenue       DOUBLE,
    is_fraud      BOOLEAN
) WITH
    compaction = { 'class': 'LeveledCompactionStrategy' }
    AND gc_grace_seconds = 86400
    AND default_time_to_live = 604800;   -- auto-expire raw events after 7 days

-- ── User-level recent activity (for real-time targeting) ─────────────────────
-- Partition by user_id, cluster by time DESC → fast "latest N events per user"
CREATE TABLE IF NOT EXISTS user_ad_activity (
    user_id       TEXT,
    event_ts      TIMESTAMP,
    event_id      UUID,
    event_type    TEXT,
    campaign_id   TEXT,
    platform      TEXT,
    country       TEXT,
    revenue       DOUBLE,
    PRIMARY KEY (user_id, event_ts, event_id)
) WITH
    CLUSTERING ORDER BY (event_ts DESC, event_id ASC)
    AND default_time_to_live = 86400    -- keep 24 h of per-user activity
    AND compaction = { 'class': 'TimeWindowCompactionStrategy',
                       'compaction_window_unit': 'HOURS',
                       'compaction_window_size': 1 };

-- ── Campaign click counters (for frequency capping) ──────────────────────────
CREATE TABLE IF NOT EXISTS campaign_counters (
    campaign_id   TEXT,
    date_hour     TEXT,          -- 'YYYY-MM-DD HH'
    impressions   COUNTER,
    clicks        COUNTER,
    conversions   COUNTER,
    PRIMARY KEY (campaign_id, date_hour)
) WITH CLUSTERING ORDER BY (date_hour DESC);
