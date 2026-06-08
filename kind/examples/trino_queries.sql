-- Trino queries against the Iceberg ads catalog
-- Assumes a Trino catalog named "iceberg" pointing at the MinIO-backed Iceberg warehouse.

-- ── 1. Recent event volume by type (last 24 h) ────────────────────────────────
SELECT
    event_type,
    COUNT(*)                      AS event_count,
    ROUND(SUM(revenue), 2)        AS total_revenue_usd
FROM iceberg.ads.ad_events
WHERE event_date >= CAST(CURRENT_DATE - INTERVAL '1' DAY AS VARCHAR)
GROUP BY event_type
ORDER BY event_count DESC;


-- ── 2. Campaign performance summary ───────────────────────────────────────────
SELECT
    campaign_id,
    COUNT(*)                                              AS impressions,
    COUNT(*) FILTER (WHERE event_type = 'click')          AS clicks,
    COUNT(*) FILTER (WHERE event_type = 'conversion')     AS conversions,
    ROUND(
        COUNT(*) FILTER (WHERE event_type = 'click') * 100.0
        / NULLIF(COUNT(*), 0), 2
    )                                                     AS ctr_pct,
    ROUND(SUM(revenue), 4)                                AS total_revenue,
    ROUND(AVG(viewability), 3)                            AS avg_viewability
FROM iceberg.ads.ad_events
WHERE event_date >= CAST(CURRENT_DATE - INTERVAL '7' DAY AS VARCHAR)
  AND is_fraud = FALSE
GROUP BY campaign_id
ORDER BY total_revenue DESC
LIMIT 20;


-- ── 3. Hourly revenue trend ───────────────────────────────────────────────────
SELECT
    DATE_TRUNC('hour', CAST(event_ts AS TIMESTAMP)) AS hour_bucket,
    platform,
    ROUND(SUM(revenue), 4)                          AS revenue,
    COUNT(*)                                        AS impressions
FROM iceberg.ads.ad_events
WHERE event_date = CAST(CURRENT_DATE AS VARCHAR)
GROUP BY 1, 2
ORDER BY 1 DESC, revenue DESC;


-- ── 4. Fraud analysis ─────────────────────────────────────────────────────────
SELECT
    event_date,
    country,
    COUNT(*)                                             AS total_events,
    COUNT(*) FILTER (WHERE is_fraud = TRUE)              AS fraud_events,
    ROUND(
        COUNT(*) FILTER (WHERE is_fraud = TRUE) * 100.0
        / NULLIF(COUNT(*), 0), 3
    )                                                    AS fraud_rate_pct
FROM iceberg.ads.ad_events
WHERE event_date >= CAST(CURRENT_DATE - INTERVAL '7' DAY AS VARCHAR)
GROUP BY event_date, country
HAVING COUNT(*) > 100
ORDER BY fraud_rate_pct DESC;


-- ── 5. Model training feature export ─────────────────────────────────────────
-- Extract enriched feature set for offline model training.
-- Export via: trino --execute "..." --output-format TSV > features.tsv
SELECT
    user_id,
    platform,
    country,
    category,
    ad_type,
    AVG(bid_floor)   AS avg_bid_floor,
    AVG(winning_bid) AS avg_cpm,
    AVG(viewability) AS avg_viewability,
    SUM(revenue)     AS lifetime_revenue,
    COUNT(*)                                              AS impression_count,
    COUNT(*) FILTER (WHERE event_type = 'click')          AS click_count,
    COUNT(*) FILTER (WHERE event_type = 'conversion')     AS conversion_count,
    ROUND(
        COUNT(*) FILTER (WHERE event_type = 'conversion') * 1.0
        / NULLIF(COUNT(*) FILTER (WHERE event_type = 'click'), 0), 4
    )                                                     AS cvr
FROM iceberg.ads.ad_events
WHERE event_date >= CAST(CURRENT_DATE - INTERVAL '30' DAY AS VARCHAR)
  AND is_fraud = FALSE
GROUP BY user_id, platform, country, category, ad_type
HAVING COUNT(*) >= 10   -- filter low-signal users
ORDER BY lifetime_revenue DESC;


-- ── 6. Query the 1-minute aggregation table (faster for dashboards) ───────────
SELECT
    window_start,
    campaign_id,
    platform,
    impressions,
    clicks,
    conversions,
    ROUND(total_revenue, 4) AS revenue,
    fraud_count
FROM iceberg.ads.campaign_1min_agg
WHERE campaign_id = 'campaign_001'
  AND window_start >= NOW() - INTERVAL '1' HOUR
ORDER BY window_start DESC;
