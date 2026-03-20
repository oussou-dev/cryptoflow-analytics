/* @bruin
name: analytics.fear_greed_impact
type: duckdb.sql
materialization:
    type: table
depends:
    - stg.fear_greed_daily

columns:
    - name: sentiment_zone
      type: string
      description: "Aggregated sentiment zone"
      checks:
        - name: not_null
    - name: avg_index_value
      type: float
      description: "Average Fear & Greed index for the zone"

custom_checks:
    - name: "all_zones_represented"
      query: |
        SELECT COUNT(DISTINCT sentiment_zone) >= 3
        FROM analytics.fear_greed_impact
      value: 1
@bruin */

WITH daily_stats AS (
    SELECT
        timestamp_date,
        value,
        classification,
        sentiment_zone,
        ma_7d,
        ma_14d,
        daily_change,

        -- Trend direction based on moving averages
        CASE
            WHEN ma_7d > ma_14d THEN 'improving'
            WHEN ma_7d < ma_14d THEN 'deteriorating'
            ELSE 'flat'
        END AS sentiment_trend,

        -- Consecutive days in same zone
        ROW_NUMBER() OVER (
            PARTITION BY sentiment_zone
            ORDER BY timestamp_date
        ) AS days_in_zone

    FROM stg.fear_greed_daily
),

zone_analysis AS (
    SELECT
        sentiment_zone,
        COUNT(*) AS days_count,
        ROUND(AVG(value), 1) AS avg_index_value,
        MIN(value) AS min_value,
        MAX(value) AS max_value,
        ROUND(STDDEV(value), 1) AS std_dev,
        ROUND(
            COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),
        1) AS pct_of_total_days
    FROM daily_stats
    GROUP BY sentiment_zone
),

trend_summary AS (
    SELECT
        sentiment_trend,
        COUNT(*) AS trend_days,
        ROUND(AVG(value), 1) AS avg_value_during_trend
    FROM daily_stats
    WHERE sentiment_trend IS NOT NULL
    GROUP BY sentiment_trend
)

SELECT
    CURRENT_DATE AS analysis_date,
    za.sentiment_zone,
    za.days_count,
    za.avg_index_value,
    za.min_value,
    za.max_value,
    za.std_dev,
    za.pct_of_total_days,

    -- Current state
    (SELECT value FROM daily_stats ORDER BY timestamp_date DESC LIMIT 1) AS current_value,
    (SELECT sentiment_zone FROM daily_stats ORDER BY timestamp_date DESC LIMIT 1) AS current_zone,
    (SELECT ma_7d FROM daily_stats ORDER BY timestamp_date DESC LIMIT 1) AS current_ma_7d,
    (SELECT sentiment_trend FROM daily_stats ORDER BY timestamp_date DESC LIMIT 1) AS current_trend

FROM zone_analysis za
ORDER BY za.avg_index_value
