/* @bruin

name: analytics.fear_greed_impact
type: duckdb.sql
description: |
  Cryptocurrency market sentiment analysis aggregated by psychological zones from the Fear & Greed Index.

  This table provides statistical insights into how often the market experiences different sentiment
  zones (extreme_fear, fear, neutral, greed, extreme_greed) and the behavioral patterns within each zone.

  The Fear & Greed Index (0-100 scale) is sourced from alternative.me and represents market sentiment
  based on volatility, market momentum, social media sentiment, surveys, Bitcoin dominance, and Google trends.

  Key insights include zone duration statistics, volatility patterns, current market state, and
  trend analysis. This is commonly used for contrarian trading signals and market timing analysis.

  Refreshes daily with the full historical dataset and includes current market position indicators.

  Operational characteristics: This analytical mart contains exactly 5 rows (one per sentiment zone)
  making it highly performant for dashboard queries. The daily refresh processes the complete
  historical dataset but produces a compact summary table ideal for real-time applications.
tags:
  - domain:finance
  - domain:crypto
  - data_type:analytics_mart
  - sentiment:fear_greed
  - update_pattern:daily_snapshot
  - sensitivity:public
  - use_case:market_timing
  - use_case:contrarian_trading
  - use_case:risk_management

materialization:
  type: table

depends:
  - stg.fear_greed_daily

columns:
  - name: analysis_date
    type: date
    description: Date when this analysis was generated (snapshot date)
    checks:
      - name: not_null
  - name: sentiment_zone
    type: string
    description: |
      Market sentiment category derived from Fear & Greed Index ranges:
      - extreme_fear: 0-25 (historically strong buy signal)
      - fear: 26-45 (potential buying opportunity)
      - neutral: 46-55 (market equilibrium)
      - greed: 56-75 (potential selling signal)
      - extreme_greed: 76-100 (historically strong sell signal)
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - extreme_fear
          - fear
          - neutral
          - greed
          - extreme_greed
  - name: days_count
    type: integer
    description: Total number of days the market spent in this sentiment zone
    checks:
      - name: not_null
      - name: positive
  - name: avg_index_value
    type: float
    description: Average Fear & Greed index value within this zone (0-100 scale)
    checks:
      - name: not_null
      - name: positive
      - name: max
        value: 100
  - name: min_value
    type: integer
    description: Lowest index value recorded in this sentiment zone
    checks:
      - name: not_null
      - name: positive
      - name: max
        value: 100
  - name: max_value
    type: integer
    description: Highest index value recorded in this sentiment zone
    checks:
      - name: not_null
      - name: positive
      - name: max
        value: 100
  - name: std_dev
    type: float
    description: Standard deviation of index values within the zone (measures volatility)
    checks:
      - name: not_null
      - name: min
        value: 0
  - name: pct_of_total_days
    type: float
    description: Percentage of total observation period spent in this zone
    checks:
      - name: not_null
      - name: positive
      - name: max
        value: 100
  - name: current_value
    type: integer
    description: Most recent Fear & Greed Index reading
    checks:
      - name: not_null
      - name: positive
      - name: max
        value: 100
  - name: current_zone
    type: string
    description: Current market sentiment zone based on latest index value
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - extreme_fear
          - fear
          - neutral
          - greed
          - extreme_greed
  - name: current_ma_7d
    type: float
    description: Current 7-day moving average of the index (smoothed trend indicator)
    checks:
      - name: not_null
      - name: positive
      - name: max
        value: 100
  - name: current_trend
    type: string
    description: |
      Current sentiment trend direction based on 7d vs 14d moving averages:
      - improving: short-term average above long-term (sentiment recovering)
      - deteriorating: short-term average below long-term (sentiment declining)
      - flat: averages are equal (sentiment stable)
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - improving
          - deteriorating
          - flat

custom_checks:
  - name: all_zones_represented
    value: 1
    query: |
      SELECT COUNT(DISTINCT sentiment_zone) >= 3
      FROM analytics.fear_greed_impact
  - name: percentage_totals_100
    value: 1
    query: |
      SELECT ABS(SUM(pct_of_total_days) - 100.0) < 0.1
      FROM analytics.fear_greed_impact
  - name: logical_zone_ranges
    value: 1
    query: |-
      SELECT COUNT(*) = 0
      FROM analytics.fear_greed_impact
      WHERE (sentiment_zone = 'extreme_fear' AND (min_value > 25 OR max_value > 25))
         OR (sentiment_zone = 'fear' AND (min_value > 45 OR max_value < 26))
         OR (sentiment_zone = 'neutral' AND (min_value > 55 OR max_value < 46))
         OR (sentiment_zone = 'greed' AND (min_value > 75 OR max_value < 56))
         OR (sentiment_zone = 'extreme_greed' AND min_value < 76)

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
        COALESCE(ROUND(STDDEV(value), 1), 0.0) AS std_dev,
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
