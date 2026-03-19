/* @bruin
name: stg.fear_greed_daily
type: duckdb.sql
materialization:
    type: table
depends:
    - raw.fear_greed_index

columns:
    - name: timestamp_date
      type: date
      description: "Measurement date"
      checks:
        - name: not_null
        - name: unique
    - name: value
      type: integer
      description: "Fear & Greed index (0-100)"
      checks:
        - name: not_null
    - name: classification
      type: string
      description: "Sentiment classification"
      checks:
        - name: not_null
    - name: ma_7d
      type: float
      description: "7-day moving average of the index"

custom_checks:
    - name: "moving_averages_within_range"
      query: |
        SELECT COUNT(*) = 0 FROM stg.fear_greed_daily
        WHERE ma_7d IS NOT NULL AND (ma_7d < 0 OR ma_7d > 100)
      value: 1
@bruin */

SELECT
    timestamp_date,
    value,
    value_classification AS classification,

    -- 7-day moving average
    ROUND(AVG(value) OVER (
        ORDER BY timestamp_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 1) AS ma_7d,

    -- 14-day moving average
    ROUND(AVG(value) OVER (
        ORDER BY timestamp_date
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ), 1) AS ma_14d,

    -- Day-over-day change
    value - LAG(value) OVER (ORDER BY timestamp_date) AS daily_change,

    -- Sentiment zone (simplified)
    CASE
        WHEN value <= 25 THEN 'extreme_fear'
        WHEN value <= 45 THEN 'fear'
        WHEN value <= 55 THEN 'neutral'
        WHEN value <= 75 THEN 'greed'
        ELSE 'extreme_greed'
    END AS sentiment_zone,

    ingested_at

FROM raw.fear_greed_index
ORDER BY timestamp_date
