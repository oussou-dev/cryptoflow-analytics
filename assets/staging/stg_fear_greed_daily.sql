/* @bruin

name: stg.fear_greed_daily
type: duckdb.sql
connection: motherduck-default
description: |
  Staged Fear & Greed Index data with calculated moving averages and sentiment zones for cryptocurrency market analysis.

  This staging table transforms raw fear & greed index data from alternative.me API into a more analytical format
  by adding technical indicators (7-day and 14-day moving averages), day-over-day change calculations, and
  standardized sentiment zone classifications.

  The Fear & Greed Index (0-100 scale) is a widely-used contrarian sentiment indicator in cryptocurrency markets
  where extreme fear often signals buying opportunities and extreme greed indicates potential selling points.
  Moving averages help smooth out daily volatility to identify longer-term sentiment trends.

  This table serves as the foundation for downstream analytics including market regime detection, momentum signals,
  and sentiment impact analysis. The standardized sentiment zones (extreme_fear, fear, neutral, greed, extreme_greed)
  provide consistent categorical groupings used across multiple analytical models.

  Data lineage: raw.fear_greed_index → stg.fear_greed_daily → analytics layer
  Refresh pattern: Daily, processing the full historical dataset with window functions for moving averages
tags:
  - domain:finance
  - data_type:staging_table
  - sensitivity:public
  - pipeline_role:staging
  - update_pattern:daily_snapshot
  - source:alternative_me_api
  - sentiment:fear_greed_index
  - use_case:market_timing
  - use_case:sentiment_analysis
  - contains:technical_indicators

materialization:
  type: table

depends:
  - raw.fear_greed_index

columns:
  - name: timestamp_date
    type: DATE
    description: |
      The measurement date for the sentiment reading in YYYY-MM-DD format.
      Primary key for this table, with exactly one sentiment reading per date.
      Historical data extends several years back with daily granularity, providing
      sufficient context for trend analysis and moving averages.
    checks:
      - name: not_null
      - name: unique
  - name: value
    type: BIGINT
    description: |
      Fear & Greed Index numeric value on 0-100 scale where:
      - 0-25: Extreme Fear (historically strong contrarian buy signal)
      - 26-45: Fear (potential buying opportunity)
      - 46-55: Neutral (market equilibrium)
      - 56-75: Greed (potential selling signal)
      - 76-100: Extreme Greed (historically strong contrarian sell signal)

      This composite sentiment score from alternative.me aggregates multiple data sources
      including volatility, market momentum, social media sentiment, surveys, Bitcoin
      dominance, and Google Trends. Lower values typically correlate with market bottoms.
    checks:
      - name: not_null
      - name: positive
  - name: classification
    type: VARCHAR
    description: |
      Human-readable sentiment classification from alternative.me API corresponding to the numeric value.
      Original text classifications (e.g., "Extreme Fear", "Fear", "Neutral", "Greed", "Extreme Greed")
      are preserved from the API response. Used primarily for reporting and user-facing displays.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Extreme Fear
          - Fear
          - Neutral
          - Greed
          - Extreme Greed
  - name: ma_7d
    type: DOUBLE
    description: |
      7-day moving average of the Fear & Greed Index value, rounded to 1 decimal place.
      Calculated using a rolling window of the current day plus 6 preceding days.
      Helps smooth out daily volatility to identify short-term sentiment trends.
      NULL for the first 6 days where insufficient historical data exists.
    checks:
      - name: positive
  - name: ma_14d
    type: DOUBLE
    description: |
      14-day moving average of the Fear & Greed Index value, rounded to 1 decimal place.
      Calculated using a rolling window of the current day plus 13 preceding days.
      Provides medium-term sentiment trend analysis, commonly used alongside 7-day MA
      for crossover trading signals. NULL for the first 13 days where insufficient data exists.
    checks:
      - name: positive
  - name: daily_change
    type: BIGINT
    description: |
      Day-over-day change in the Fear & Greed Index value (current day minus previous day).
      Positive values indicate increasing fear/greed sentiment, negative values indicate
      decreasing sentiment. NULL for the first chronological record where no previous day exists.
      Range typically -50 to +50 points, with large absolute values indicating significant
      sentiment shifts that often precede major market movements.
  - name: sentiment_zone
    type: VARCHAR
    description: |
      Standardized sentiment zone classification used consistently across CryptoFlow analytics.
      Derived from the Fear & Greed Index value using consistent thresholds:
      - extreme_fear (0-25): Strong contrarian buy signal historically
      - fear (26-45): Potential buying opportunity
      - neutral (46-55): Market equilibrium, trend-following preferred
      - greed (56-75): Potential selling signal
      - extreme_greed (76-100): Strong contrarian sell signal historically

      These zones are used in downstream market regime detection and momentum signal generation.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - extreme_fear
          - fear
          - neutral
          - greed
          - extreme_greed
  - name: ingested_at
    type: TIMESTAMP
    description: |
      UTC timestamp when the source record was ingested from alternative.me API.
      Used for data lineage tracking and debugging. All records from the same daily
      pipeline run will have identical ingested_at values. Inherited from raw layer.

custom_checks:
  - name: moving_averages_within_range
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM stg.fear_greed_daily
      WHERE (ma_7d IS NOT NULL AND (ma_7d < 0 OR ma_7d > 100))
         OR (ma_14d IS NOT NULL AND (ma_14d < 0 OR ma_14d > 100))
  - name: sentiment_zone_matches_value
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM stg.fear_greed_daily
      WHERE (sentiment_zone = 'extreme_fear' AND value > 25)
         OR (sentiment_zone = 'fear' AND (value <= 25 OR value > 45))
         OR (sentiment_zone = 'neutral' AND (value <= 45 OR value > 55))
         OR (sentiment_zone = 'greed' AND (value <= 55 OR value > 75))
         OR (sentiment_zone = 'extreme_greed' AND value <= 75)
  - name: reasonable_daily_changes
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM stg.fear_greed_daily
      WHERE daily_change IS NOT NULL AND ABS(daily_change) > 80
  - name: sufficient_data_for_moving_averages
    value: 1
    query: SELECT COUNT(*) >= 14 FROM stg.fear_greed_daily

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
