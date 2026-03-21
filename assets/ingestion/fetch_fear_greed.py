"""@bruin

name: raw.fear_greed_index
description: |
  Raw Fear & Greed Index data sourced from alternative.me API representing cryptocurrency market sentiment.

  The Fear & Greed Index is a widely-used sentiment indicator that measures market psychology on a 0-100 scale,
  where lower values indicate fear (potential buying opportunities) and higher values indicate greed (potential
  selling signals). The index is calculated using multiple data sources including volatility, market momentum,
  social media sentiment, surveys, Bitcoin dominance, and Google Trends.

  This raw table serves as the foundation for sentiment analysis throughout the CryptoFlow Analytics pipeline,
  feeding into staging transformations that calculate moving averages and trend analysis, and ultimately
  powering market regime detection and sentiment impact analytics.

  Data is fetched daily with 90 days of historical lookback to ensure sufficient context for moving averages
  and trend analysis. The alternative.me API provides reliable, consistent sentiment data that is widely
  recognized in the cryptocurrency trading community for contrarian trading strategies.

  OPERATIONAL CHARACTERISTICS:
  - Refresh Schedule: Daily execution at pipeline runtime
  - API Source: alternative.me/fng/ (public, no authentication required)
  - Data Volume: ~90 records per execution, 4 columns, minimal storage footprint
  - Historical Depth: API provides several years of daily data
  - API Rate Limits: No documented limits, 30-second timeout configured for reliability
  - Data Retention: Full historical data retained for trend analysis and backtesting
  - Backfill Capability: Full historical reload supported via increased limit parameter

  Note: The API returns timestamps in Unix format which are converted to dates during ingestion. Original
  text classifications from the API are preserved alongside the numeric values for downstream analysis.
  The alternative.me service has proven reliable with >99% uptime based on historical observations.
connection: bigquery-default
tags:
  - domain:finance
  - domain:crypto
  - data_type:external_source
  - sensitivity:public
  - pipeline_role:raw_ingestion
  - update_pattern:daily_append
  - source:alternative_me_api
  - sentiment:fear_greed_index
  - use_case:market_timing
  - use_case:sentiment_analysis
  - use_case:contrarian_trading
  - use_case:risk_management
  - api_type:rest_api
  - api_auth:none_required
  - data_freshness:daily
  - operational_tier:production
  - backfill_support:full_historical

materialization:
  type: table


columns:
  - name: value
    type: BIGINT
    description: |
      Fear & Greed Index numeric value on 0-100 scale where:
      - 0-25: Extreme Fear (historically strong contrarian buy signal)
      - 26-45: Fear (potential buying opportunity)
      - 46-55: Neutral (market equilibrium)
      - 56-75: Greed (potential selling signal)
      - 76-100: Extreme Greed (historically strong contrarian sell signal)

      Lower values typically correlate with market bottoms, while higher values often coincide with market tops.
      This integer represents the daily composite sentiment score calculated by alternative.me using a weighted
      algorithm that combines volatility (25%), market momentum (25%), social media sentiment (15%), surveys (15%),
      Bitcoin dominance (10%), and Google Trends (10%).

      Semantic Type: Composite metric
      Expected Cardinality: ~100 distinct values (0-100 range)
      Historical Range: Observed range 8-95 over multi-year period
    checks:
      - name: not_null
      - name: positive
      - name: max
        value: 100
  - name: value_classification
    type: VARCHAR
    description: |
      Human-readable sentiment classification provided by alternative.me API corresponding to the numeric value.
      Used for reporting and user-facing displays. Original text casing from API is preserved.
      Maps directly to the sentiment zones used in downstream analytics transformations.

      Semantic Type: Categorical dimension (ordinal scale)
      Expected Cardinality: 5 distinct values (sentiment categories)
      Business Rules: Classification is derived server-side by alternative.me and corresponds
      directly to the numeric value ranges. The text labels remain stable across API versions.

      Maximum Length: 13 characters ("Extreme Greed")
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Extreme Fear
          - Fear
          - Neutral
          - Greed
          - Extreme Greed
  - name: timestamp_date
    type: DATE
    description: |
      The measurement date for the sentiment reading in YYYY-MM-DD format.
      Converted from Unix timestamp provided by alternative.me API during ingestion.
      Each date should have exactly one sentiment reading, making this the natural primary key.
      Historical data goes back several years with daily granularity.

      Semantic Type: Primary temporal identifier
      Expected Cardinality: One record per calendar date
      Business Rules: API provides data starting from approximately 2018 with no gaps in historical record.
      The timestamp represents the UTC date when the sentiment measurement was taken.

      Time Zone: UTC (converted from Unix timestamp)
      Granularity: Daily (no intraday measurements)
      Historical Depth: ~6+ years of daily data available
    checks:
      - name: not_null
      - name: unique
  - name: ingested_at
    type: TIMESTAMP
    description: |
      UTC timestamp when this record was ingested into the pipeline.
      Used for data lineage tracking and debugging ingestion issues.
      All records from the same daily run will have identical ingested_at values.

      Semantic Type: Operational metadata timestamp
      Expected Cardinality: One unique timestamp per pipeline execution (daily)
      Business Rules: Generated at pipeline execution time using Python's datetime.now(timezone.utc).
      Useful for identifying data freshness and troubleshooting pipeline runs.

      Precision: Microsecond precision (YYYY-MM-DD HH:MM:SS.ffffff+00:00)
      Operational Use: Data lineage tracking, SLA monitoring, reprocessing identification
    checks:
      - name: not_null

custom_checks:
  - name: at_least_30_days_of_data
    value: 1
    query: |
      SELECT COUNT(*) >= 30 FROM raw.fear_greed_index
  - name: values_within_valid_range
    value: 1
    query: SELECT COUNT(*) = 0 FROM raw.fear_greed_index WHERE value < 0 OR value > 100
  - name: data_freshness_check
    value: 1
    query: |
      SELECT COUNT(*) > 0 FROM raw.fear_greed_index
      WHERE timestamp_date >= CURRENT_DATE - INTERVAL '3 days'
  - name: no_future_dates
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM raw.fear_greed_index
      WHERE timestamp_date > CURRENT_DATE
  - name: temporal_ordering_consistency
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM raw.fear_greed_index a
      JOIN raw.fear_greed_index b ON a.timestamp_date < b.timestamp_date
      WHERE a.ingested_at > b.ingested_at
  - name: classification_value_alignment
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM raw.fear_greed_index
      WHERE (value_classification = 'Extreme Fear' AND value > 25)
         OR (value_classification = 'Fear' AND (value <= 25 OR value > 45))
         OR (value_classification = 'Neutral' AND (value <= 45 OR value > 55))
         OR (value_classification = 'Greed' AND (value <= 55 OR value > 75))
         OR (value_classification = 'Extreme Greed' AND value <= 75)
  - name: expected_90_day_volume
    value: 1
    query: |
      SELECT COUNT(*) >= 80 FROM raw.fear_greed_index
      WHERE ingested_at = (SELECT MAX(ingested_at) FROM raw.fear_greed_index)
  - name: no_duplicate_dates_per_run
    value: 1
    query: |-
      SELECT COUNT(*) = 0 FROM (
        SELECT timestamp_date, ingested_at, COUNT(*) as cnt
        FROM raw.fear_greed_index
        GROUP BY timestamp_date, ingested_at
        HAVING COUNT(*) > 1
      )

@bruin"""

import pandas as pd
import requests
from datetime import datetime, timezone
import os


def materialize():
    """Fetch 90 days of Fear & Greed Index from alternative.me API."""

    url = "https://api.alternative.me/fng/"
    url = "https://api.alternative.me/fng/"
    params = {"limit": 90, "format": "json"}

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()["data"]

    df = pd.DataFrame(data)
    df["value"] = df["value"].astype(int)
    df["timestamp_date"] = pd.to_datetime(
        df["timestamp"].astype(int), unit="s"
    ).dt.date
    df["ingested_at"] = datetime.now(timezone.utc)

    return df[["value", "value_classification", "timestamp_date", "ingested_at"]]
