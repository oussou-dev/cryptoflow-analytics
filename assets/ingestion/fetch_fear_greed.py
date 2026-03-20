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

  Note: The API returns timestamps in Unix format which are converted to dates during ingestion. Original
  text classifications from the API are preserved alongside the numeric values for downstream analysis.
connection: duckdb-default
tags:
  - domain:finance
  - data_type:external_source
  - sensitivity:public
  - pipeline_role:raw_ingestion
  - update_pattern:daily_append
  - source:alternative_me_api
  - sentiment:fear_greed_index
  - use_case:market_timing
  - use_case:sentiment_analysis

materialization:
  type: table

secrets:
  - key: duckdb-default
    inject_as: duckdb-default

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
      This integer represents the daily composite sentiment score calculated by alternative.me.
    checks:
      - name: not_null
      - name: positive
  - name: value_classification
    type: VARCHAR
    description: |
      Human-readable sentiment classification provided by alternative.me API corresponding to the numeric value.
      Used for reporting and user-facing displays. Original text casing from API is preserved.
      Maps directly to the sentiment zones used in downstream analytics transformations.
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
    checks:
      - name: not_null
      - name: unique
  - name: ingested_at
    type: TIMESTAMP
    description: |
      UTC timestamp when this record was ingested into the pipeline.
      Used for data lineage tracking and debugging ingestion issues.
      All records from the same daily run will have identical ingested_at values.

custom_checks:
  - name: at_least_30_days_of_data
    value: 1
    query: |
      SELECT COUNT(*) >= 30 FROM raw.fear_greed_index
  - name: values_within_valid_range
    value: 1
    query: SELECT COUNT(*) = 0 FROM raw.fear_greed_index WHERE value < 0 OR value > 100

@bruin"""

import pandas as pd
import requests
from datetime import datetime, timezone


def materialize():
    """Fetch 90 days of Fear & Greed Index from alternative.me API."""

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
