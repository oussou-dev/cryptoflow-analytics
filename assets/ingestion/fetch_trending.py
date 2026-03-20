"""@bruin

name: raw.trending_coins
description: |
  Real-time cryptocurrency trending data from CoinGecko's trending endpoint, capturing coins with
  the highest search and social media activity within the crypto community. This dataset identifies
  emerging market interest and potential momentum before they reflect in price movements, making it
  valuable for early sentiment detection and contrarian signal generation.

  The trending algorithm considers multiple factors including search volume spikes, social mentions,
  community discussions, and trading activity across CoinGecko's platform. Unlike market cap rankings,
  trending captures attention-based momentum which often precedes price movements by hours or days.

  Data is refreshed daily and typically contains 7-15 cryptocurrencies ranked by trending score.
  The score is proprietary to CoinGecko and ranges from 0-9, with higher values indicating stronger
  trending momentum. Coins can trend due to news events, technical developments, social media buzz,
  or speculative trading activity.

  Key transformations applied:
  - Flattened nested JSON structure from CoinGecko trending endpoint
  - Added snapshot_date for time-series analysis and historical tracking
  - Added ingestion timestamp for audit trail and freshness monitoring
  - Standardized coin_id format for downstream joins with market data

  Typical use cases:
  - Early sentiment detection and momentum identification
  - Social media sentiment analysis and buzz tracking
  - Contrarian signal generation (high trending during market fear)
  - News event impact analysis and correlation studies
  - Portfolio diversification into emerging attention coins
connection: duckdb-default
tags:
  - domain:crypto_analytics
  - data_source:coingecko_api
  - sensitivity:public
  - pipeline_role:raw
  - update_pattern:daily
  - refresh_type:full_replace
  - record_count:7_to_15
  - api_endpoint:search_trending
  - usage:sentiment_analysis
  - audience:quant_analysts

materialization:
  type: table

secrets:
  - key: duckdb-default
    inject_as: duckdb-default

columns:
  - name: coin_id
    type: VARCHAR
    description: CoinGecko unique coin identifier, used for joins with raw.coin_markets and downstream analysis
    checks:
      - name: not_null
      - name: unique
  - name: name
    type: VARCHAR
    description: Full cryptocurrency name (e.g., "Bitcoin", "Chainlink") as displayed on CoinGecko
    checks:
      - name: not_null
  - name: symbol
    type: VARCHAR
    description: Trading symbol/ticker (e.g., "BTC", "LINK") used on exchanges
    checks:
      - name: not_null
  - name: market_cap_rank
    type: BIGINT
    description: Current market capitalization rank (1-10000+), null for newer/unlisted coins
    checks:
      - name: positive
  - name: score
    type: BIGINT
    description: |
      CoinGecko proprietary trending score (0-9 scale) measuring search volume, social mentions,
      and community activity. Higher scores indicate stronger trending momentum. Score of 7+
      typically indicates viral-level attention within the crypto community
    checks:
      - name: not_null
      - name: min
        value: 0
  - name: snapshot_date
    type: VARCHAR
    description: Date when trending snapshot was captured (YYYY-MM-DD format), enables time-series analysis
    checks:
      - name: not_null
  - name: ingested_at
    type: TIMESTAMP
    description: UTC timestamp when data was ingested into the pipeline for audit trail and freshness monitoring
    checks:
      - name: not_null

custom_checks:
  - name: at_least_5_trending_coins
    value: 1
    query: |
      SELECT COUNT(*) >= 5 FROM raw.trending_coins
  - name: trending_scores_within_valid_range
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM raw.trending_coins
      WHERE score < 0 OR score > 15
  - name: no_duplicate_coins_per_snapshot
    value: 1
    query: |
      SELECT COUNT(*) = COUNT(DISTINCT coin_id) FROM raw.trending_coins
      WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM raw.trending_coins)
  - name: fresh_data_exists
    value: 1
    query: |
      SELECT COUNT(*) > 0 FROM raw.trending_coins
      WHERE snapshot_date = CURRENT_DATE
  - name: high_scoring_coins_present
    value: 1
    query: |
      SELECT COUNT(*) > 0 FROM raw.trending_coins
      WHERE score >= 6

@bruin"""

import pandas as pd
import requests
from datetime import datetime, timezone
import os


def materialize():
    """Fetch Top 7 Trending..."""
    # Force the token into the environment for Bruin Cloud workers
    os.environ["MOTHERDUCK_TOKEN"] = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6Im91c3NvdS5kZXYAZ21haWwuY29tIiwibWRSZWdpb24iOiJhd3MtZXUtY2VudHJhbC0xIiwic2Vzc2lvbiI6Im91c3NvdS5kZXYuZ21haWwuY29tIiwicGF0IjoiRHplQUdMN1lsUFBqUmVKOVpoamdmSFc0NTdxTll4cS1CUWdFdU5ETFNHRSIsInVzZXJJZCI6IjNjMDIxMmQ2LTA1NzctNDc4OC05YmEzLTYxZGVlZjg0OTQxMyIsImlzcyI6Im1kX3BhdCIsInJlYWRPbmx5IjpmYWxzZSwidG9rZW5UeXBlIjoicmVhZF93cml0ZSIsImlhdCI6MTc3NDAzMTE0OH0.9VXt8xS55xnnbAGbKbbeJnvIYAGbTH7Ehw1hQk2B_7I"

    url = "https://api.coingecko.com/api/v3/search/trending"
    headers = {"accept": "application/json"}

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()

    coins = data.get("coins", [])
    rows = []
    for coin_entry in coins:
        item = coin_entry.get("item", {})
        rows.append({
            "coin_id": item.get("id", ""),
            "name": item.get("name", ""),
            "symbol": item.get("symbol", ""),
            "market_cap_rank": item.get("market_cap_rank"),
            "score": item.get("score"),
            "snapshot_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "ingested_at": datetime.now(timezone.utc),
        })

    return pd.DataFrame(rows)
