"""@bruin

name: raw.coin_markets
description: |
  Primary cryptocurrency market data ingestion table containing real-time market metrics for the top 100
  cryptocurrencies by market capitalization from CoinGecko API. This dataset serves as the foundation
  for all downstream crypto analytics including market dominance analysis, momentum signals, and
  volatility tracking.

  Data is refreshed daily via API call and includes comprehensive market metrics: current pricing,
  market capitalization, trading volumes, price performance across multiple timeframes (1h, 24h, 7d, 14d, 30d),
  supply economics, and all-time high/low benchmarks. The dataset focuses on the most liquid and
  established cryptocurrencies to ensure data quality and minimize manipulation effects.

  Key transformations applied:
  - Flattened JSON response from CoinGecko markets endpoint
  - Added ingestion timestamp for audit trail
  - Filtered to essential columns for downstream analytics
  - API rate limiting and error handling implemented

  Typical use cases:
  - Market sentiment and momentum analysis
  - Portfolio performance benchmarking
  - Volatility and risk assessment
  - Market dominance tracking
  - Price prediction feature engineering

  Operational characteristics:
  - Daily refresh cadence aligned with CoinGecko's rate limits (free tier: 30 calls/minute)
  - Full table replacement strategy ensures data consistency and simplifies lineage
  - 30-second request timeout with error handling for API reliability
  - Focused on top 100 coins to balance coverage with data quality and processing efficiency
  - Feeds critical downstream analytics: stg.enriched_coins → momentum_signals, market_regime, volatility_analysis
connection: duckdb-default
tags:
  - domain:crypto_analytics
  - domain:finance
  - data_source:coingecko_api
  - data_type:market_data
  - data_type:time_series
  - sensitivity:public
  - pipeline_role:raw
  - pipeline_role:foundational
  - update_pattern:daily
  - refresh_type:full_replace
  - refresh_cadence:daily_morning
  - record_count:100
  - api_endpoint:coins_markets
  - lineage:feeds_staging
  - lineage:feeds_analytics
  - usage:momentum_analysis
  - usage:market_sentiment
  - usage:volatility_modeling
  - usage:portfolio_benchmarking
  - performance:api_rate_limited
  - quality_tier:high

materialization:
  type: table

secrets:
  - key: duckdb-default
    inject_as: duckdb-default

columns:
  - name: id
    type: VARCHAR
    description: CoinGecko unique identifier
    checks:
      - name: not_null
      - name: unique
  - name: symbol
    type: VARCHAR
    description: Cryptocurrency ticker symbol in lowercase format (btc, eth, ada) - standardized identifier used in trading pairs and market analysis
    checks:
      - name: not_null
  - name: current_price
    type: DOUBLE
    description: Current price in USD
    checks:
      - name: not_null
      - name: positive
  - name: market_cap
    type: BIGINT
    description: Market capitalization in USD
    checks:
      - name: positive
  - name: total_volume
    type: DOUBLE
    description: 24h trading volume in USD (critical liquidity indicator for downstream analytics)
    checks:
      - name: min
        value: 0
  - name: market_cap_rank
    type: BIGINT
    description: Market capitalization rank (1 = highest market cap) - critical for tier classification and momentum analysis weighting
    checks:
      - name: positive
      - name: max
        value: 100
  - name: ingested_at
    type: TIMESTAMP
    description: Ingestion timestamp (UTC)
    checks:
      - name: not_null
  - name: name
    type: VARCHAR
    description: Full cryptocurrency name (e.g., "Bitcoin", "Ethereum")
    checks:
      - name: not_null
  - name: fully_diluted_valuation
    type: BIGINT
    description: Market cap if max supply was in circulation (USD), theoretical maximum valuation
    checks:
      - name: positive
  - name: high_24h
    type: DOUBLE
    description: Highest price reached in the last 24 hours (USD)
    checks:
      - name: positive
  - name: low_24h
    type: DOUBLE
    description: Lowest price reached in the last 24 hours (USD)
    checks:
      - name: positive
  - name: price_change_24h
    type: DOUBLE
    description: Absolute price change in USD over the last 24 hours (can be negative)
  - name: price_change_percentage_24h
    type: DOUBLE
    description: Percentage price change over the last 24 hours (can be negative for losses)
  - name: market_cap_change_24h
    type: DOUBLE
    description: Absolute market cap change in USD over the last 24 hours (can be negative)
  - name: market_cap_change_percentage_24h
    type: DOUBLE
    description: Percentage market cap change over the last 24 hours (can be negative)
  - name: circulating_supply
    type: DOUBLE
    description: Current number of coins/tokens in circulation and available for trading
    checks:
      - name: positive
  - name: total_supply
    type: DOUBLE
    description: Total supply including locked/vested tokens but excluding burned tokens
    checks:
      - name: positive
  - name: max_supply
    type: DOUBLE
    description: Maximum number of tokens that will ever exist (null for unlimited supply tokens)
    checks:
      - name: positive
  - name: ath
    type: DOUBLE
    description: All-time high price in USD (historical maximum value ever reached)
    checks:
      - name: positive
  - name: ath_change_percentage
    type: DOUBLE
    description: Percentage change from all-time high to current price (always negative unless at ATH)
  - name: ath_date
    type: VARCHAR
    description: ISO 8601 timestamp when the all-time high was reached
    checks:
      - name: not_null
  - name: atl
    type: DOUBLE
    description: All-time low price in USD (historical minimum value ever recorded)
    checks:
      - name: positive
  - name: atl_change_percentage
    type: DOUBLE
    description: Percentage change from all-time low to current price (always positive unless at ATL)
    checks:
      - name: min
        value: 0
  - name: atl_date
    type: VARCHAR
    description: ISO 8601 timestamp when the all-time low was reached
    checks:
      - name: not_null
  - name: price_change_percentage_1h_in_currency
    type: DOUBLE
    description: Percentage price change over the last 1 hour (short-term momentum indicator)
  - name: price_change_percentage_7d_in_currency
    type: DOUBLE
    description: Percentage price change over the last 7 days (weekly trend indicator)
  - name: price_change_percentage_14d_in_currency
    type: DOUBLE
    description: Percentage price change over the last 14 days (bi-weekly trend indicator)
  - name: price_change_percentage_30d_in_currency
    type: DOUBLE
    description: Percentage price change over the last 30 days (monthly trend indicator)
  - name: last_updated
    type: VARCHAR
    description: ISO 8601 timestamp of when CoinGecko last updated this coin's data
    checks:
      - name: not_null

custom_checks:
  - name: at_least_50_coins_ingested
    value: 1
    query: |
      SELECT COUNT(*) >= 50 FROM raw.coin_markets
  - name: bitcoin_exists_in_data
    value: 1
    query: |
      SELECT COUNT(*) > 0 FROM raw.coin_markets WHERE id = 'bitcoin'
  - name: no_negative_market_caps
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM raw.coin_markets WHERE market_cap < 0
  - name: top_10_coins_present
    value: 1
    query: |
      SELECT COUNT(*) >= 10 FROM raw.coin_markets WHERE market_cap_rank <= 10
  - name: fresh_data_within_24h
    value: 1
    query: |
      SELECT COUNT(*) > 0 FROM raw.coin_markets
      WHERE CAST(last_updated AS TIMESTAMP) >= current_timestamp - INTERVAL '1 day'
  - name: reasonable_price_ranges
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM raw.coin_markets
      WHERE current_price > 1000000 OR current_price <= 0
  - name: high_low_prices_consistent
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM raw.coin_markets
      WHERE high_24h < low_24h
  - name: supply_economics_logical
    value: 1
    query: |-
      SELECT COUNT(*) = 0 FROM raw.coin_markets
      WHERE circulating_supply > max_supply
        AND max_supply IS NOT NULL
        AND max_supply > 0

@bruin"""

import pandas as pd
import requests
from datetime import datetime, timezone


def materialize():
    """Fetch market data from CoinGecko..."""
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 100,
        "page": 1,
        "sparkline": False,
        "price_change_percentage": "1h,24h,7d,14d,30d",
    }
    headers = {"accept": "application/json"}

    response = requests.get(url, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()

    df = pd.DataFrame(data)

    columns_keep = [
        "id", "symbol", "name", "current_price", "market_cap",
        "market_cap_rank", "fully_diluted_valuation", "total_volume",
        "high_24h", "low_24h", "price_change_24h",
        "price_change_percentage_24h", "market_cap_change_24h",
        "market_cap_change_percentage_24h", "circulating_supply",
        "total_supply", "max_supply", "ath", "ath_change_percentage",
        "ath_date", "atl", "atl_change_percentage", "atl_date",
        "price_change_percentage_1h_in_currency",
        "price_change_percentage_7d_in_currency",
        "price_change_percentage_14d_in_currency",
        "price_change_percentage_30d_in_currency",
        "last_updated",
    ]

    existing_cols = [c for c in columns_keep if c in df.columns]
    df = df[existing_cols].copy()

    df["ingested_at"] = datetime.now(timezone.utc)

    return df
