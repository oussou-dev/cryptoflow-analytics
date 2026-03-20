"""@bruin

name: raw.global_market
description: |
  Raw global cryptocurrency market data sourced from CoinGecko's /global endpoint, capturing daily snapshots
  of overall market conditions. This foundational dataset feeds the CryptoFlow Analytics medallion architecture,
  providing essential metrics for market regime detection, dominance analysis, and portfolio allocation strategies.

  Data is ingested daily via API calls and represents aggregate statistics across all tracked cryptocurrencies
  on CoinGecko. The dominance percentages are based on market cap and sum to less than 100% due to the
  presence of other cryptocurrencies beyond BTC/ETH. This raw data undergoes transformation in the staging
  layer (stg.global_metrics) where additional calculated fields like altcoin dominance and volume ratios
  are derived.

  Operational characteristics:
  - API endpoint: https://api.coingecko.com/api/v3/global (public, no auth required)
  - Rate limits: 10-30 calls/minute for free tier, sufficient for daily ingestion
  - Response time: Typically <2 seconds, with 30-second timeout configured
  - Data freshness: CoinGecko updates approximately every 5-10 minutes
  - Historical availability: Limited to current snapshot only, no historical data via this endpoint
  - Failure handling: Request timeout and HTTP error status raise exceptions

  Key usage: Essential input for market regime classification, dominance trend analysis, and global market
  health monitoring. Downstream consumers include analytics.market_regime and analytics.market_dominance.
connection: duckdb-default
tags:
  - domain:finance
  - domain:crypto
  - data_type:external_source
  - data_type:market_data
  - data_type:api_endpoint
  - pipeline_role:raw
  - update_pattern:daily_snapshot
  - sensitivity:public
  - source:coingecko_api
  - use_case:market_analysis
  - use_case:regime_detection
  - use_case:dominance_tracking
  - api_pattern:rest_endpoint
  - refresh_cadence:daily
  - data_latency:near_realtime
  - business_critical:high
  - operational_tier:foundational

materialization:
  type: table

secrets:
  - key: duckdb-default
    inject_as: duckdb-default

columns:
  - name: total_market_cap_usd
    type: DOUBLE
    description: |
      Total cryptocurrency market capitalization in USD across all coins tracked by CoinGecko.
      Represents the combined value of all cryptocurrencies and serves as a key metric for
      overall market health and size. Used in volume ratio calculations and market regime analysis.
    checks:
      - name: not_null
      - name: positive
  - name: btc_dominance
    type: DOUBLE
    description: |
      Bitcoin's market capitalization as a percentage of total crypto market cap. Typically ranges
      between 30-70% and is a key indicator of market sentiment and altcoin season dynamics.
      Higher BTC dominance often indicates risk-off behavior or bear market conditions.
    checks:
      - name: not_null
      - name: positive
      - name: max
        value: 100
  - name: eth_dominance
    type: DOUBLE
    description: |
      Ethereum's market capitalization as a percentage of total crypto market cap. Usually ranges
      between 8-25% and inversely correlates with BTC dominance during altcoin cycles.
      Key metric for Layer 1 competition and DeFi ecosystem health.
    checks:
      - name: not_null
      - name: positive
      - name: max
        value: 100
  - name: snapshot_date
    type: VARCHAR
    description: |
      ISO date (YYYY-MM-DD) when the global market snapshot was taken, generated at ingestion time
      in UTC timezone. Used for temporal analysis and ensuring data freshness across the pipeline.
    checks:
      - name: not_null
      - name: unique
  - name: total_volume_usd
    type: DOUBLE
    description: |
      Total 24-hour trading volume in USD across all cryptocurrencies. Critical metric for market
      liquidity assessment and used to calculate global volume-to-market-cap ratios in staging layer.
      Higher volumes typically indicate increased market activity and liquidity.
    checks:
      - name: not_null
      - name: positive
  - name: active_cryptocurrencies
    type: BIGINT
    description: |
      Count of actively tracked cryptocurrencies on CoinGecko with recent price data.
      Provides context for market breadth and ecosystem growth. Typically ranges from 8,000-12,000+.
    checks:
      - name: not_null
      - name: positive
  - name: markets
    type: BIGINT
    description: |
      Total number of cryptocurrency trading markets/pairs tracked across all exchanges.
      Indicates market fragmentation and trading venue diversity. Usually exceeds 50,000 markets.
    checks:
      - name: not_null
      - name: positive
  - name: market_cap_change_24h_pct
    type: DOUBLE
    description: |
      24-hour percentage change in total cryptocurrency market capitalization in USD.
      Key momentum indicator used in market regime detection. Can range from -50% to +50%
      during extreme market conditions, typically within ±10% during normal periods.
    checks:
      - name: not_null
  - name: ingested_at
    type: TIMESTAMP
    description: |
      UTC timestamp when the record was ingested into the data pipeline. Used for data lineage
      tracking, debugging ingestion issues, and ensuring data freshness. Generated automatically
      by the ingestion process.
    checks:
      - name: not_null

custom_checks:
  - name: dominance_values_reasonable
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM raw.global_market
      WHERE btc_dominance < 0 OR btc_dominance > 100
        OR eth_dominance < 0 OR eth_dominance > 100
  - name: market_cap_volume_relationship
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM raw.global_market
      WHERE total_volume_usd > total_market_cap_usd * 2
  - name: dominance_sum_logical
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM raw.global_market
      WHERE (btc_dominance + eth_dominance) > 100
  - name: snapshot_date_format
    value: 1
    query: |-
      SELECT COUNT(*) = 0 FROM raw.global_market
      WHERE snapshot_date !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
  - name: ingestion_timestamp_reasonable
    value: 1
    query: |-
      SELECT COUNT(*) = 0 FROM raw.global_market
      WHERE ingested_at > CURRENT_TIMESTAMP
        OR ingested_at < CURRENT_TIMESTAMP - INTERVAL '7 days'

@bruin"""

import pandas as pd
import requests
from datetime import datetime, timezone


def materialize():
    """Fetch global cryptocurrency market data from CoinGecko API."""

    url = "https://api.coingecko.com/api/v3/global"
    headers = {"accept": "application/json"}

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()["data"]

    row = {
        "total_market_cap_usd": data["total_market_cap"].get("usd", 0),
        "total_volume_usd": data["total_volume"].get("usd", 0),
        "btc_dominance": data["market_cap_percentage"].get("btc", 0),
        "eth_dominance": data["market_cap_percentage"].get("eth", 0),
        "active_cryptocurrencies": data.get("active_cryptocurrencies", 0),
        "markets": data.get("markets", 0),
        "market_cap_change_24h_pct": data.get(
            "market_cap_change_percentage_24h_usd", 0
        ),
        "snapshot_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "ingested_at": datetime.now(timezone.utc),
    }

    return pd.DataFrame([row])
