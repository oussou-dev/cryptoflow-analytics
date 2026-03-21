/* @bruin

name: stg.global_metrics
type: bigquery.sql
connection: bigquery-default
description: |
  Staged global cryptocurrency market metrics with derived calculations and standardized data types for downstream analytics.

  This staging table transforms raw CoinGecko global market data by performing data type casting, precision rounding, and
  calculating derived metrics like altcoin dominance (non-BTC-ETH market share) and global volume ratios for liquidity analysis.
  The transformed dataset provides the foundational market overview for regime detection, dominance trend analysis, and
  portfolio allocation strategies across the CryptoFlow Analytics pipeline.

  Key transformations include:
  - Date standardization (VARCHAR to DATE casting)
  - Precision rounding for dominance percentages (2 decimal places)
  - Derivation of altcoin dominance as complement to BTC+ETH dominance
  - Volume-to-market-cap ratio calculation for global liquidity measurement
  - Data quality enforcement through range and relationship validations

  This table serves as the single source of truth for global market state and feeds critical downstream analytics including
  market regime classification (analytics.market_regime) and competitive landscape analysis (analytics.market_dominance).

  Data lineage: raw.global_market → stg.global_metrics → analytics layer
  Refresh pattern: Daily snapshots processed at UTC midnight, maintaining one record per calendar day
tags:
  - domain:finance
  - domain:crypto
  - data_type:staging_table
  - data_type:market_data
  - pipeline_role:staging
  - update_pattern:daily_snapshot
  - sensitivity:public
  - source:coingecko_api
  - use_case:market_analysis
  - use_case:regime_detection
  - use_case:dominance_analysis
  - contains:derived_metrics

materialization:
  type: table

depends:
  - raw.global_market

columns:
  - name: snapshot_date
    type: DATE
    description: |
      Standardized date of the global market snapshot, cast from VARCHAR to DATE type for temporal analysis.
      Represents the calendar day (UTC) when market data was captured, used for time-series analysis and
      ensuring data freshness across the pipeline. Unique constraint enforces one snapshot per day.
    checks:
      - name: not_null
      - name: unique
  - name: total_market_cap_usd
    type: DOUBLE
    description: |
      Total cryptocurrency market capitalization in USD across all coins tracked by CoinGecko.
      Represents the aggregate market value and serves as the denominator for dominance calculations.
      Typically ranges from $800B to $3T+, used in volume ratio calculations and market regime analysis.
    checks:
      - name: not_null
      - name: positive
  - name: btc_dominance
    type: DOUBLE
    description: |
      Bitcoin's market capitalization as percentage of total crypto market cap, rounded to 2 decimal places.
      Key indicator of market sentiment and risk appetite - typically ranges 30-70%. Higher dominance often
      indicates risk-off behavior or bear market conditions. Used in regime detection and allocation strategies.
    checks:
      - name: not_null
      - name: positive
  - name: altcoin_dominance
    type: DOUBLE
    description: |
      Derived metric calculating combined market share of all cryptocurrencies excluding Bitcoin and Ethereum.
      Formula: 100 - btc_dominance - eth_dominance, rounded to 2 decimal places. Represents the "altcoin season"
      indicator where values >50% suggest broad altcoin strength versus major assets.
    checks:
      - name: not_null
      - name: positive
  - name: total_volume_usd
    type: DOUBLE
    description: |
      Total 24-hour trading volume in USD across all cryptocurrencies tracked by CoinGecko.
      Critical liquidity metric used to calculate global volume-to-market-cap ratios. Higher volumes
      typically indicate increased market activity and institutional participation.
    checks:
      - name: not_null
      - name: positive
  - name: eth_dominance
    type: DOUBLE
    description: |
      Ethereum's market capitalization as percentage of total crypto market cap, rounded to 2 decimal places.
      Usually ranges 8-25% and inversely correlates with BTC dominance during altcoin cycles. Key metric for
      Layer 1 competition analysis and DeFi ecosystem health assessment.
    checks:
      - name: not_null
      - name: positive
  - name: active_cryptocurrencies
    type: BIGINT
    description: |
      Count of actively tracked cryptocurrencies on CoinGecko with recent price data and market activity.
      Provides context for market breadth and ecosystem growth. Typically ranges 8,000-12,000+ and growing.
      Used to assess market maturation and diversification trends.
    checks:
      - name: not_null
      - name: positive
  - name: markets
    type: BIGINT
    description: |
      Total number of cryptocurrency trading markets/pairs tracked across all exchanges on CoinGecko.
      Indicates market fragmentation and trading venue diversity. Usually exceeds 50,000+ markets,
      providing context for liquidity distribution and market structure analysis.
    checks:
      - name: not_null
      - name: positive
  - name: market_cap_change_24h_pct
    type: DOUBLE
    description: |
      24-hour percentage change in total cryptocurrency market capitalization, rounded to 2 decimal places.
      Key momentum indicator used in market regime detection algorithms. Typically ranges ±10% during normal
      periods, can reach ±50% during extreme market conditions. Direct input for regime scoring.
    checks:
      - name: not_null
  - name: global_volume_ratio
    type: DOUBLE
    description: |
      Derived liquidity metric calculating total_volume_usd / total_market_cap_usd ratio, rounded to 4 decimal places.
      Higher ratios (>0.10) indicate active trading and market liquidity, lower ratios suggest consolidation.
      Used in market regime analysis and liquidity assessment. Handles division-by-zero edge cases.
    checks:
      - name: not_null
      - name: positive
  - name: ingested_at
    type: TIMESTAMP
    description: |
      UTC timestamp when the record was originally ingested from CoinGecko API into the raw layer.
      Preserved for data lineage tracking, debugging ingestion issues, and ensuring data freshness.
      Generated automatically by the ingestion process, not the staging transformation.
    checks:
      - name: not_null

custom_checks:
  - name: dominance_sums_reasonable
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM stg.global_metrics
      WHERE (btc_dominance + eth_dominance) > 100
  - name: dominance_values_within_range
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM stg.global_metrics
      WHERE btc_dominance < 20 OR btc_dominance > 80
        OR eth_dominance < 5 OR eth_dominance > 30
  - name: volume_ratio_reasonable
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM stg.global_metrics
      WHERE global_volume_ratio > 2.0
  - name: market_counts_logical
    value: 1
    query: |-
      SELECT COUNT(*) = 0 FROM stg.global_metrics
      WHERE markets = 0

@bruin */

SELECT
    CAST(snapshot_date AS DATE) AS snapshot_date,
    total_market_cap_usd,
    total_volume_usd,
    ROUND(btc_dominance, 2) AS btc_dominance,
    ROUND(eth_dominance, 2) AS eth_dominance,
    ROUND(100 - btc_dominance - eth_dominance, 2) AS altcoin_dominance,
    active_cryptocurrencies,
    markets,
    ROUND(market_cap_change_24h_pct, 2) AS market_cap_change_24h_pct,

    -- Volume-to-market-cap ratio (global liquidity)
    CASE
        WHEN total_market_cap_usd > 0
        THEN ROUND(total_volume_usd / total_market_cap_usd, 4)
        ELSE 0
    END AS global_volume_ratio,

    ingested_at

FROM raw.global_market
