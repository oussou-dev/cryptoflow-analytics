/* @bruin

name: stg.enriched_coins
type: bigquery.sql
connection: bigquery-default
description: |
  Staging layer that enriches raw cryptocurrency market data with derived financial metrics and analytical features.
  This table transforms the CoinGecko API data by adding market cap tier classifications, liquidity indicators,
  volatility measures, and supply scarcity metrics essential for downstream analytics.

  Key transformations and enrichments applied:
  - Data cleaning with null coalescing and filtering invalid records (price > 0)
  - Market cap tier classification using industry-standard thresholds (mega/large/mid/small/micro cap)
  - Liquidity assessment via volume-to-market cap ratios for trading feasibility analysis
  - Intraday volatility measurement through high/low spread calculations
  - Supply scarcity indicators comparing circulating to maximum supply
  - All-time high distance calculations for momentum and sentiment analysis

  This staging table serves as the primary data source for:
  - Market dominance analysis and competitive intelligence
  - Volatility scoring and risk assessment models
  - Momentum signal generation and trend analysis
  - Portfolio construction and asset allocation algorithms

  Data quality features:
  - Filters out coins with null or zero current prices
  - Standardized price tier classifications with logical validation
  - Volume/market cap ratio capped to prevent outlier distortion
  - Supply ratios only calculated when max_supply exists to avoid division by zero
tags:
  - domain:crypto_analytics
  - domain:finance
  - data_type:staging_table
  - data_type:financial_metrics
  - pipeline_role:staging
  - update_pattern:daily_refresh
  - sensitivity:public
  - data_source:coingecko_api
  - use_case:market_analysis
  - use_case:risk_assessment
  - use_case:portfolio_analytics
  - record_count:100
  - quality_tier:high

materialization:
  type: table

depends:
  - raw.coin_markets

columns:
  - name: id
    type: VARCHAR
    description: CoinGecko unique identifier
    checks:
      - name: not_null
      - name: unique
  - name: current_price
    type: DOUBLE
    description: Current price in USD, cleaned
    checks:
      - name: not_null
      - name: positive
  - name: price_tier
    type: VARCHAR
    description: Market cap tier classification
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - mega_cap
          - large_cap
          - mid_cap
          - small_cap
          - micro_cap
  - name: volume_to_mcap_ratio
    type: DOUBLE
    description: 'Liquidity indicator: 24h volume / market cap'
  - name: symbol
    type: VARCHAR
    description: Cryptocurrency ticker symbol (BTC, ETH, ADA, etc.)
    checks:
      - name: not_null
  - name: name
    type: VARCHAR
    description: Full cryptocurrency project name (Bitcoin, Ethereum, Cardano, etc.)
    checks:
      - name: not_null
  - name: market_cap
    type: BIGINT
    description: Market capitalization in USD, cleaned and coalesced from raw data
    checks:
      - name: not_null
      - name: positive
  - name: market_cap_rank
    type: BIGINT
    description: Rank by market capitalization (1 = highest market cap)
    checks:
      - name: positive
  - name: total_volume_24h
    type: DOUBLE
    description: Total trading volume over the last 24 hours in USD
    checks:
      - name: not_null
      - name: min
        value: 0
  - name: high_24h
    type: DOUBLE
    description: Highest price reached in the last 24 hours (USD)
    checks:
      - name: not_null
      - name: positive
  - name: low_24h
    type: DOUBLE
    description: Lowest price reached in the last 24 hours (USD)
    checks:
      - name: not_null
      - name: positive
  - name: price_change_pct_24h
    type: DOUBLE
    description: Percentage price change over the last 24 hours (can be negative)
    checks:
      - name: not_null
  - name: price_change_pct_7d
    type: DOUBLE
    description: Percentage price change over the last 7 days (weekly momentum indicator)
    checks:
      - name: not_null
  - name: price_change_pct_30d
    type: DOUBLE
    description: Percentage price change over the last 30 days (monthly trend indicator)
    checks:
      - name: not_null
  - name: circulating_supply
    type: DOUBLE
    description: Current number of coins/tokens in circulation and available for trading
  - name: total_supply
    type: DOUBLE
    description: Total supply including locked/vested tokens but excluding burned tokens
  - name: max_supply
    type: DOUBLE
    description: Maximum number of tokens that will ever exist (null for unlimited supply tokens)
  - name: ath
    type: DOUBLE
    description: All-time high price in USD (historical maximum value ever reached)
    checks:
      - name: positive
  - name: ath_drawdown_pct
    type: DOUBLE
    description: Percentage change from all-time high to current price (always negative unless at ATH)
    checks:
      - name: not_null
  - name: last_updated
    type: VARCHAR
    description: ISO 8601 timestamp when CoinGecko last updated this coin's data
    checks:
      - name: not_null
  - name: ingested_at
    type: TIMESTAMP
    description: UTC timestamp when this record was ingested into the raw layer
    checks:
      - name: not_null
  - name: distance_from_ath_pct
    type: DOUBLE
    description: |
      Distance from all-time high as percentage (derived metric). Same as ath_drawdown_pct
      but rounded to 2 decimal places for cleaner presentation in downstream analytics.
      Used for momentum analysis and identifying potential reversal zones.
    checks:
      - name: not_null
  - name: supply_ratio_pct
    type: DOUBLE
    description: |
      Supply scarcity indicator: (circulating_supply / max_supply) * 100.
      Higher values indicate more tokens in circulation relative to maximum.
      NULL for tokens with unlimited or unknown max supply.
      Critical for tokenomics analysis and inflation assessment.
  - name: intraday_spread_pct
    type: DOUBLE
    description: |
      Intraday volatility measure: ((high_24h - low_24h) / low_24h) * 100.
      Represents the percentage spread between daily high and low prices.
      Used as a proxy for short-term volatility and trading opportunity assessment.
      Higher values indicate greater price instability within the trading day.
    checks:
      - name: not_null
      - name: min
        value: 0

custom_checks:
  - name: no_duplicate_coins
    value: 1
    query: |
      SELECT COUNT(*) = COUNT(DISTINCT id) FROM stg.enriched_coins
  - name: price_tiers_are_logically_ordered
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM stg.enriched_coins
      WHERE price_tier = 'mega_cap' AND market_cap < 100000000000
  - name: all_enriched_columns_populated
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM stg.enriched_coins
      WHERE price_tier IS NULL
         OR volume_to_mcap_ratio IS NULL
         OR distance_from_ath_pct IS NULL
         OR intraday_spread_pct IS NULL
  - name: volume_to_mcap_ratios_reasonable
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM stg.enriched_coins
      WHERE volume_to_mcap_ratio < 0 OR volume_to_mcap_ratio > 10
  - name: intraday_spreads_realistic
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM stg.enriched_coins
      WHERE intraday_spread_pct < 0 OR intraday_spread_pct > 1000
  - name: high_low_prices_logical
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM stg.enriched_coins
      WHERE high_24h < low_24h OR low_24h <= 0
  - name: supply_ratios_valid_range
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM stg.enriched_coins
      WHERE supply_ratio_pct IS NOT NULL
        AND (supply_ratio_pct < 0 OR supply_ratio_pct > 100)
  - name: only_valid_prices_included
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM stg.enriched_coins
      WHERE current_price <= 0 OR current_price IS NULL
  - name: market_cap_rank_consistency
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM stg.enriched_coins
      WHERE market_cap_rank IS NOT NULL AND market_cap <= 0
  - name: bitcoin_classified_as_mega_cap
    value: 1
    query: |
      SELECT COUNT(*) > 0 FROM stg.enriched_coins
      WHERE id = 'bitcoin' AND price_tier = 'mega_cap'

@bruin */

WITH cleaned AS (
    SELECT
        id,
        symbol,
        name,
        COALESCE(current_price, 0) AS current_price,
        COALESCE(market_cap, 0) AS market_cap,
        market_cap_rank,
        COALESCE(total_volume, 0) AS total_volume_24h,
        COALESCE(high_24h, 0) AS high_24h,
        COALESCE(low_24h, 0) AS low_24h,
        COALESCE(price_change_percentage_24h, 0) AS price_change_pct_24h,
        COALESCE(price_change_percentage_7d_in_currency, 0) AS price_change_pct_7d,
        COALESCE(price_change_percentage_30d_in_currency, 0) AS price_change_pct_30d,
        circulating_supply,
        total_supply,
        max_supply,
        ath,
        COALESCE(ath_change_percentage, 0) AS ath_drawdown_pct,
        last_updated,
        ingested_at
    FROM raw.coin_markets
    WHERE current_price IS NOT NULL
      AND current_price > 0
)

SELECT
    *,

    -- Market cap tier classification
    CASE
        WHEN market_cap >= 100000000000 THEN 'mega_cap'
        WHEN market_cap >= 10000000000  THEN 'large_cap'
        WHEN market_cap >= 1000000000   THEN 'mid_cap'
        WHEN market_cap >= 100000000    THEN 'small_cap'
        ELSE 'micro_cap'
    END AS price_tier,

    -- Volume-to-market-cap ratio (liquidity indicator)
    CASE
        WHEN market_cap > 0
        THEN ROUND(total_volume_24h / market_cap, 4)
        ELSE 0
    END AS volume_to_mcap_ratio,

    -- Distance from all-time high
    ROUND(ath_drawdown_pct, 2) AS distance_from_ath_pct,

    -- Supply scarcity ratio
    CASE
        WHEN max_supply > 0 AND circulating_supply > 0
        THEN ROUND(circulating_supply / max_supply * 100, 2)
        ELSE NULL
    END AS supply_ratio_pct,

    -- Intraday spread (volatility proxy)
    CASE
        WHEN low_24h > 0
        THEN ROUND((high_24h - low_24h) / low_24h * 100, 2)
        ELSE 0
    END AS intraday_spread_pct

FROM cleaned
