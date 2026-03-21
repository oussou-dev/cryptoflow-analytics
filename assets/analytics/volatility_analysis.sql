/* @bruin

name: analytics.volatility_analysis
type: duckdb.sql
connection: motherduck-default
description: |
  Comprehensive volatility analysis for the top 50 cryptocurrencies by market cap, calculating composite
  volatility scores across multiple timeframes (24h, 7d, 30d) and intraday spreads. The analysis produces
  five-tier volatility classifications from 'stable' to 'ultra_volatile' using a weighted scoring methodology.

  The composite volatility score combines:
  - Absolute 24-hour price changes (30% weight)
  - Intraday high/low spreads (30% weight)
  - Normalized 7-day price movements (20% weight)
  - Normalized 30-day price movements (20% weight)

  This table serves portfolio managers and traders for risk assessment, position sizing, and identifying
  trading opportunities based on volatility characteristics. Volume activity levels provide additional
  context for liquidity-adjusted volatility assessment.
tags:
  - domain:finance
  - domain:crypto
  - data_type:risk_metrics
  - data_type:volatility_analysis
  - pipeline_role:analytics
  - update_pattern:daily_snapshot
  - refresh_cadence:daily_morning
  - sensitivity:public
  - use_case:risk_management
  - use_case:portfolio_optimization
  - use_case:trading_signals
  - audience:portfolio_managers
  - audience:risk_analysts
  - audience:trading_algorithms
  - performance:fast_query
  - record_count:50
  - data_quality:high
  - business_critical:high

materialization:
  type: table

depends:
  - stg.enriched_coins

columns:
  - name: analysis_date
    type: date
    description: Date when the volatility analysis was performed (CURRENT_DATE)
    checks:
      - name: not_null
  - name: id
    type: string
    description: CoinGecko unique identifier for the cryptocurrency
    checks:
      - name: not_null
  - name: name
    type: string
    description: Full name of the cryptocurrency (e.g., 'Bitcoin', 'Ethereum')
    checks:
      - name: not_null
  - name: symbol
    type: string
    description: Trading symbol/ticker of the cryptocurrency (e.g., 'BTC', 'ETH')
    checks:
      - name: not_null
  - name: market_cap_rank
    type: integer
    description: Current ranking by market capitalization (limited to top 50 in analysis)
    checks:
      - name: not_null
      - name: positive
  - name: price_tier
    type: string
    description: Market capitalization tier classification (mega_cap, large_cap, etc.)
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - mega_cap
          - large_cap
          - mid_cap
          - small_cap
          - micro_cap
  - name: current_price
    type: float
    description: Current price in USD at time of analysis
    checks:
      - name: not_null
      - name: positive
  - name: intraday_spread_pct
    type: float
    description: Intraday volatility measure calculated as (high_24h - low_24h) / low_24h * 100. Represents percentage spread between daily high and low prices. Typical ranges 0.5-15% for stable coins, 20-200%+ for highly volatile assets.
    checks:
      - name: not_null
      - name: positive
  - name: abs_change_24h
    type: float
    description: Absolute value of 24-hour price change percentage (magnitude of movement)
    checks:
      - name: not_null
  - name: abs_change_7d
    type: float
    description: Absolute value of 7-day price change percentage (magnitude of movement)
    checks:
      - name: not_null
  - name: abs_change_30d
    type: float
    description: Absolute value of 30-day price change percentage (magnitude of movement)
    checks:
      - name: not_null
  - name: volatility_score
    type: float
    description: |
      Composite volatility metric combining multi-timeframe price movements with weighted averages.
      Calculated as: (24h_change * 0.3) + (intraday_spread * 0.3) + (7d_change/7 * 0.2) + (30d_change/30 * 0.2).
      Typical ranges: 0-2 (stable), 2-8 (moderate), 8-15 (high), >15 (ultra volatile). Used for position sizing and risk assessment.
    checks:
      - name: not_null
      - name: positive
  - name: volatility_tier
    type: string
    description: Five-tier volatility classification based on composite scoring (>15=ultra_volatile, >8=high, >3=moderate, >1=low, <=1=stable)
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - ultra_volatile
          - high
          - moderate
          - low
          - stable
  - name: distance_from_ath_pct
    type: float
    description: Percentage distance from all-time high (negative values indicate drawdown from peak)
    checks:
      - name: not_null
  - name: volume_to_mcap_ratio
    type: float
    description: |
      Liquidity indicator calculated as 24-hour trading volume divided by market capitalization.
      Higher ratios indicate more active trading relative to market size. Typical ranges: 0.01-0.1 (low activity),
      0.1-0.3 (normal), 0.3-1.0 (elevated), >1.0 (abnormally high speculative interest).
    checks:
      - name: not_null
      - name: positive
  - name: volume_activity_level
    type: string
    description: Categorical volume activity classification based on volume-to-market-cap ratios
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - abnormally_high
          - elevated
          - normal
          - low

custom_checks:
  - name: volatility_scores_not_negative
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM analytics.volatility_analysis
      WHERE volatility_score < 0
  - name: top_50_coins_only
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM analytics.volatility_analysis
      WHERE market_cap_rank > 50
  - name: volatility_tiers_logically_ordered
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM analytics.volatility_analysis
      WHERE (volatility_tier = 'ultra_volatile' AND volatility_score <= 15)
         OR (volatility_tier = 'high' AND (volatility_score <= 8 OR volatility_score > 15))
         OR (volatility_tier = 'moderate' AND (volatility_score <= 3 OR volatility_score > 8))
         OR (volatility_tier = 'low' AND (volatility_score <= 1 OR volatility_score > 3))
         OR (volatility_tier = 'stable' AND volatility_score > 1)
  - name: daily_single_snapshot
    value: 1
    query: SELECT COUNT(DISTINCT analysis_date) = 1 FROM analytics.volatility_analysis
  - name: intraday_spreads_realistic
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM analytics.volatility_analysis
      WHERE intraday_spread_pct < 0 OR intraday_spread_pct > 500
  - name: volume_ratios_reasonable
    value: 1
    query: |-
      SELECT COUNT(*) = 0 FROM analytics.volatility_analysis
      WHERE volume_to_mcap_ratio < 0 OR volume_to_mcap_ratio > 10

@bruin */

SELECT
    CURRENT_DATE AS analysis_date,
    id,
    name,
    symbol,
    market_cap_rank,
    price_tier,
    current_price,

    -- Intraday spread as primary volatility measure
    intraday_spread_pct,

    -- Multi-horizon absolute price changes (magnitude of movement)
    ABS(price_change_pct_24h) AS abs_change_24h,
    ABS(price_change_pct_7d) AS abs_change_7d,
    ABS(price_change_pct_30d) AS abs_change_30d,

    -- Composite volatility score (weighted average of absolute changes)
    ROUND(
        (ABS(price_change_pct_24h) * 0.3)
        + (intraday_spread_pct * 0.3)
        + (ABS(price_change_pct_7d) / 7.0 * 0.2)
        + (ABS(price_change_pct_30d) / 30.0 * 0.2),
    2) AS volatility_score,

    -- Volatility tier
    CASE
        WHEN (ABS(price_change_pct_24h) * 0.3)
             + (intraday_spread_pct * 0.3)
             + (ABS(price_change_pct_7d) / 7.0 * 0.2)
             + (ABS(price_change_pct_30d) / 30.0 * 0.2) > 15 THEN 'ultra_volatile'
        WHEN (ABS(price_change_pct_24h) * 0.3)
             + (intraday_spread_pct * 0.3)
             + (ABS(price_change_pct_7d) / 7.0 * 0.2)
             + (ABS(price_change_pct_30d) / 30.0 * 0.2) > 8  THEN 'high'
        WHEN (ABS(price_change_pct_24h) * 0.3)
             + (intraday_spread_pct * 0.3)
             + (ABS(price_change_pct_7d) / 7.0 * 0.2)
             + (ABS(price_change_pct_30d) / 30.0 * 0.2) > 3  THEN 'moderate'
        WHEN (ABS(price_change_pct_24h) * 0.3)
             + (intraday_spread_pct * 0.3)
             + (ABS(price_change_pct_7d) / 7.0 * 0.2)
             + (ABS(price_change_pct_30d) / 30.0 * 0.2) > 1  THEN 'low'
        ELSE 'stable'
    END AS volatility_tier,

    -- Distance from ATH (coins near ATH tend to be less volatile downward)
    distance_from_ath_pct,

    -- Volume anomaly: ratio compared to median
    volume_to_mcap_ratio,
    CASE
        WHEN volume_to_mcap_ratio > 0.5 THEN 'abnormally_high'
        WHEN volume_to_mcap_ratio > 0.2 THEN 'elevated'
        WHEN volume_to_mcap_ratio > 0.05 THEN 'normal'
        ELSE 'low'
    END AS volume_activity_level

FROM stg.enriched_coins
WHERE market_cap_rank <= 50
ORDER BY volatility_score DESC
