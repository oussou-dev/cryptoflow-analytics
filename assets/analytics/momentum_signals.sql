/* @bruin

name: analytics.momentum_signals
type: bigquery.sql
connection: bigquery-default
description: |
  Multi-timeframe momentum analysis with sentiment-enhanced trading signals for top 50 cryptocurrencies. Combines technical momentum indicators (24h/7d/30d price changes) with volume confirmation, ATH proximity analysis, and contrarian sentiment signals from Fear & Greed Index. Generates actionable buy/sell signals with confidence levels for systematic trading strategies. The momentum score ranges from -50 (strong bearish) to +100 (strong bullish), incorporating short-term momentum weighting, volume confirmation, and trend strength indicators. Contrarian approach: high momentum during extreme fear periods generates strongest buy signals.
tags:
  - domain:crypto
  - domain:finance
  - data_type:ml_feature
  - data_type:trading_signals
  - data_type:momentum_analysis
  - sensitivity:public
  - pipeline_role:mart
  - pipeline_role:analytical_output
  - update_pattern:daily
  - update_pattern:snapshot
  - refresh_cadence:daily_morning
  - usage:trading_signals
  - usage:risk_assessment
  - usage:portfolio_optimization
  - audience:quant_analysts
  - audience:portfolio_managers
  - audience:trading_algorithms
  - performance:fast_query
  - record_count:50
  - data_quality:high
  - contrarian_strategy:sentiment_based

materialization:
  type: table

depends:
  - stg.enriched_coins
  - stg.fear_greed_daily

columns:
  - name: analysis_date
    type: date
    description: Date when the analysis was performed (snapshot date)
    checks:
      - name: not_null
  - name: id
    type: string
    description: CoinGecko unique coin identifier (primary key)
    checks:
      - name: not_null
      - name: unique
  - name: name
    type: string
    description: Human-readable cryptocurrency name (e.g., "Bitcoin")
    checks:
      - name: not_null
  - name: symbol
    type: string
    description: Trading symbol/ticker (e.g., "BTC")
    checks:
      - name: not_null
  - name: current_price
    type: float
    description: Current USD price at analysis time
    checks:
      - name: not_null
      - name: positive
  - name: market_cap_rank
    type: integer
    description: CoinGecko market capitalization ranking (1-50 for this dataset)
    checks:
      - name: not_null
      - name: positive
      - name: min
        value: 1
      - name: max
        value: 50
  - name: price_tier
    type: string
    description: Market cap tier classification (mega_cap, large_cap, mid_cap, small_cap, micro_cap)
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - mega_cap
          - large_cap
          - mid_cap
          - small_cap
          - micro_cap
  - name: price_change_pct_24h
    type: float
    description: 24-hour price change percentage (short-term momentum component)
  - name: price_change_pct_7d
    type: float
    description: 7-day price change percentage (medium-term momentum component)
  - name: price_change_pct_30d
    type: float
    description: 30-day price change percentage (long-term momentum component)
  - name: volume_to_mcap_ratio
    type: float
    description: 24h trading volume divided by market cap (liquidity/interest indicator). Higher ratios indicate more active trading relative to market size. Typical range 0.01-3.0, with values >1.0 suggesting high speculative interest.
    checks:
      - name: positive
      - name: max
        value: 10
  - name: distance_from_ath_pct
    type: float
    description: Percentage distance from all-time high (negative values indicate drawdown)
  - name: momentum_score
    type: float
    description: |
      Composite momentum score ranging from -50 (strong bearish) to +100 (strong bullish). Weighted combination of: 24h momentum (max 20pts), 7d momentum (max 25pts), 30d momentum (max 25pts), volume confirmation (max 15pts), ATH proximity (max 15pts). Extreme values below -60 or above +110 indicate calculation anomalies.
    checks:
      - name: not_null
      - name: min
        value: -60
      - name: max
        value: 110
  - name: fear_greed_index
    type: integer
    description: Latest Fear & Greed Index value (0-100, lower values indicate market fear). Sourced from alternative.me API and used as contrarian sentiment signal.
    checks:
      - name: not_null
      - name: min
        value: 0
      - name: max
        value: 100
  - name: market_sentiment
    type: string
    description: Market sentiment classification based on Fear & Greed Index (Extreme Fear, Fear, Neutral, Greed, Extreme Greed). Inherited from stg.fear_greed_daily.classification. Used as contrarian indicator in signal generation logic.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Extreme Fear
          - Fear
          - Neutral
          - Greed
          - Extreme Greed
  - name: signal
    type: string
    description: |
      Contrarian trading signal combining momentum and sentiment. STRONG_BUY: high momentum (≥60) + extreme fear (<30). BUY: strong momentum (≥40). NEUTRAL: moderate momentum (≥-10). SELL/STRONG_SELL: negative momentum with graduated thresholds
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - STRONG_BUY
          - BUY
          - NEUTRAL
          - SELL
          - STRONG_SELL
  - name: signal_confidence
    type: string
    description: |
      Signal strength based on absolute momentum score magnitude. HIGH: |momentum_score| > 50, MEDIUM: > 25, LOW: ≤ 25
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - HIGH
          - MEDIUM
          - LOW

custom_checks:
  - name: all_top50_coins_have_signals
    value: 1
    query: |
      SELECT COUNT(*) >= 40 FROM analytics.momentum_signals
  - name: momentum_score_within_expected_range
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM analytics.momentum_signals
      WHERE momentum_score < -60 OR momentum_score > 110
  - name: strong_buy_signals_have_high_confidence
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM analytics.momentum_signals
      WHERE signal = 'STRONG_BUY' AND signal_confidence = 'LOW'
  - name: fear_greed_index_in_valid_range
    value: 1
    query: |-
      SELECT COUNT(*) = 0 FROM analytics.momentum_signals
      WHERE fear_greed_index < 0 OR fear_greed_index > 100
  - name: volume_ratios_reasonable
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM analytics.momentum_signals
      WHERE volume_to_mcap_ratio < 0 OR volume_to_mcap_ratio > 10
  - name: signal_distribution_reasonable
    value: 1
    query: |
      SELECT COUNT(*) >= 2 FROM (
        SELECT signal, COUNT(*) as cnt
        FROM analytics.momentum_signals
        GROUP BY signal
      ) WHERE cnt > 0
  - name: strong_buy_only_during_fear
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM analytics.momentum_signals
      WHERE signal = 'STRONG_BUY'
        AND market_sentiment NOT IN ('Extreme Fear', 'Fear')
  - name: price_changes_correlation_check
    value: 1
    query: |-
      SELECT COUNT(*) = 0 FROM analytics.momentum_signals
      WHERE price_change_pct_24h > 50 AND momentum_score < 0

@bruin */

WITH price_momentum AS (
    SELECT
        id,
        name,
        symbol,
        current_price,
        market_cap_rank,
        price_tier,
        price_change_pct_24h,
        price_change_pct_7d,
        price_change_pct_30d,
        volume_to_mcap_ratio,
        distance_from_ath_pct,
        intraday_spread_pct,

        -- Composite momentum score (range: roughly -50 to +100)
        ROUND(
            -- Short-term momentum (24h)
            (CASE
                WHEN price_change_pct_24h > 5  THEN 20
                WHEN price_change_pct_24h > 0  THEN 10
                WHEN price_change_pct_24h > -5 THEN 0
                ELSE -10
            END)
            -- Medium-term momentum (7d)
            + (CASE
                WHEN price_change_pct_7d > 15 THEN 25
                WHEN price_change_pct_7d > 0  THEN 12
                WHEN price_change_pct_7d > -10 THEN 0
                ELSE -12
            END)
            -- Long-term momentum (30d)
            + (CASE
                WHEN price_change_pct_30d > 30 THEN 25
                WHEN price_change_pct_30d > 0  THEN 12
                WHEN price_change_pct_30d > -20 THEN 0
                ELSE -12
            END)
            -- Volume confirmation
            + (CASE
                WHEN volume_to_mcap_ratio > 0.3 THEN 15
                WHEN volume_to_mcap_ratio > 0.1 THEN 8
                ELSE 0
            END)
            -- ATH proximity (nearer ATH = stronger trend)
            + (CASE
                WHEN distance_from_ath_pct > -20 THEN 15
                WHEN distance_from_ath_pct > -50 THEN 8
                ELSE 0
            END)
        , 1) AS momentum_score

    FROM stg.enriched_coins
    WHERE market_cap_rank <= 50
),

latest_fear_greed AS (
    SELECT value AS fg_value, classification AS fg_class
    FROM stg.fear_greed_daily
    ORDER BY timestamp_date DESC
    LIMIT 1
)

SELECT
    CURRENT_DATE AS analysis_date,
    pm.id,
    pm.name,
    pm.symbol,
    pm.current_price,
    pm.market_cap_rank,
    pm.price_tier,
    pm.price_change_pct_24h,
    pm.price_change_pct_7d,
    pm.price_change_pct_30d,
    pm.volume_to_mcap_ratio,
    pm.distance_from_ath_pct,
    pm.momentum_score,
    fg.fg_value AS fear_greed_index,
    fg.fg_class AS market_sentiment,

    -- Composite signal combining momentum + contrarian sentiment
    CASE
        WHEN pm.momentum_score >= 60 AND fg.fg_value < 30 THEN 'STRONG_BUY'
        WHEN pm.momentum_score >= 40                       THEN 'BUY'
        WHEN pm.momentum_score >= -10                      THEN 'NEUTRAL'
        WHEN pm.momentum_score >= -40                      THEN 'SELL'
        ELSE 'STRONG_SELL'
    END AS signal,

    -- Signal confidence
    CASE
        WHEN ABS(pm.momentum_score) > 50 THEN 'HIGH'
        WHEN ABS(pm.momentum_score) > 25 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS signal_confidence

FROM price_momentum pm
CROSS JOIN latest_fear_greed fg
ORDER BY pm.momentum_score DESC
