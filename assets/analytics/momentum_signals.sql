/* @bruin
name: analytics.momentum_signals
type: duckdb.sql
materialization:
    type: table
depends:
    - stg.enriched_coins
    - stg.fear_greed_daily

columns:
    - name: id
      type: string
      description: "CoinGecko coin identifier"
      checks:
        - name: not_null
    - name: momentum_score
      type: float
      description: "Composite momentum score (-50 to +100)"
      checks:
        - name: not_null
    - name: signal
      type: string
      description: "Trading signal derived from momentum + sentiment"
      checks:
        - name: not_null
        - name: accepted_values
          value: ["STRONG_BUY", "BUY", "NEUTRAL", "SELL", "STRONG_SELL"]
    - name: signal_confidence
      type: string
      description: "Confidence level of the signal"
      checks:
        - name: not_null
        - name: accepted_values
          value: ["HIGH", "MEDIUM", "LOW"]

custom_checks:
    - name: "all_top50_coins_have_signals"
      query: |
        SELECT COUNT(*) >= 40 FROM analytics.momentum_signals
      value: 1
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
