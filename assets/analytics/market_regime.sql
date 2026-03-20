/* @bruin
name: analytics.market_regime
type: duckdb.sql
materialization:
    type: table
depends:
    - stg.enriched_coins
    - stg.fear_greed_daily
    - stg.global_metrics

columns:
    - name: regime
      type: string
      description: "Current market regime classification"
      checks:
        - name: not_null
        - name: accepted_values
          value: ["STRONG_BULL", "BULL", "NEUTRAL", "BEAR", "STRONG_BEAR"]
    - name: regime_score
      type: float
      description: "Composite score driving regime classification (-100 to +100)"
      checks:
        - name: not_null

custom_checks:
    - name: "exactly_one_regime_row"
      query: |
        SELECT COUNT(*) = 1 FROM analytics.market_regime
      value: 1
@bruin */

WITH market_breadth AS (
    -- How many coins are up vs down across timeframes
    SELECT
        COUNT(*) AS total_coins,
        SUM(CASE WHEN price_change_pct_24h > 0 THEN 1 ELSE 0 END) AS up_24h,
        SUM(CASE WHEN price_change_pct_24h < 0 THEN 1 ELSE 0 END) AS down_24h,
        SUM(CASE WHEN price_change_pct_7d > 0 THEN 1 ELSE 0 END) AS up_7d,
        SUM(CASE WHEN price_change_pct_7d < 0 THEN 1 ELSE 0 END) AS down_7d,
        SUM(CASE WHEN price_change_pct_30d > 0 THEN 1 ELSE 0 END) AS up_30d,
        SUM(CASE WHEN price_change_pct_30d < 0 THEN 1 ELSE 0 END) AS down_30d,
        ROUND(AVG(price_change_pct_24h), 2) AS avg_change_24h,
        ROUND(AVG(price_change_pct_7d), 2) AS avg_change_7d,
        ROUND(AVG(price_change_pct_30d), 2) AS avg_change_30d,
        ROUND(AVG(volume_to_mcap_ratio), 4) AS avg_volume_ratio
    FROM stg.enriched_coins
    WHERE market_cap_rank <= 50
),

sentiment AS (
    SELECT
        value AS fg_current,
        ma_7d AS fg_ma_7d,
        sentiment_zone
    FROM stg.fear_greed_daily
    ORDER BY timestamp_date DESC
    LIMIT 1
),

global_state AS (
    SELECT
        market_cap_change_24h_pct,
        btc_dominance,
        altcoin_dominance,
        global_volume_ratio
    FROM stg.global_metrics
    ORDER BY snapshot_date DESC
    LIMIT 1
),

regime_calc AS (
    SELECT
        -- Breadth score: % of coins in positive territory (weighted)
        ROUND(
            ((mb.up_24h * 100.0 / NULLIF(mb.total_coins, 0)) - 50) * 0.2
            + ((mb.up_7d * 100.0 / NULLIF(mb.total_coins, 0)) - 50) * 0.3
            + ((mb.up_30d * 100.0 / NULLIF(mb.total_coins, 0)) - 50) * 0.3
            + (CASE
                WHEN s.fg_current >= 75 THEN 20
                WHEN s.fg_current >= 55 THEN 10
                WHEN s.fg_current >= 45 THEN 0
                WHEN s.fg_current >= 25 THEN -10
                ELSE -20
            END) * 0.2
        , 1) AS regime_score,

        mb.total_coins,
        mb.up_24h,
        mb.down_24h,
        mb.up_7d,
        mb.down_7d,
        mb.up_30d,
        mb.down_30d,
        mb.avg_change_24h,
        mb.avg_change_7d,
        mb.avg_change_30d,
        mb.avg_volume_ratio,
        s.fg_current AS fear_greed_current,
        s.fg_ma_7d AS fear_greed_ma_7d,
        s.sentiment_zone,
        gs.market_cap_change_24h_pct,
        gs.btc_dominance,
        gs.altcoin_dominance

    FROM market_breadth mb
    CROSS JOIN sentiment s
    CROSS JOIN global_state gs
)

SELECT
    CURRENT_DATE AS analysis_date,

    -- Regime classification
    CASE
        WHEN regime_score >= 25  THEN 'STRONG_BULL'
        WHEN regime_score >= 10  THEN 'BULL'
        WHEN regime_score >= -10 THEN 'NEUTRAL'
        WHEN regime_score >= -25 THEN 'BEAR'
        ELSE 'STRONG_BEAR'
    END AS regime,

    regime_score,

    -- Market breadth indicators
    total_coins AS coins_analyzed,
    ROUND(up_24h * 100.0 / NULLIF(total_coins, 0), 1) AS pct_up_24h,
    ROUND(up_7d * 100.0 / NULLIF(total_coins, 0), 1) AS pct_up_7d,
    ROUND(up_30d * 100.0 / NULLIF(total_coins, 0), 1) AS pct_up_30d,

    -- Average performance
    avg_change_24h,
    avg_change_7d,
    avg_change_30d,

    -- Sentiment
    fear_greed_current,
    fear_greed_ma_7d,
    sentiment_zone,

    -- Global market
    market_cap_change_24h_pct,
    btc_dominance,
    altcoin_dominance,
    avg_volume_ratio,

    -- Summary narrative
    CASE
        WHEN regime_score >= 25 THEN 'Strong bullish momentum across the market. Majority of top coins are trending up with positive sentiment.'
        WHEN regime_score >= 10 THEN 'Moderately bullish conditions. Most indicators point upward but some caution warranted.'
        WHEN regime_score >= -10 THEN 'Mixed signals. Market is consolidating with no clear directional bias.'
        WHEN regime_score >= -25 THEN 'Bearish pressure building. More coins are declining and sentiment is weakening.'
        ELSE 'Strong bearish conditions. Widespread selling pressure with extreme fear in the market.'
    END AS regime_narrative

FROM regime_calc
