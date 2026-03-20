/* @bruin
name: analytics.volatility_analysis
type: duckdb.sql
materialization:
    type: table
depends:
    - stg.enriched_coins

columns:
    - name: id
      type: string
      description: "CoinGecko coin identifier"
      checks:
        - name: not_null
    - name: volatility_tier
      type: string
      description: "Volatility classification"
      checks:
        - name: not_null
        - name: accepted_values
          value: ["ultra_volatile", "high", "moderate", "low", "stable"]

custom_checks:
    - name: "volatility_scores_not_negative"
      query: |
        SELECT COUNT(*) = 0 FROM analytics.volatility_analysis
        WHERE volatility_score < 0
      value: 1
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
