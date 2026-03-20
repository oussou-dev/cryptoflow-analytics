/* @bruin
name: analytics.market_dominance
type: duckdb.sql
materialization:
    type: table
depends:
    - stg.enriched_coins
    - stg.global_metrics

columns:
    - name: id
      type: string
      description: "CoinGecko coin identifier"
      checks:
        - name: not_null
    - name: dominance_pct
      type: float
      description: "Individual coin market share percentage"

custom_checks:
    - name: "dominance_sums_near_100"
      query: |
        SELECT ABS(SUM(dominance_pct) - 100) < 2
        FROM analytics.market_dominance
      value: 1
@bruin */

WITH coin_dominance AS (
    SELECT
        CURRENT_DATE AS snapshot_date,
        id,
        name,
        symbol,
        market_cap,
        price_tier,
        ROUND(
            market_cap * 100.0 / NULLIF(SUM(market_cap) OVER (), 0),
        4) AS dominance_pct,
        RANK() OVER (ORDER BY market_cap DESC) AS rank_by_mcap
    FROM stg.enriched_coins
),

tier_summary AS (
    SELECT
        price_tier,
        COUNT(*) AS coin_count,
        SUM(market_cap) AS tier_market_cap,
        ROUND(
            SUM(market_cap) * 100.0 / NULLIF(SUM(SUM(market_cap)) OVER (), 0),
        2) AS tier_dominance_pct
    FROM stg.enriched_coins
    GROUP BY price_tier
)

SELECT
    cd.snapshot_date,
    cd.id,
    cd.name,
    cd.symbol,
    cd.market_cap,
    cd.dominance_pct,
    cd.rank_by_mcap,
    cd.price_tier,
    ts.tier_dominance_pct,
    ts.coin_count AS coins_in_tier
FROM coin_dominance cd
LEFT JOIN tier_summary ts ON cd.price_tier = ts.price_tier
ORDER BY cd.rank_by_mcap
