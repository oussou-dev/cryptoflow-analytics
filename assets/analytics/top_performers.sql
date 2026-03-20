/* @bruin
name: analytics.top_performers
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
    - name: performance_category
      type: string
      description: "Gainer or loser classification"
      checks:
        - name: not_null
        - name: accepted_values
          value: ["top_gainer_24h", "top_loser_24h", "top_gainer_7d", "top_loser_7d", "top_gainer_30d", "top_loser_30d"]

custom_checks:
    - name: "has_both_gainers_and_losers"
      query: |
        SELECT COUNT(DISTINCT performance_category) >= 4
        FROM analytics.top_performers
      value: 1
@bruin */

WITH ranked_24h AS (
    SELECT id, name, symbol, current_price, market_cap_rank, price_tier,
        price_change_pct_24h AS change_pct,
        '24h' AS timeframe,
        ROW_NUMBER() OVER (ORDER BY price_change_pct_24h DESC) AS gainer_rank,
        ROW_NUMBER() OVER (ORDER BY price_change_pct_24h ASC) AS loser_rank
    FROM stg.enriched_coins
    WHERE market_cap_rank <= 100
),

ranked_7d AS (
    SELECT id, name, symbol, current_price, market_cap_rank, price_tier,
        price_change_pct_7d AS change_pct,
        '7d' AS timeframe,
        ROW_NUMBER() OVER (ORDER BY price_change_pct_7d DESC) AS gainer_rank,
        ROW_NUMBER() OVER (ORDER BY price_change_pct_7d ASC) AS loser_rank
    FROM stg.enriched_coins
    WHERE market_cap_rank <= 100
),

ranked_30d AS (
    SELECT id, name, symbol, current_price, market_cap_rank, price_tier,
        price_change_pct_30d AS change_pct,
        '30d' AS timeframe,
        ROW_NUMBER() OVER (ORDER BY price_change_pct_30d DESC) AS gainer_rank,
        ROW_NUMBER() OVER (ORDER BY price_change_pct_30d ASC) AS loser_rank
    FROM stg.enriched_coins
    WHERE market_cap_rank <= 100
),

all_performers AS (
    -- Top 10 gainers & losers for each timeframe
    SELECT *, 'top_gainer_24h' AS performance_category FROM ranked_24h WHERE gainer_rank <= 10
    UNION ALL
    SELECT *, 'top_loser_24h' AS performance_category FROM ranked_24h WHERE loser_rank <= 10
    UNION ALL
    SELECT *, 'top_gainer_7d' AS performance_category FROM ranked_7d WHERE gainer_rank <= 10
    UNION ALL
    SELECT *, 'top_loser_7d' AS performance_category FROM ranked_7d WHERE loser_rank <= 10
    UNION ALL
    SELECT *, 'top_gainer_30d' AS performance_category FROM ranked_30d WHERE gainer_rank <= 10
    UNION ALL
    SELECT *, 'top_loser_30d' AS performance_category FROM ranked_30d WHERE loser_rank <= 10
)

SELECT
    CURRENT_DATE AS analysis_date,
    id,
    name,
    symbol,
    current_price,
    market_cap_rank,
    price_tier,
    timeframe,
    ROUND(change_pct, 2) AS change_pct,
    performance_category,
    COALESCE(
        CASE WHEN performance_category LIKE '%gainer%' THEN gainer_rank ELSE loser_rank END,
        0
    ) AS rank_in_category
FROM all_performers
ORDER BY performance_category, rank_in_category
