/* @bruin

name: analytics.top_performers
type: duckdb.sql
description: |
  Identifies and ranks the top 10 gaining and losing cryptocurrencies across multiple timeframes (24h, 7d, 30d).

  This mart provides a daily snapshot of market momentum by analyzing price changes among the top 100 coins by market cap.
  Each timeframe captures different momentum patterns: 24h for immediate market reactions, 7d for weekly trends,
  and 30d for medium-term market cycles.

  The asset is commonly used for:
  - Daily market monitoring and alerts
  - Momentum-based trading strategies
  - Market sentiment analysis
  - Performance benchmarking against market leaders

  Filters to top 100 coins by market cap to focus on liquid, established assets and exclude low-cap manipulation.
  Each coin can appear in multiple categories if it performs strongly across different timeframes.
tags:
  - domain:crypto_analytics
  - type:fact_table
  - pipeline_role:mart
  - update_pattern:daily_snapshot
  - use_case:market_monitoring
  - use_case:momentum_analysis
  - sensitivity:public

materialization:
  type: table

depends:
  - stg.enriched_coins

columns:
  - name: analysis_date
    type: date
    description: Date when the performance analysis was executed (CURRENT_DATE)
    checks:
      - name: not_null
  - name: id
    type: string
    description: CoinGecko unique coin identifier (primary key from upstream API)
    checks:
      - name: not_null
  - name: name
    type: string
    description: Human-readable cryptocurrency name (e.g., Bitcoin, Ethereum)
    checks:
      - name: not_null
  - name: symbol
    type: string
    description: Trading ticker symbol (e.g., BTC, ETH) - may not be unique across exchanges
    checks:
      - name: not_null
  - name: current_price
    type: float
    description: Latest USD price from CoinGecko API at time of analysis
    checks:
      - name: not_null
      - name: positive
  - name: market_cap_rank
    type: integer
    description: CoinGecko market capitalization ranking (1 = highest market cap)
    checks:
      - name: not_null
      - name: positive
  - name: price_tier
    type: string
    description: Market capitalization tier classification (mega/large/mid/small/micro cap)
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - mega_cap
          - large_cap
          - mid_cap
          - small_cap
          - micro_cap
  - name: timeframe
    type: string
    description: Period over which the price change was measured
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - 24h
          - 7d
          - 30d
  - name: change_pct
    type: float
    description: Price change percentage over the specified timeframe (rounded to 2 decimal places)
    checks:
      - name: not_null
  - name: performance_category
    type: string
    description: Specific gainer/loser classification combining timeframe and direction
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - top_gainer_24h
          - top_loser_24h
          - top_gainer_7d
          - top_loser_7d
          - top_gainer_30d
          - top_loser_30d
  - name: rank_in_category
    type: integer
    description: Ranking within the performance category (1-10, where 1 = highest gain/loss)
    checks:
      - name: not_null
      - name: positive

custom_checks:
  - name: has_both_gainers_and_losers
    value: 1
    query: |
      SELECT COUNT(DISTINCT performance_category) >= 4
      FROM analytics.top_performers
  - name: exactly_10_performers_per_category
    value: 1
    query: |
      SELECT COUNT(*) = 0 FROM (
        SELECT performance_category, COUNT(*) as cnt
        FROM analytics.top_performers
        GROUP BY performance_category
        HAVING cnt != 10
      )
  - name: all_coins_within_top100_mcap
    value: 1
    query: |-
      SELECT COUNT(*) = 0
      FROM analytics.top_performers
      WHERE market_cap_rank > 100

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
