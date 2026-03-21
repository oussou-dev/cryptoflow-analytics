/* @bruin

name: analytics.top_performers
type: bq.sql
connection: bigquery-default
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

  Operational characteristics:
  - Expected output: Exactly 60 rows per execution (6 categories × 10 performers each)
  - Data freshness: Reflects price changes as of CoinGecko's last_updated timestamp
  - Execution time: < 5 seconds for typical workload (analyzing 100 coins across 3 timeframes)
  - Dependencies: Requires stg.enriched_coins to contain valid market data for top 100 coins by market cap
  - Performance: Optimized with efficient ROW_NUMBER() ranking and minimal data scanning
tags:
  - domain:crypto_analytics
  - domain:finance
  - type:fact_table
  - data_type:market_intelligence
  - data_type:performance_metrics
  - pipeline_role:mart
  - update_pattern:daily_snapshot
  - refresh_cadence:24h
  - use_case:market_monitoring
  - use_case:momentum_analysis
  - use_case:trading_signals
  - use_case:portfolio_rebalancing
  - use_case:market_alerts
  - sensitivity:public
  - data_source:coingecko_api
  - record_count:60
  - quality_tier:high
  - cardinality:medium

materialization:
  type: table

depends:
  - stg.enriched_coins

columns:
  - name: analysis_date
    type: date
    description: Date when the performance analysis was executed (CURRENT_DATE())
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
    description: |
      Price change percentage over the specified timeframe (rounded to 2 decimal places).
      Positive values indicate price increases (gainers), negative values indicate decreases (losers).
      Values can range dramatically during volatile periods, with extreme outliers possible during market stress.
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
    description: |
      Ranking within the performance category (1-10, where 1 = highest gain/loss).
      This rank is derived using ROW_NUMBER() within each category, ensuring no ties.
      For gainer categories: rank 1 = highest positive change_pct
      For loser categories: rank 1 = most negative change_pct
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
  - name: ranks_within_expected_range
    value: 1
    query: |
      SELECT COUNT(*) = 0
      FROM analytics.top_performers
      WHERE rank_in_category < 1 OR rank_in_category > 10
  - name: gainers_have_positive_changes
    value: 1
    query: |
      SELECT COUNT(*) = 0
      FROM analytics.top_performers
      WHERE performance_category LIKE '%gainer%' AND change_pct <= 0
  - name: losers_have_negative_changes
    value: 1
    query: |
      SELECT COUNT(*) = 0
      FROM analytics.top_performers
      WHERE performance_category LIKE '%loser%' AND change_pct >= 0
  - name: rank1_is_most_extreme_performer
    value: 1
    query: |
      WITH category_extremes AS (
        SELECT performance_category,
               MAX(ABS(change_pct)) AS max_abs_change,
               MAX(CASE WHEN rank_in_category = 1 THEN ABS(change_pct) END) AS rank1_abs_change
        FROM analytics.top_performers
        GROUP BY performance_category
      )
      SELECT COUNT(*) = 0
      FROM category_extremes
      WHERE max_abs_change != rank1_abs_change
  - name: no_duplicate_coins_per_category
    value: 1
    query: |-
      SELECT COUNT(*) = 0 FROM (
        SELECT performance_category, id, COUNT(*) as cnt
        FROM analytics.top_performers
        GROUP BY performance_category, id
        HAVING cnt > 1
      )

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
    CURRENT_DATE() AS analysis_date,
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
