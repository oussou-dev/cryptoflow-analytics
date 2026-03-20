/* @bruin

name: analytics.market_dominance
type: duckdb.sql
description: |
  Cryptocurrency market dominance analysis showing each coin's market share percentage
  and aggregated tier-level dominance. Used for competitive landscape analysis, market
  concentration monitoring, and portfolio diversification insights. Includes both
  individual coin dominance and price tier aggregations to identify market leaders
  across different market cap segments.
tags:
  - finance
  - market_analysis
  - fact_table
  - daily
  - competitive_intelligence
  - portfolio_analytics

materialization:
  type: table

depends:
  - stg.enriched_coins
  - stg.global_metrics

columns:
  - name: snapshot_date
    type: date
    description: Date of market dominance snapshot, always current date
    checks:
      - name: not_null
  - name: id
    type: string
    description: CoinGecko coin identifier, primary key for cryptocurrency
    checks:
      - name: not_null
      - name: unique
  - name: name
    type: string
    description: Full cryptocurrency name (e.g., "Bitcoin", "Ethereum")
    checks:
      - name: not_null
  - name: symbol
    type: string
    description: Cryptocurrency ticker symbol (e.g., "BTC", "ETH")
    checks:
      - name: not_null
  - name: market_cap
    type: float
    description: Current market capitalization in USD, basis for dominance calculation
    checks:
      - name: not_null
      - name: positive
  - name: dominance_pct
    type: float
    description: Individual coin market share percentage (0-100), calculated as coin_mcap / total_mcap * 100
    checks:
      - name: not_null
      - name: positive
  - name: rank_by_mcap
    type: integer
    description: Market capitalization ranking, 1 = largest market cap
    checks:
      - name: not_null
      - name: positive
  - name: price_tier
    type: string
    description: Market cap tier classification for grouping analysis
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - mega_cap
          - large_cap
          - mid_cap
          - small_cap
          - micro_cap
  - name: tier_dominance_pct
    type: float
    description: Combined market share percentage for the entire price tier
    checks:
      - name: not_null
      - name: positive
  - name: coins_in_tier
    type: integer
    description: Total number of coins in the same price tier
    checks:
      - name: not_null
      - name: positive

custom_checks:
  - name: dominance_sums_near_100
    value: 1
    query: |
      SELECT ABS(SUM(dominance_pct) - 100) < 2
      FROM analytics.market_dominance
  - name: btc_dominance_reasonable
    value: 1
    query: |
      SELECT COUNT(*) = 1 FROM analytics.market_dominance
      WHERE symbol = 'BTC' AND dominance_pct BETWEEN 20 AND 80
  - name: tier_dominance_consistency
    value: 1
    query: |-
      SELECT COUNT(*) = 0 FROM analytics.market_dominance
      WHERE tier_dominance_pct > 100 OR tier_dominance_pct < 0

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
