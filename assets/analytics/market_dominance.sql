/* @bruin

name: analytics.market_dominance
type: bq.sql
connection: bigquery-default
description: |
  Cryptocurrency market dominance analysis providing comprehensive competitive intelligence through individual coin
  market share calculations and aggregated tier-level dominance metrics. This table transforms enriched market data
  into actionable dominance insights for investment strategy, risk assessment, and market structure analysis.

  Key analytical features:
  - Individual coin dominance percentages calculated against total market capitalization
  - Market capitalization rankings with tier-based groupings (mega/large/mid/small/micro cap)
  - Tier-level dominance aggregations revealing market concentration patterns
  - Daily snapshots enabling trend analysis and regime detection

  Primary use cases:
  - Competitive landscape monitoring and market share tracking
  - Portfolio diversification analysis across market cap segments
  - Market concentration risk assessment for regulatory and investment purposes
  - Identifying emerging leaders and market structure shifts over time
  - Input data for regime detection algorithms and allocation models

  Data characteristics:
  - Daily refresh aligns with upstream staging layer updates
  - Dominance percentages sum to ~100% with minimal rounding variance
  - Covers top cryptocurrencies by market cap with active trading data
  - Bitcoin dominance typically ranges 35-70% depending on market cycle
tags:
  - domain:finance
  - domain:crypto
  - data_type:fact_table
  - data_type:market_intelligence
  - data_type:competitive_analysis
  - pipeline_role:analytics
  - update_pattern:daily_snapshot
  - sensitivity:public
  - source:coingecko_derived
  - use_case:competitive_intelligence
  - use_case:portfolio_analytics
  - use_case:market_concentration
  - use_case:regime_detection
  - contains:dominance_metrics
  - contains:tier_aggregations

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
    description: |
      Individual coin market share percentage (0-100), calculated as coin_mcap / total_mcap * 100.
      Bitcoin typically ranges 35-70%, Ethereum 8-25%, with most altcoins <1%.
      Key metric for competitive positioning and market influence assessment.
    checks:
      - name: not_null
      - name: positive
  - name: rank_by_mcap
    type: integer
    description: |
      Market capitalization ranking where 1 = highest market cap (typically Bitcoin).
      Provides ordinal positioning for competitive analysis and tier boundary identification.
      Typically covers top 50-100 actively traded cryptocurrencies.
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
    description: |
      Combined market share percentage for the entire price tier (0-100).
      Mega-cap typically dominates 70-90%, revealing market concentration patterns.
      Used for diversification analysis and identifying tier rotation trends.
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
      WHERE LOWER(symbol) = 'btc' AND dominance_pct BETWEEN 10 AND 95
  - name: tier_dominance_consistency
    value: 1
    query: |-
      SELECT COUNT(*) = 0 FROM analytics.market_dominance
      WHERE tier_dominance_pct > 100 OR tier_dominance_pct < 0
  - name: mega_cap_tier_dominance_realistic
    value: 1
    query: |-
      SELECT COUNT(*) = 0 FROM analytics.market_dominance
      WHERE price_tier = 'mega_cap' AND (tier_dominance_pct < 50 OR tier_dominance_pct > 95)

@bruin */

WITH coin_dominance AS (
    SELECT
        CURRENT_DATE() AS snapshot_date,
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
