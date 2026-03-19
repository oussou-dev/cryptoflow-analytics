/* @bruin
name: stg.global_metrics
type: duckdb.sql
materialization:
    type: table
depends:
    - raw.global_market

columns:
    - name: snapshot_date
      type: date
      description: "Date of the global market snapshot"
      checks:
        - name: not_null
    - name: total_market_cap_usd
      type: float
      description: "Total crypto market cap in USD"
      checks:
        - name: not_null
        - name: positive
    - name: btc_dominance
      type: float
      description: "Bitcoin dominance percentage"
      checks:
        - name: not_null
    - name: altcoin_dominance
      type: float
      description: "Combined non-BTC-ETH market share"

custom_checks:
    - name: "dominance_sums_reasonable"
      query: |
        SELECT COUNT(*) = 0 FROM stg.global_metrics
        WHERE (btc_dominance + eth_dominance) > 100
      value: 1
@bruin */

SELECT
    CAST(snapshot_date AS DATE) AS snapshot_date,
    total_market_cap_usd,
    total_volume_usd,
    ROUND(btc_dominance, 2) AS btc_dominance,
    ROUND(eth_dominance, 2) AS eth_dominance,
    ROUND(100 - btc_dominance - eth_dominance, 2) AS altcoin_dominance,
    active_cryptocurrencies,
    markets,
    ROUND(market_cap_change_24h_pct, 2) AS market_cap_change_24h_pct,

    -- Volume-to-market-cap ratio (global liquidity)
    CASE
        WHEN total_market_cap_usd > 0
        THEN ROUND(total_volume_usd / total_market_cap_usd, 4)
        ELSE 0
    END AS global_volume_ratio,

    ingested_at

FROM raw.global_market
