/* @bruin
name: stg.enriched_coins
type: duckdb.sql
materialization:
    type: table
depends:
    - raw.coin_markets

columns:
    - name: id
      type: string
      description: "CoinGecko unique identifier"
      checks:
        - name: not_null
        - name: unique
    - name: current_price
      type: float
      description: "Current price in USD, cleaned"
      checks:
        - name: not_null
        - name: positive
    - name: price_tier
      type: string
      description: "Market cap tier classification"
      checks:
        - name: not_null
        - name: accepted_values
          value: ["mega_cap", "large_cap", "mid_cap", "small_cap", "micro_cap"]
    - name: volume_to_mcap_ratio
      type: float
      description: "Liquidity indicator: 24h volume / market cap"

custom_checks:
    - name: "no_duplicate_coins"
      query: |
        SELECT COUNT(*) = COUNT(DISTINCT id) FROM stg.enriched_coins
      value: 1
    - name: "price_tiers_are_logically_ordered"
      query: |
        SELECT COUNT(*) = 0 FROM stg.enriched_coins
        WHERE price_tier = 'mega_cap' AND market_cap < 100000000000
      value: 1
@bruin */

WITH cleaned AS (
    SELECT
        id,
        symbol,
        name,
        COALESCE(current_price, 0) AS current_price,
        COALESCE(market_cap, 0) AS market_cap,
        market_cap_rank,
        COALESCE(total_volume, 0) AS total_volume_24h,
        COALESCE(high_24h, 0) AS high_24h,
        COALESCE(low_24h, 0) AS low_24h,
        COALESCE(price_change_percentage_24h, 0) AS price_change_pct_24h,
        COALESCE(price_change_percentage_7d_in_currency, 0) AS price_change_pct_7d,
        COALESCE(price_change_percentage_30d_in_currency, 0) AS price_change_pct_30d,
        circulating_supply,
        total_supply,
        max_supply,
        ath,
        COALESCE(ath_change_percentage, 0) AS ath_drawdown_pct,
        last_updated,
        ingested_at
    FROM raw.coin_markets
    WHERE current_price IS NOT NULL
      AND current_price > 0
)

SELECT
    *,

    -- Market cap tier classification
    CASE
        WHEN market_cap >= 100000000000 THEN 'mega_cap'
        WHEN market_cap >= 10000000000  THEN 'large_cap'
        WHEN market_cap >= 1000000000   THEN 'mid_cap'
        WHEN market_cap >= 100000000    THEN 'small_cap'
        ELSE 'micro_cap'
    END AS price_tier,

    -- Volume-to-market-cap ratio (liquidity indicator)
    CASE
        WHEN market_cap > 0
        THEN ROUND(total_volume_24h / market_cap, 4)
        ELSE 0
    END AS volume_to_mcap_ratio,

    -- Distance from all-time high
    ROUND(ath_drawdown_pct, 2) AS distance_from_ath_pct,

    -- Supply scarcity ratio
    CASE
        WHEN max_supply > 0 AND circulating_supply > 0
        THEN ROUND(circulating_supply / max_supply * 100, 2)
        ELSE NULL
    END AS supply_ratio_pct,

    -- Intraday spread (volatility proxy)
    CASE
        WHEN low_24h > 0
        THEN ROUND((high_24h - low_24h) / low_24h * 100, 2)
        ELSE 0
    END AS intraday_spread_pct

FROM cleaned
