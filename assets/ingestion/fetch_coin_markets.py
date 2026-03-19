"""@bruin
name: raw.coin_markets
type: python
connection: duckdb-default
materialization:
    type: table

columns:
    - name: id
      type: string
      description: "CoinGecko unique identifier"
      checks:
        - name: not_null
        - name: unique
    - name: symbol
      type: string
      description: "Coin ticker symbol (btc, eth...)"
      checks:
        - name: not_null
    - name: current_price
      type: float
      description: "Current price in USD"
      checks:
        - name: not_null
        - name: positive
    - name: market_cap
      type: float
      description: "Market capitalization in USD"
      checks:
        - name: positive
    - name: total_volume
      type: float
      description: "24h trading volume in USD"
    - name: market_cap_rank
      type: integer
      description: "Rank by market cap"
      checks:
        - name: positive
    - name: ingested_at
      type: timestamp
      description: "Ingestion timestamp (UTC)"
      checks:
        - name: not_null

custom_checks:
    - name: "at_least_50_coins_ingested"
      query: |
        SELECT COUNT(*) >= 50 FROM raw.coin_markets
      value: 1
    - name: "bitcoin_exists_in_data"
      query: |
        SELECT COUNT(*) > 0 FROM raw.coin_markets WHERE id = 'bitcoin'
      value: 1
    - name: "no_negative_market_caps"
      query: |
        SELECT COUNT(*) = 0 FROM raw.coin_markets WHERE market_cap < 0
      value: 1
@bruin"""

import pandas as pd
import requests
from datetime import datetime, timezone


def materialize():
    """Fetch top 100 cryptocurrencies by market cap from CoinGecko API."""

    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 100,
        "page": 1,
        "sparkline": False,
        "price_change_percentage": "1h,24h,7d,14d,30d",
    }
    headers = {"accept": "application/json"}

    response = requests.get(url, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()

    df = pd.DataFrame(data)

    columns_keep = [
        "id", "symbol", "name", "current_price", "market_cap",
        "market_cap_rank", "fully_diluted_valuation", "total_volume",
        "high_24h", "low_24h", "price_change_24h",
        "price_change_percentage_24h", "market_cap_change_24h",
        "market_cap_change_percentage_24h", "circulating_supply",
        "total_supply", "max_supply", "ath", "ath_change_percentage",
        "ath_date", "atl", "atl_change_percentage", "atl_date",
        "price_change_percentage_1h_in_currency",
        "price_change_percentage_7d_in_currency",
        "price_change_percentage_14d_in_currency",
        "price_change_percentage_30d_in_currency",
        "last_updated",
    ]

    existing_cols = [c for c in columns_keep if c in df.columns]
    df = df[existing_cols].copy()

    df["ingested_at"] = datetime.now(timezone.utc)

    return df
