"""@bruin
name: raw.trending_coins
type: python
connection: duckdb-default
materialization:
    type: table

columns:
    - name: coin_id
      type: string
      description: "CoinGecko coin identifier"
      checks:
        - name: not_null
    - name: name
      type: string
      description: "Coin display name"
      checks:
        - name: not_null
    - name: market_cap_rank
      type: integer
      description: "Current market cap rank"
    - name: snapshot_date
      type: date
      description: "Date when trending data was captured"
      checks:
        - name: not_null

custom_checks:
    - name: "at_least_5_trending_coins"
      query: |
        SELECT COUNT(*) >= 5 FROM raw.trending_coins
      value: 1
@bruin"""

import pandas as pd
import requests
from datetime import datetime, timezone


def materialize():
    """Fetch currently trending coins from CoinGecko API."""

    url = "https://api.coingecko.com/api/v3/search/trending"
    headers = {"accept": "application/json"}

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()

    coins = data.get("coins", [])
    rows = []
    for coin_entry in coins:
        item = coin_entry.get("item", {})
        rows.append({
            "coin_id": item.get("id", ""),
            "name": item.get("name", ""),
            "symbol": item.get("symbol", ""),
            "market_cap_rank": item.get("market_cap_rank"),
            "score": item.get("score"),
            "snapshot_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "ingested_at": datetime.now(timezone.utc),
        })

    return pd.DataFrame(rows)
