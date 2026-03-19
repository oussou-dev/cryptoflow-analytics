"""@bruin
name: raw.global_market
type: python
connection: duckdb-default
materialization:
    type: table

columns:
    - name: total_market_cap_usd
      type: float
      description: "Total crypto market capitalization in USD"
      checks:
        - name: not_null
        - name: positive
    - name: btc_dominance
      type: float
      description: "Bitcoin market dominance percentage"
      checks:
        - name: not_null
    - name: eth_dominance
      type: float
      description: "Ethereum market dominance percentage"
      checks:
        - name: not_null
    - name: snapshot_date
      type: date
      description: "Date of the snapshot"
      checks:
        - name: not_null

custom_checks:
    - name: "dominance_values_reasonable"
      query: |
        SELECT COUNT(*) = 0 FROM raw.global_market
        WHERE btc_dominance < 0 OR btc_dominance > 100
          OR eth_dominance < 0 OR eth_dominance > 100
      value: 1
@bruin"""

import pandas as pd
import requests
from datetime import datetime, timezone


def materialize():
    """Fetch global cryptocurrency market data from CoinGecko API."""

    url = "https://api.coingecko.com/api/v3/global"
    headers = {"accept": "application/json"}

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()["data"]

    row = {
        "total_market_cap_usd": data["total_market_cap"].get("usd", 0),
        "total_volume_usd": data["total_volume"].get("usd", 0),
        "btc_dominance": data["market_cap_percentage"].get("btc", 0),
        "eth_dominance": data["market_cap_percentage"].get("eth", 0),
        "active_cryptocurrencies": data.get("active_cryptocurrencies", 0),
        "markets": data.get("markets", 0),
        "market_cap_change_24h_pct": data.get(
            "market_cap_change_percentage_24h_usd", 0
        ),
        "snapshot_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "ingested_at": datetime.now(timezone.utc),
    }

    return pd.DataFrame([row])
