"""@bruin
name: raw.fear_greed_index
type: python
connection: duckdb-default
materialization:
    type: table

columns:
    - name: value
      type: integer
      description: "Fear & Greed index value (0-100)"
      checks:
        - name: not_null
        - name: positive
    - name: value_classification
      type: string
      description: "Text classification of the index"
      checks:
        - name: not_null
        - name: accepted_values
          value: ["Extreme Fear", "Fear", "Neutral", "Greed", "Extreme Greed"]
    - name: timestamp_date
      type: date
      description: "Measurement date"
      checks:
        - name: not_null
        - name: unique

custom_checks:
    - name: "at_least_30_days_of_data"
      query: |
        SELECT COUNT(*) >= 30 FROM raw.fear_greed_index
      value: 1
    - name: "values_within_valid_range"
      query: |
        SELECT COUNT(*) = 0 FROM raw.fear_greed_index WHERE value < 0 OR value > 100
      value: 1
@bruin"""

import pandas as pd
import requests
from datetime import datetime, timezone


def materialize():
    """Fetch 90 days of Fear & Greed Index from alternative.me API."""

    url = "https://api.alternative.me/fng/"
    params = {"limit": 90, "format": "json"}

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()["data"]

    df = pd.DataFrame(data)
    df["value"] = df["value"].astype(int)
    df["timestamp_date"] = pd.to_datetime(
        df["timestamp"].astype(int), unit="s"
    ).dt.date
    df["ingested_at"] = datetime.now(timezone.utc)

    return df[["value", "value_classification", "timestamp_date", "ingested_at"]]
