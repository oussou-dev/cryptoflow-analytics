"""@bruin

name: raw.coin_categories
description: |
  Reference dataset mapping cryptocurrency coins to their functional categories and subcategories.
  Serves as the foundational classification layer for market intelligence analysis, portfolio allocation,
  and risk management strategies. Currently contains 35 hand-curated cryptocurrencies covering major
  market segments including Layer 1 protocols, DeFi platforms, meme coins, stablecoins, and infrastructure
  projects.

  This static reference data is sourced from a seed CSV file and represents the most liquid and
  significant cryptocurrencies by market capitalization and trading volume. Categories follow industry
  standard classifications with custom subcategories that provide granular insights for sector-based
  analysis and portfolio diversification strategies.

  Data is used downstream in market regime detection, sentiment analysis, and momentum signal generation
  to provide sector-specific insights and cross-category performance comparisons.
connection: duckdb-default
tags:
  - domain:crypto
  - data_type:reference_data
  - data_type:dimension_table
  - pipeline_role:raw
  - update_pattern:manual_seed
  - sensitivity:public
  - use_case:classification
  - use_case:portfolio_allocation
  - use_case:risk_management

materialization:
  type: table

secrets:
  - key: duckdb-default
    inject_as: duckdb-default

columns:
  - name: coin_id
    type: VARCHAR
    description: |
      CoinGecko unique identifier used as the primary key for joining with market data.
      These identifiers match the 'id' field from CoinGecko's /coins/markets API endpoint.
      Examples include 'bitcoin', 'ethereum', 'binancecoin'. All 35 values are unique
      and correspond to the most liquid cryptocurrencies by market cap and volume.
    checks:
      - name: not_null
      - name: unique
  - name: category
    type: VARCHAR
    description: |
      Primary functional classification of the cryptocurrency project. Represents the
      main use case or protocol type. Contains 13 distinct categories including
      'Smart Contract Platform' (most common), 'DeFi', 'Meme', 'Stablecoin', 'Payments',
      'Scaling', 'Storage', 'Interoperability', 'Oracle', 'Infrastructure', 'AI & Compute',
      'Exchange Token', and 'Store of Value'. Used for high-level sector analysis
      and cross-category performance comparison.
    checks:
      - name: not_null
  - name: subcategory
    type: VARCHAR
    description: |
      Granular classification within the primary category providing deeper context
      for analysis. Contains 20 distinct subcategories such as 'Layer 1', 'Layer 2',
      'USD-Pegged', 'Decentralized Storage', 'GPU Network', etc. Enables fine-grained
      portfolio allocation and risk assessment. Some subcategories like 'Layer 1'
      appear across multiple categories (Smart Contract Platform, Store of Value).
    checks:
      - name: not_null

@bruin"""

import pandas as pd
import os


def materialize():
    """Load coin categories from a seed CSV file."""
    # The seeds folder is at the root, we can use the relative path
    # since bruin run happens at the project root.
    file_path = "seeds/coin_categories.csv"
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Seed file not found: {file_path}")
        
    df = pd.read_csv(file_path)
    
    return df
