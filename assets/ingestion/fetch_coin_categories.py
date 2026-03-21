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

  Operational characteristics:
  - Data size: ~35 records, minimal storage footprint (~1KB)
  - Refresh pattern: Manual updates via CSV file modifications
  - Growth expectations: Stable size, curated to top-tier cryptocurrencies only
  - Performance: Excellent for joins due to small size and primary key constraints
  - Data lineage: Seeds → raw layer → potential enrichment in staging/analytics layers
connection: duckdb-default
tags:
  - domain:crypto
  - domain:finance
  - data_type:reference_data
  - data_type:dimension_table
  - data_type:lookup_table
  - pipeline_role:raw
  - pipeline_role:foundation
  - update_pattern:manual_seed
  - update_pattern:static
  - refresh_cadence:manual
  - sensitivity:public
  - data_source:curated_seed
  - record_count:35
  - size_tier:small
  - quality_tier:high
  - use_case:classification
  - use_case:portfolio_allocation
  - use_case:risk_management
  - use_case:sector_analysis
  - use_case:market_intelligence

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

      Semantic type: Primary identifier / Foreign key
      Cardinality: 35 distinct values (100% unique)
      Business context: Critical for data lineage and joins with market_data tables
      Format: lowercase, hyphenated strings (e.g. 'avalanche-2', 'polygon-ecosystem-token')
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

      Semantic type: Categorical dimension / Business classification
      Cardinality: 13 distinct values (~37% unique)
      Business context: Primary grouping for portfolio allocation and risk assessment
      Most common values: 'Smart Contract Platform' (14 coins), followed by 'DeFi', 'Meme'
      Usage notes: Enables sector rotation strategies and category-based performance benchmarking
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

      Semantic type: Hierarchical dimension / Technical classification
      Cardinality: 20 distinct values (~57% unique)
      Business context: Technical specificity for detailed portfolio construction
      Cross-category patterns: 'Layer 1' spans multiple categories (blockchain architecture)
      Examples: 'Digital Gold', 'CEX', 'Cross-Border', 'DEX', 'Bitcoin L2', 'Solana Meme'
      Usage notes: Critical for understanding technical infrastructure and ecosystem positioning
    checks:
      - name: not_null

@bruin"""

import pandas as pd
import os


def materialize():
    """Load coin categories from a seed CSV file."""
    os.environ["MOTHERDUCK_TOKEN"] = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6Im91c3NvdS5kZXZAZ21haWwuY29tIiwibWRSZWdpb24iOiJhd3MtZXUtY2VudHJhbC0xIiwic2Vzc2lvbiI6Im91c3NvdS5kZXYuZ21haWwuY29tIiwicGF0IjoiTzVlbVVmRHVUN3EtQWpTbHNlRzVLVTRKOGIyMmFPLU1EOE55ZFM1TjVZQSIsInVzZXJJZCI6IjNjMDIxMmQ2LTA1NzctNDc4OC05YmEzLTYxZGVlZjg0OTQxMyIsImlzcyI6Im1kX3BhdCIsInJlYWRPbmx5IjpmYWxzZSwidG9rZW5UeXBlIjoicmVhZF93cml0ZSIsImlhdCI6MTc3NDA5MTI0OX0.Vt9YpbUtF1cAquI65hYlsIXCz2dMksHBkFfKH6B1iOA"

    # The seeds folder is at the root, we can use the relative path
    # since bruin run happens at the project root.
    file_path = "seeds/coin_categories.csv"
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Seed file not found: {file_path}")
        
    df = pd.read_csv(file_path)
    
    return df
