# 🏆 Plan d'Implémentation — Crypto Pipeline Hackathon

## Data Engineering Zoomcamp 2026 × Bruin Competition

---

## 1. SYNTHÈSE DES EXIGENCES DE LA COMPÉTITION

### Prix visés

| Prix                                  | Condition                                                                                                                                  | Critère clé                                                     |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------- |
| **Participation** (1 mois Claude Pro) | Bruin pour ingestion + transformation + orchestration + analyse IA, post Slack #projects, GitHub + README                                  | Cocher toutes les cases                                         |
| **Top 3** (1 an Claude Pro)           | Idem + votes communautaires sur Slack (thumbs-up)                                                                                          | Projet visuellement impressionnant, README soigné, storytelling |
| **Outstanding** (Mac Mini)            | Idem + post LinkedIn détaillé (features Bruin, design choices, comparaison outils) + screenshots AI analyst. Top 10 likes → tirage au sort | Visibilité LinkedIn + qualité de l'analyse IA                   |

### Checklist obligatoire

- [ ] Bruin CLI pour **ingestion** (ingestr ou Python asset)
- [ ] Bruin pour **transformation** (SQL assets DuckDB/BigQuery)
- [ ] Bruin pour **orchestration** (pipeline.yml avec schedule + depends)
- [ ] Bruin **AI data analyst** (screenshots d'analyses conversationnelles)
- [ ] Repo GitHub avec **README complet**
- [ ] Post sur **Slack #projects**
- [ ] Post **LinkedIn** (pour le prix Outstanding)

---

## 2. ARCHITECTURE DU PROJET

### Nom du projet : **CryptoFlow Analytics**

*Real-time Cryptocurrency Market Intelligence Pipeline*

### Vision

Un pipeline end-to-end qui ingère des données crypto multi-sources (prix, volumes, métriques on-chain, sentiment), les transforme en couches analytiques, et produit des insights actionnables via l'AI analyst de Bruin — le tout orchestré par Bruin CLI.

### Architecture en couches (Medallion)

```
┌──────────────────────────────────────────────────────────────────┐
│                     SOURCES EXTERNES                             │
│  CoinGecko API (Free) │ Fear & Greed Index │ CSV Seeds (stables) │
└──────────┬───────────────────┬──────────────────┬────────────────┘
           │                   │                  │
           ▼                   ▼                  ▼
┌──────────────────────────────────────────────────────────────────┐
│              🥉 BRONZE — Ingestion (raw.)                        │
│  Python assets (API fetch) │ Ingestr assets │ Seed assets (CSV)  │
│  raw.coin_prices │ raw.coin_markets │ raw.fear_greed │ raw.stablecoins │
└──────────────────────────────┬───────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│              🥈 SILVER — Staging (stg.)                          │
│  Nettoyage, typage, déduplication, jointures                     │
│  stg.daily_prices │ stg.market_metrics │ stg.enriched_coins      │
└──────────────────────────────┬───────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│              🥇 GOLD — Analytics (analytics.)                    │
│  Métriques agrégées, scores, rankings, signaux                   │
│  analytics.market_summary │ analytics.volatility_report          │
│  analytics.correlation_matrix │ analytics.whale_signals          │
│  analytics.portfolio_optimizer │ analytics.trend_detector        │
└──────────────────────────────────────────────────────────────────┘
```

---

## 3. STRUCTURE DU REPO GITHUB

```
cryptoflow-analytics/
├── .bruin.yml                          # Config projet + connexions
├── pipeline.yml                        # Pipeline principal + schedule
├── README.md                           # Documentation complète (crucial!)
├── LICENSE
├── .gitignore
│
├── assets/
│   ├── ingestion/                      # 🥉 BRONZE
│   │   ├── fetch_coin_markets.py       # Python asset: CoinGecko /coins/markets
│   │   ├── fetch_coin_history.py       # Python asset: CoinGecko /coins/{id}/market_chart
│   │   ├── fetch_fear_greed.py         # Python asset: Fear & Greed Index API
│   │   ├── fetch_global_data.py        # Python asset: CoinGecko /global
│   │   ├── fetch_trending.py           # Python asset: CoinGecko /search/trending
│   │   └── stablecoin_reference.asset.yml  # Seed CSV: référentiel stablecoins
│   │
│   ├── staging/                        # 🥈 SILVER
│   │   ├── stg_daily_prices.sql        # Prix nettoyés + typés
│   │   ├── stg_market_metrics.sql      # Market cap, volume normalisés
│   │   ├── stg_enriched_coins.sql      # Jointure prix + market + metadata
│   │   ├── stg_fear_greed_daily.sql    # Index nettoyé + moyenne mobile
│   │   └── stg_global_market.sql       # Données marché global nettoyées
│   │
│   └── analytics/                      # 🥇 GOLD
│       ├── market_dominance.sql        # BTC/ETH/Altcoin dominance tracker
│       ├── volatility_analysis.sql     # Volatilité 7j/30j/90j par coin
│       ├── correlation_matrix.sql      # Corrélations inter-cryptos
│       ├── momentum_signals.sql        # RSI simplifié, moyennes mobiles
│       ├── whale_alert_summary.sql     # Détection mouvements inhabituels
│       ├── fear_greed_impact.sql       # Corrélation sentiment ↔ prix
│       ├── top_performers.sql          # Classement gains/pertes
│       └── market_regime_detector.sql  # Bull/Bear/Neutral classification
│
├── seeds/
│   ├── stablecoin_list.csv             # Liste stablecoins (USDT, USDC, DAI...)
│   └── coin_categories.csv            # Catégories manuelles (DeFi, L1, L2, Meme...)
│
├── glossary.yml                        # Glossaire Bruin (termes métier)
├── docs/
│   ├── architecture.md                 # Diagramme d'architecture détaillé
│   ├── data_dictionary.md              # Dictionnaire de données
│   └── ai_analyst_screenshots/         # Screenshots pour LinkedIn
│       ├── market_analysis.png
│       ├── correlation_query.png
│       └── portfolio_insight.png
│
└── scripts/
    └── setup.sh                        # Script d'installation one-liner
```

---

## 4. ROADMAP DÉTAILLÉE (Estimation: 8-12h de travail)

### Phase 1 — Setup & Scaffolding (1-2h)

| Étape | Action                         | Commande / Détail                                                                      |
| ----- | ------------------------------ | -------------------------------------------------------------------------------------- |
| 1.1   | Installer Bruin CLI            | `curl -sSL https://raw.githubusercontent.com/bruin-data/bruin/main/install.sh \| bash` |
| 1.2   | Installer VS Code extension    | Extension "Bruin" dans VS Code                                                         |
| 1.3   | Configurer Bruin MCP           | Ajouter dans `.vscode/mcp.json`                                                        |
| 1.4   | Créer le projet                | `bruin init default cryptoflow-analytics`                                              |
| 1.5   | Obtenir clé CoinGecko          | Inscription gratuite sur coingecko.com/en/api                                          |
| 1.6   | Configurer `.bruin.yml`        | Connexions DuckDB + variables d'environnement                                          |
| 1.7   | Initialiser le repo Git        | `git init` + `.gitignore` + premier commit                                             |
| 1.8   | Créer la structure de dossiers | `mkdir -p assets/{ingestion,staging,analytics} seeds docs/ai_analyst_screenshots`      |

### Phase 2 — Ingestion Layer (2-3h)

| Étape | Action                  | Détail                                                |
| ----- | ----------------------- | ----------------------------------------------------- |
| 2.1   | `fetch_coin_markets.py` | CoinGecko `/coins/markets` — top 100 coins            |
| 2.2   | `fetch_coin_history.py` | CoinGecko `/coins/{id}/market_chart` — historique 90j |
| 2.3   | `fetch_fear_greed.py`   | Alternative.me Fear & Greed API                       |
| 2.4   | `fetch_global_data.py`  | CoinGecko `/global` — métriques globales              |
| 2.5   | `fetch_trending.py`     | CoinGecko `/search/trending` — trending coins         |
| 2.6   | Seeds CSV               | Créer stablecoin_list.csv + coin_categories.csv       |
| 2.7   | Valider l'ingestion     | `bruin run assets/ingestion/`                         |

### Phase 3 — Transformation Layer (2-3h)

| Étape | Action                          | Détail                                              |
| ----- | ------------------------------- | --------------------------------------------------- |
| 3.1   | `stg_daily_prices.sql`          | Nettoyage, déduplication, cast types                |
| 3.2   | `stg_market_metrics.sql`        | Calcul variations %, ranks                          |
| 3.3   | `stg_enriched_coins.sql`        | Jointure avec catégories + stablecoin flag          |
| 3.4   | `stg_fear_greed_daily.sql`      | Moyenne mobile 7j du Fear & Greed                   |
| 3.5   | `stg_global_market.sql`         | Indicateurs marché global normalisés                |
| 3.6   | Quality checks sur chaque asset | `not_null`, `unique`, `positive`, `accepted_values` |
| 3.7   | Custom checks métier            | Vérifier cohérence prix, volumes > 0, etc.          |

### Phase 4 — Analytics Layer (2-3h)

| Étape | Action                       | Détail                                      |
| ----- | ---------------------------- | ------------------------------------------- |
| 4.1   | `market_dominance.sql`       | % BTC vs ETH vs Altcoins                    |
| 4.2   | `volatility_analysis.sql`    | Écart-type rolling 7/30/90j                 |
| 4.3   | `correlation_matrix.sql`     | Corrélation prix top 20 coins               |
| 4.4   | `momentum_signals.sql`       | SMA 7/21, RSI simplifié, signal achat/vente |
| 4.5   | `fear_greed_impact.sql`      | Corrélation Fear&Greed ↔ prix BTC           |
| 4.6   | `top_performers.sql`         | Ranking gains/pertes 24h/7j/30j             |
| 4.7   | `market_regime_detector.sql` | Classification Bull/Bear/Neutral            |
| 4.8   | `whale_alert_summary.sql`    | Détection volumes anormaux                  |

### Phase 5 — Bruin AI Analyst + Polish (1-2h)

| Étape | Action                   | Détail                                |
| ----- | ------------------------ | ------------------------------------- |
| 5.1   | Déployer sur Bruin Cloud | Connecter GitHub repo                 |
| 5.2   | Interroger l'AI analyst  | Questions d'analyse conversationnelle |
| 5.3   | Capturer screenshots     | 5-8 screenshots d'analyses IA         |
| 5.4   | Rédiger le README        | Template complet ci-dessous           |
| 5.5   | Créer le glossaire Bruin | `glossary.yml` avec termes crypto     |

### Phase 6 — Publication & Communication (1h)

| Étape | Action                                       |
| ----- | -------------------------------------------- |
| 6.1   | Push final sur GitHub                        |
| 6.2   | Post Slack #projects avec description + repo |
| 6.3   | Rédiger et publier post LinkedIn             |
| 6.4   | Vérifier la lineage avec `bruin lineage`     |

---

## 5. IMPLÉMENTATION DÉTAILLÉE — CODE

### 5.1 Configuration `.bruin.yml`

```yaml
default_environment: default
environments:
  default:
    connections:
      duckdb:
        - name: "duckdb-default"
          path: "crypto.db"

  # Pour déploiement cloud (optionnel, bonus)
  production:
    connections:
      google_cloud_platform:
        - name: "gcp"
          service_account_file: "/path/to/key.json"
          project_id: "cryptoflow-analytics"
```

### 5.2 Configuration `pipeline.yml`

```yaml
name: cryptoflow
schedule: "daily"  # Exécution quotidienne

# Variables du pipeline
start_date: "2024-01-01"

# Notifications (si Bruin Cloud)
notifications:
  slack:
    - channel: "#data-alerts"

# Default settings pour tous les assets
default_parameters:
  destination: duckdb
```

### 5.3 Asset d'ingestion — `fetch_coin_markets.py`

```python
"""@bruin
name: raw.coin_markets
type: python
connection: duckdb-default
materialization:
    type: table
    strategy: merge

columns:
    - name: id
      type: string
      description: "Identifiant unique CoinGecko"
      checks:
        - name: not_null
        - name: unique
    - name: symbol
      type: string
      description: "Symbole du coin (BTC, ETH...)"
      checks:
        - name: not_null
    - name: current_price
      type: float
      description: "Prix actuel en USD"
      checks:
        - name: not_null
        - name: positive
    - name: market_cap
      type: float
      description: "Capitalisation boursière en USD"
      checks:
        - name: positive
    - name: total_volume
      type: float
      description: "Volume de trading 24h en USD"
    - name: price_change_percentage_24h
      type: float
      description: "Variation de prix sur 24h en %"
    - name: ingested_at
      type: timestamp
      description: "Timestamp d'ingestion"
      checks:
        - name: not_null

custom_checks:
    - name: "Au moins 50 coins ingérés"
      query: |
        SELECT COUNT(*) >= 50 FROM raw.coin_markets
      value: 1
    - name: "Bitcoin présent dans les données"
      query: |
        SELECT COUNT(*) > 0 FROM raw.coin_markets WHERE id = 'bitcoin'
      value: 1
@bruin"""

import pandas as pd
import requests
from datetime import datetime

def materialize():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 100,
        "page": 1,
        "sparkline": False,
        "price_change_percentage": "1h,24h,7d,14d,30d"
    }

    # Headers avec clé API (gratuite)
    headers = {
        "accept": "application/json",
        # "x-cg-demo-api-key": "YOUR_API_KEY"  # Optionnel pour le plan gratuit
    }

    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    data = response.json()

    df = pd.DataFrame(data)

    # Sélection et renommage des colonnes pertinentes
    columns_keep = [
        'id', 'symbol', 'name', 'current_price', 'market_cap',
        'market_cap_rank', 'fully_diluted_valuation', 'total_volume',
        'high_24h', 'low_24h', 'price_change_24h',
        'price_change_percentage_24h', 'market_cap_change_24h',
        'market_cap_change_percentage_24h', 'circulating_supply',
        'total_supply', 'max_supply', 'ath', 'ath_change_percentage',
        'ath_date', 'atl', 'atl_change_percentage', 'atl_date',
        'price_change_percentage_1h_in_currency',
        'price_change_percentage_7d_in_currency',
        'price_change_percentage_14d_in_currency',
        'price_change_percentage_30d_in_currency',
        'last_updated'
    ]

    existing_cols = [c for c in columns_keep if c in df.columns]
    df = df[existing_cols].copy()

    # Ajout timestamp d'ingestion
    df['ingested_at'] = datetime.utcnow()

    return df
```

### 5.4 Asset d'ingestion — `fetch_fear_greed.py`

```python
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
      description: "Classification textuelle (Extreme Fear, Fear, Neutral, Greed, Extreme Greed)"
      checks:
        - name: not_null
        - name: accepted_values
          value: ["Extreme Fear", "Fear", "Neutral", "Greed", "Extreme Greed"]
    - name: timestamp_date
      type: date
      description: "Date de la mesure"
      checks:
        - name: not_null
        - name: unique
@bruin"""

import pandas as pd
import requests
from datetime import datetime

def materialize():
    url = "https://api.alternative.me/fng/"
    params = {"limit": 90, "format": "json"}

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()["data"]

    df = pd.DataFrame(data)
    df['value'] = df['value'].astype(int)
    df['timestamp_date'] = pd.to_datetime(df['timestamp'].astype(int), unit='s').dt.date
    df['ingested_at'] = datetime.utcnow()

    return df[['value', 'value_classification', 'timestamp_date', 'ingested_at']]
```

### 5.5 Asset d'ingestion — `fetch_global_data.py`

```python
"""@bruin
name: raw.global_market
type: python
connection: duckdb-default
materialization:
    type: table

columns:
    - name: total_market_cap_usd
      type: float
      description: "Capitalisation totale du marché crypto en USD"
      checks:
        - name: not_null
        - name: positive
    - name: btc_dominance
      type: float
      description: "Dominance Bitcoin en %"
      checks:
        - name: not_null
@bruin"""

import pandas as pd
import requests
from datetime import datetime

def materialize():
    url = "https://api.coingecko.com/api/v3/global"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()["data"]

    row = {
        "total_market_cap_usd": data["total_market_cap"].get("usd", 0),
        "total_volume_usd": data["total_volume"].get("usd", 0),
        "btc_dominance": data["market_cap_percentage"].get("btc", 0),
        "eth_dominance": data["market_cap_percentage"].get("eth", 0),
        "active_cryptocurrencies": data.get("active_cryptocurrencies", 0),
        "markets": data.get("markets", 0),
        "market_cap_change_24h_pct": data.get("market_cap_change_percentage_24h_usd", 0),
        "snapshot_date": datetime.utcnow().strftime("%Y-%m-%d"),
        "ingested_at": datetime.utcnow()
    }

    return pd.DataFrame([row])
```

### 5.6 Asset staging — `stg_enriched_coins.sql`

```sql
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
      description: "Identifiant unique CoinGecko"
      checks:
        - name: not_null
        - name: unique
    - name: market_cap_rank
      type: integer
      description: "Rang par capitalisation"
      checks:
        - name: positive
    - name: price_tier
      type: string
      description: "Catégorie de prix"
      checks:
        - name: not_null
        - name: accepted_values
          value: ["mega_cap", "large_cap", "mid_cap", "small_cap", "micro_cap"]

custom_checks:
    - name: "Pas de doublons sur l'id"
      query: |
        SELECT COUNT(*) = COUNT(DISTINCT id) FROM stg.enriched_coins
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
),

categorized AS (
    SELECT
        *,
        -- Classification par taille de marché
        CASE
            WHEN market_cap >= 100000000000 THEN 'mega_cap'
            WHEN market_cap >= 10000000000 THEN 'large_cap'
            WHEN market_cap >= 1000000000 THEN 'mid_cap'
            WHEN market_cap >= 100000000 THEN 'small_cap'
            ELSE 'micro_cap'
        END AS price_tier,

        -- Ratio volume/market cap (indicateur de liquidité)
        CASE
            WHEN market_cap > 0 THEN ROUND(total_volume_24h / market_cap, 4)
            ELSE 0
        END AS volume_to_mcap_ratio,

        -- Distance par rapport à l'ATH
        ROUND(ath_drawdown_pct, 2) AS distance_from_ath_pct,

        -- Supply ratio (si max_supply existe)
        CASE
            WHEN max_supply > 0 AND circulating_supply > 0 
            THEN ROUND(circulating_supply / max_supply * 100, 2)
            ELSE NULL
        END AS supply_ratio_pct,

        -- Spread intraday (volatilité)
        CASE
            WHEN low_24h > 0 
            THEN ROUND((high_24h - low_24h) / low_24h * 100, 2)
            ELSE 0
        END AS intraday_spread_pct

    FROM cleaned
)

SELECT * FROM categorized
```

### 5.7 Asset analytics — `market_dominance.sql`

```sql
/* @bruin
name: analytics.market_dominance
type: duckdb.sql
materialization:
    type: table
depends:
    - stg.enriched_coins
    - raw.global_market

columns:
    - name: snapshot_date
      type: date
      checks:
        - name: not_null

custom_checks:
    - name: "Dominances totalisent environ 100%"
      query: |
        SELECT ABS(SUM(dominance_pct) - 100) < 5 
        FROM analytics.market_dominance
      value: 1
@bruin */

WITH coin_dominance AS (
    SELECT
        CURRENT_DATE AS snapshot_date,
        id,
        name,
        symbol,
        market_cap,
        price_tier,
        ROUND(market_cap * 100.0 / NULLIF(SUM(market_cap) OVER(), 0), 4) AS dominance_pct,
        RANK() OVER (ORDER BY market_cap DESC) AS rank_by_mcap
    FROM stg.enriched_coins
),

tier_summary AS (
    SELECT
        CURRENT_DATE AS snapshot_date,
        price_tier,
        COUNT(*) AS coin_count,
        SUM(market_cap) AS tier_market_cap,
        ROUND(SUM(market_cap) * 100.0 / NULLIF(SUM(SUM(market_cap)) OVER(), 0), 2) AS tier_dominance_pct
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
LEFT JOIN tier_summary ts 
    ON cd.price_tier = ts.price_tier 
    AND cd.snapshot_date = ts.snapshot_date
ORDER BY cd.rank_by_mcap
```

### 5.8 Asset analytics — `momentum_signals.sql`

```sql
/* @bruin
name: analytics.momentum_signals
type: duckdb.sql
materialization:
    type: table
depends:
    - stg.enriched_coins
    - raw.fear_greed_index

columns:
    - name: signal
      type: string
      description: "Signal de momentum (STRONG_BUY, BUY, NEUTRAL, SELL, STRONG_SELL)"
      checks:
        - name: not_null
        - name: accepted_values
          value: ["STRONG_BUY", "BUY", "NEUTRAL", "SELL", "STRONG_SELL"]
@bruin */

WITH price_momentum AS (
    SELECT
        id, name, symbol, current_price, market_cap_rank, price_tier,
        price_change_pct_24h,
        price_change_pct_7d,
        price_change_pct_30d,
        volume_to_mcap_ratio,
        distance_from_ath_pct,
        intraday_spread_pct,

        -- Score de momentum composite (sur 100)
        ROUND(
            (CASE WHEN price_change_pct_24h > 5 THEN 20
                  WHEN price_change_pct_24h > 0 THEN 10
                  WHEN price_change_pct_24h > -5 THEN 0
                  ELSE -10 END)
            +
            (CASE WHEN price_change_pct_7d > 15 THEN 25
                  WHEN price_change_pct_7d > 0 THEN 12
                  WHEN price_change_pct_7d > -10 THEN 0
                  ELSE -12 END)
            +
            (CASE WHEN price_change_pct_30d > 30 THEN 25
                  WHEN price_change_pct_30d > 0 THEN 12
                  WHEN price_change_pct_30d > -20 THEN 0
                  ELSE -12 END)
            +
            (CASE WHEN volume_to_mcap_ratio > 0.3 THEN 15
                  WHEN volume_to_mcap_ratio > 0.1 THEN 8
                  ELSE 0 END)
            +
            (CASE WHEN distance_from_ath_pct > -20 THEN 15
                  WHEN distance_from_ath_pct > -50 THEN 8
                  ELSE 0 END)
        , 1) AS momentum_score
    FROM stg.enriched_coins
    WHERE market_cap_rank <= 50
),

fear_greed AS (
    SELECT value AS fg_value, value_classification AS fg_class
    FROM raw.fear_greed_index
    ORDER BY timestamp_date DESC
    LIMIT 1
)

SELECT
    CURRENT_DATE AS analysis_date,
    pm.*,
    fg.fg_value AS fear_greed_index,
    fg.fg_class AS market_sentiment,

    -- Signal composite
    CASE
        WHEN momentum_score >= 60 AND fg.fg_value < 30 THEN 'STRONG_BUY'
        WHEN momentum_score >= 40 THEN 'BUY'
        WHEN momentum_score >= -10 THEN 'NEUTRAL'
        WHEN momentum_score >= -40 THEN 'SELL'
        ELSE 'STRONG_SELL'
    END AS signal,

    -- Confidence du signal
    CASE
        WHEN ABS(momentum_score) > 50 THEN 'HIGH'
        WHEN ABS(momentum_score) > 25 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS signal_confidence

FROM price_momentum pm
CROSS JOIN fear_greed fg
ORDER BY momentum_score DESC
```

### 5.9 Seed CSV — `coin_categories.csv`

```csv
coin_id,category,subcategory
bitcoin,Store of Value,Digital Gold
ethereum,Smart Contract Platform,Layer 1
binancecoin,Exchange Token,CEX
solana,Smart Contract Platform,Layer 1
cardano,Smart Contract Platform,Layer 1
ripple,Payments,Cross-Border
dogecoin,Meme,OG Meme
polkadot,Interoperability,Layer 0
chainlink,Oracle,Data Feed
avalanche-2,Smart Contract Platform,Layer 1
tron,Smart Contract Platform,Layer 1
uniswap,DeFi,DEX
litecoin,Payments,Digital Silver
cosmos,Interoperability,Layer 0
near,Smart Contract Platform,Layer 1
aptos,Smart Contract Platform,Layer 1
arbitrum,Scaling,Layer 2
optimism,Scaling,Layer 2
polygon-ecosystem-token,Scaling,Layer 2
filecoin,Storage,Decentralized Storage
```

### 5.10 Glossaire Bruin — `glossary.yml`

```yaml
entities:
  Cryptocurrency:
    description: "Actif numérique utilisant la cryptographie, échangé sur des marchés décentralisés ou centralisés."
    attributes:
      market_cap:
        type: float
        description: "Prix × offre en circulation. Indicateur principal de taille."
      volume_24h:
        type: float
        description: "Montant total échangé sur 24h en USD."
      dominance:
        type: float
        description: "Part de marché relative d'un coin (market_cap / total_market_cap)."

  MarketRegime:
    description: "Classification de l'état du marché crypto basée sur des indicateurs techniques et sentimentaux."
    attributes:
      fear_greed_index:
        type: integer
        description: "Indice 0-100 mesurant le sentiment du marché. <25=Extreme Fear, >75=Extreme Greed."
      momentum_score:
        type: float
        description: "Score composite calculé à partir des variations de prix multi-horizons."

  PriceTier:
    description: "Classification des cryptomonnaies par capitalisation boursière."
    attributes:
      mega_cap:
        type: string
        description: ">100B USD (BTC, ETH)"
      large_cap:
        type: string
        description: "10B-100B USD"
      mid_cap:
        type: string
        description: "1B-10B USD"
      small_cap:
        type: string
        description: "100M-1B USD"
```

---

## 6. ÉLÉMENTS DIFFÉRENCIANTS (Ce qui vous fera gagner)

### 6.1 Analyses avancées qui sortent du lot

1. **Market Regime Detector** — Classification automatique Bull/Bear/Neutral en combinant Fear & Greed + momentum + volume (la plupart des projets ne font que des tableaux de prix)
2. **Corrélation Sentiment ↔ Prix** — Analyse quantitative de l'impact du Fear & Greed sur les prix BTC (approche data science)
3. **Score de Momentum Composite** — Indicateur propriétaire combinant 5 facteurs (pas juste copier-coller des variations %)
4. **Détection de Volumes Anormaux** — Alerting sur les mouvements "whale" via z-score sur volumes (vraie valeur ajoutée business)
5. **Supply Scarcity Analysis** — Ratio supply circulante/max, impact sur la valorisation

### 6.2 Excellence technique Bruin

1. **Quality checks exhaustifs** — Built-in (not_null, unique, positive, accepted_values) + custom checks métier sur chaque asset
2. **Glossaire structuré** — Termes crypto documentés, montre la maîtrise de Bruin governance
3. **Architecture en couches propre** — Bronze/Silver/Gold avec dépendances explicites
4. **Merge strategy** sur l'ingestion — Pas de duplications, pipeline idempotent
5. **Variables et templating** — Utiliser `start_date`/`end_date` pour le backfill

### 6.3 README exceptionnel (crucial pour les votes)

Le README doit contenir :

- Titre accrocheur + badge/emoji
- Diagramme d'architecture (Mermaid ou image)
- GIF ou screenshot du pipeline en action
- Tableau des features Bruin utilisées
- Section "AI Analyst Insights" avec screenshots
- Instructions d'installation en 3 commandes max
- Section "What I Learned" (storytelling)
- Comparaison Bruin vs alternatives (dbt, Airflow)

### 6.4 Post LinkedIn gagnant (pour le prix Outstanding)

Structure recommandée :

1. Hook : "J'ai construit un pipeline d'intelligence crypto en 48h avec un seul outil..."
2. Problème : "Gérer 5 outils (Airflow + dbt + Great Expectations + ...) pour un simple pipeline ?"
3. Solution : "Bruin combine tout en un CLI unique"
4. Features utilisées : ingestion, SQL transformations, quality checks, lineage, AI analyst
5. Screenshots de l'AI analyst
6. Comparaison honnête Bruin vs dbt/Airflow
7. Call to action + liens

---

## 7. QUESTIONS TYPES POUR BRUIN AI ANALYST

Pour capturer des screenshots impressionnants, posez ces questions à l'AI analyst une fois le pipeline déployé :

1. *"Quels sont les 5 coins avec le meilleur momentum score aujourd'hui et pourquoi ?"*
2. *"Le marché est-il en phase Bull ou Bear actuellement ? Montre-moi les indicateurs."*
3. *"Quelle est la corrélation entre le Fear & Greed index et le prix du Bitcoin sur les 30 derniers jours ?"*
4. *"Quels coins ont un volume anormalement élevé par rapport à leur capitalisation ?"*
5. *"Compare la performance des Layer 1 vs Layer 2 sur les 7 derniers jours."*
6. *"Quel est le pourcentage de coins dans chaque catégorie de momentum (STRONG_BUY à STRONG_SELL) ?"*

---

## 8. RESSOURCES CLÉS

| Ressource             | URL                                   | Usage                       |
| --------------------- | ------------------------------------- | --------------------------- |
| Bruin CLI Docs        | getbruin.com/docs/bruin/              | Référence technique         |
| Bruin Academy         | getbruin.com/learn/                   | Tutoriels vidéo             |
| Bruin MCP Setup       | (dans les notes du Module 5 Zoomcamp) | Config AI agent             |
| CoinGecko API Docs    | docs.coingecko.com                    | Documentation des endpoints |
| CoinGecko Free Plan   | coingecko.com/en/api                  | 30 calls/min, 10K/mois      |
| Fear & Greed API      | api.alternative.me/fng/               | API gratuite, pas de clé    |
| Bruin Cloud           | cloud.getbruin.com                    | Déploiement + AI analyst    |
| DuckDB Docs           | duckdb.org/docs                       | Fonctions SQL analytiques   |
| Slack Bruin Community | (lien dans la page compétition)       | Support + post projet       |

---

## 9. TIMELINE RECOMMANDÉE

| Jour          | Focus                                | Livrable                       |
| ------------- | ------------------------------------ | ------------------------------ |
| **J1 (3-4h)** | Setup + Ingestion complète           | Toutes les données dans DuckDB |
| **J2 (3-4h)** | Staging + Analytics + Quality checks | Pipeline complet fonctionnel   |
| **J3 (2-3h)** | AI Analyst + README + LinkedIn       | Projet publié et soumis        |

**Temps total estimé : 8-12 heures** pour un Senior Data Engineer.

---

*Ce plan est conçu pour maximiser vos chances dans les 3 catégories de prix. Bonne chance ! 🚀*
