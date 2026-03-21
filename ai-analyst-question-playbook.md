# 🤖 CryptoFlow Analytics — Bruin AI Analyst Question Playbook

> Structured questions to ask the Bruin AI Data Analyst for maximum impact.
> Each question is designed to produce a screenshot-worthy answer.

---

## How the AI Analyst Works

The Bruin AI Analyst is a conversational interface (available in Bruin Cloud UI and Slack) that:

1. Reads your question in natural language
2. Understands your pipeline schema (tables, columns, relationships)
3. Generates SQL to answer the question
4. Executes it against your database
5. Returns formatted results + the SQL used

**Before you start:**

1. Deploy your pipeline to Bruin Cloud (connect GitHub repo)
2. Run the pipeline at least once so tables are populated
3. Run `bruin ai enhance` to enrich metadata (descriptions, lineage)
4. Create an AI agent in Bruin Cloud → Agents section
5. Open the agent in Cloud UI or connect it to Slack

---

## Screenshot Strategy

You need **5-8 screenshots** for the competition:

- **4 minimum** for the README (AI Analyst Insights section)
- **3-4 extra** for the LinkedIn post

**What makes a great screenshot:**

- The question is clearly visible at the top
- The AI shows the SQL it generated (proves it understood the schema)
- The result table has meaningful, interpretable data
- The answer demonstrates cross-table analysis (not just SELECT * FROM one table)

**Screenshot tips:**

- Use Bruin Cloud UI (browser) for cleaner screenshots than Slack
- Expand the full result if it fits
- Include the SQL panel open — reviewers love seeing the generated query
- Crop tightly: question + SQL + result, no browser chrome

---

## CATEGORY 1 — Competition Must-Haves (4 screenshots for README)

These questions directly showcase the pipeline's core analytics.
Each maps to a key feature you want judges to notice.

### 1.1 — Market Regime (Crown Jewel)

```
What is the current market regime? Show me the regime classification,
the score, the percentage of coins trending up across 24h/7d/30d,
and the overall narrative.
```

**Why this question:**

- Hits `analytics.market_regime` — your most complex and unique asset
- The narrative field produces a human-readable summary
- Shows multi-dimensional analysis (breadth + sentiment + global metrics)

**Expected output:** Single row with regime, score, breadth percentages, sentiment, narrative.

**Screenshot name:** `market_regime_analysis.png`

---

### 1.2 — Momentum Signals (Key Differentiator)

```
Show me the top 10 coins by momentum score with their signal,
confidence level, and the Fear & Greed index value.
Sort by momentum score descending.
```

**Why this question:**

- Hits `analytics.momentum_signals` — your proprietary scoring system
- Cross-references sentiment data (Fear & Greed)
- Clean tabular output with actionable BUY/SELL signals

**Expected output:** 10 rows with name, momentum_score, signal, confidence, fear_greed_index.

**Screenshot name:** `top_momentum_signals.png`

---

### 1.3 — Sentiment Trend (Shows Data Depth)

```
How has market sentiment evolved over the last 90 days?
Show me the distribution of days spent in each sentiment zone
and what the current trend direction is.
```

**Why this question:**

- Hits `analytics.fear_greed_impact` — sentiment zone analysis
- Shows time-series awareness (90 days of history)
- Produces a distribution table (% of days in each zone)

**Expected output:** Rows per sentiment zone with days_count, pct_of_total_days, current state.

**Screenshot name:** `sentiment_distribution.png`

---

### 1.4 — Top Performers (Universally Understood)

```
What are the top 5 biggest gainers and top 5 biggest losers
over the past 7 days? Include their price tier and percentage change.
```

**Why this question:**

- Hits `analytics.top_performers` — everyone understands winners/losers
- Visually impactful (green vs red, large % numbers)
- Shows the pipeline handles multi-timeframe analysis

**Expected output:** 10 rows with gainers and losers, change_pct, price_tier.

**Screenshot name:** `top_performers_7d.png`

---

## CATEGORY 2 — LinkedIn Wow Factor (3-4 screenshots)

These questions demonstrate analytical depth that impresses
data engineers and crypto enthusiasts on LinkedIn.

### 2.1 — Cross-Category Comparison

```
Compare the average momentum score and volatility score
between Layer 1 coins, DeFi coins, and Meme coins.
Which category has the strongest buy signals right now?
```

**Why this question:**

- Forces a JOIN between `analytics.momentum_signals` and the coin categories seed
- Cross-category comparison is visually compelling
- Shows business-level insight, not just raw data

**Expected output:** 3 rows (L1, DeFi, Meme) with avg scores and signal distribution.

**Screenshot name:** `category_comparison.png`

---

### 2.2 — Whale Detection / Volume Anomalies

```
Which coins have abnormally high trading volume relative
to their market cap? Show any coins with a volume-to-market-cap
ratio above 0.3 and their volatility tier.
```

**Why this question:**

- Hits `analytics.volatility_analysis` with volume_activity_level
- "Whale detection" is a buzzword that catches attention on LinkedIn
- Demonstrates the pipeline goes beyond basic price tracking

**Expected output:** Variable rows (depends on market conditions), with volume ratios and tiers.

**Screenshot name:** `volume_anomalies.png`

---

### 2.3 — Contrarian Signal (Intellectually Interesting)

```
Are there any coins that show STRONG_BUY signals while the overall
market sentiment is in the Fear zone? This would indicate
contrarian buying opportunities.
```

**Why this question:**

- Demonstrates the contrarian logic built into momentum_signals
- "Buy when there's blood in the streets" resonates with crypto audience
- Shows the AI analyst can reason about signal + sentiment combinations

**Expected output:** Depends on market conditions — could be 0 rows (which is also interesting to explain).

**Screenshot name:** `contrarian_signals.png`

---

### 2.4 — Market Dominance Shift

```
Show me the market dominance breakdown by price tier.
What percentage of total market cap is controlled by mega-cap coins
versus all other tiers combined?
```

**Why this question:**

- Hits `analytics.market_dominance` with tier_dominance_pct
- Illustrates market concentration (likely 70%+ in mega_cap)
- Clean, simple output that's easy to read in a LinkedIn post

**Expected output:** 5 rows (one per tier) with dominance percentages.

**Screenshot name:** `dominance_by_tier.png`

---

## CATEGORY 3 — Verification & Debug (no screenshot needed)

Run these first to confirm the AI analyst understands your schema.
If these fail, check that `bruin ai enhance` ran correctly.

### 3.1 — Schema Discovery

```
What tables are available in this pipeline?
List all table names with their row counts.
```

**Purpose:** Verify the AI sees all 14 tables.

---

### 3.2 — Data Freshness

```
When was the most recent data ingested?
Show the latest ingested_at timestamp from raw.coin_markets.
```

**Purpose:** Confirm the pipeline ran recently and data is fresh.

---

### 3.3 — Quality Check Verification

```
Are there any NULL values in the signal column
of analytics.momentum_signals?
```

**Purpose:** Confirm quality checks are working (should return 0).

---

### 3.4 — Lineage Awareness

```
What tables does analytics.market_regime depend on?
Trace the full dependency chain back to raw sources.
```

**Purpose:** Test if the AI understands the DAG / lineage.

---

### 3.5 — Simple Aggregation

```
How many coins are in each price tier in stg.enriched_coins?
```

**Purpose:** Quick sanity check on staging layer.

---

## Execution Order

Follow this order when you sit down with the AI analyst:

### Step 1 — Verify (5 min, no screenshots)

Run questions 3.1 through 3.5 to confirm everything works.
Fix any issues before proceeding.

### Step 2 — Must-Have Screenshots (15 min)

Run questions 1.1 → 1.2 → 1.3 → 1.4 in order.
Screenshot each one with the SQL panel visible.

### Step 3 — LinkedIn Screenshots (15 min)

Run questions 2.1 → 2.2 → 2.3 → 2.4.
Screenshot the most visually impressive results.

### Step 4 — Pick Your Best 5-8

Select the screenshots that:

- Show the most data (avoid empty results)
- Have the clearest formatting
- Demonstrate different capabilities (regime, signals, comparison, anomalies)

---

## File Naming Convention

Save all screenshots in `docs/ai_analyst_screenshots/` with these names:

```
docs/ai_analyst_screenshots/
├── market_regime_analysis.png      # Must-have #1
├── top_momentum_signals.png        # Must-have #2
├── sentiment_distribution.png      # Must-have #3
├── top_performers_7d.png           # Must-have #4
├── category_comparison.png         # LinkedIn #1
├── volume_anomalies.png            # LinkedIn #2
├── contrarian_signals.png          # LinkedIn #3
└── dominance_by_tier.png           # LinkedIn #4
```

---

## Bonus: Alternative Questions If Market Conditions Are Unusual

If the market is completely flat (all NEUTRAL signals), try:

```
What coins have the highest intraday spread percentage today?
Even in a flat market, some coins move — which ones?
```

If Fear & Greed is stuck in one zone:

```
Show me the 7-day moving average of the Fear & Greed index.
Is it trending up or down compared to the 14-day average?
```

If few coins have STRONG_BUY signals:

```
Show me all coins with BUY or STRONG_BUY signals.
How does their average market cap rank compare to coins
with SELL signals?
```

---

## Bruin Cloud Setup Checklist (before asking questions)

- [ ] Register at cloud.getbruin.com
- [ ] Go to Team Settings → Projects → Add your GitHub repo
- [ ] Enable the `cryptoflow` pipeline
- [ ] Run the pipeline (or trigger backfill)
- [ ] Confirm all assets show green (successful) in the UI
- [ ] Run `bruin ai enhance` locally to enrich column descriptions
- [ ] Push the enhanced metadata to GitHub
- [ ] Go to Agents → Create new agent
- [ ] Select repo + pipeline + DuckDB connection
- [ ] Test in Cloud UI first, then optionally connect to Slack
