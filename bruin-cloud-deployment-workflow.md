# ☁️ CryptoFlow Analytics — Bruin Cloud Deployment Workflow

> Step-by-step guide with exact timing relative to the git-commit-guide.md

---

## When to Start — Mapping to Git Commits

```
COMMIT 1-11  ← LOCAL DEVELOPMENT (your machine only)
                Do NOT touch Bruin Cloud yet.
                Focus: build, test, iterate.

COMMIT 12    ← FULL PIPELINE VALIDATION (local)
                Everything runs end-to-end.
                This is your GO / NO-GO checkpoint.
                ✅ If all 14 assets pass → proceed to Cloud.
                ❌ If anything fails → fix locally first.

─── CLOUD DEPLOYMENT STARTS HERE ───

COMMIT 12.5  ← BRUIN AI ENHANCE (new step, between 12 and 13)
                Run `bruin ai enhance` to enrich metadata.
                Commit the enhanced files.

COMMIT 13    ← GLOSSARY (already planned)

COMMIT 14    ← SETUP SCRIPT (already planned)

─── BRUIN CLOUD CONFIGURATION HERE ───

STEP A       ← Register + connect GitHub repo on Bruin Cloud
STEP B       ← Add connections (DuckDB or BigQuery)
STEP C       ← Enable pipeline + first Cloud run
STEP D       ← Create AI Agent
STEP E       ← Test AI Analyst (Cloud UI)
STEP F       ← Capture screenshots

─── BACK TO GIT ───

COMMIT 15    ← README (already planned)
COMMIT 16    ← Communication templates (already planned)
COMMIT 17    ← AI ANALYST SCREENSHOTS (commit the captured images)
COMMIT 18    ← Final README update with screenshot references
```

**The key insight: Cloud deployment happens AFTER commit 12 (full local validation) and BEFORE commit 15 (README). This way your README already references real screenshots and real Cloud deployment.**

---

## Pre-requisite Checklist

Before starting the Cloud workflow, confirm:

- [ ] All 18 commits through commit 12 are done
- [ ] `bruin run .` succeeds locally with all 14 assets green
- [ ] `bruin validate .` passes with no errors
- [ ] `crypto.db` contains all expected tables
- [ ] GitHub repo is pushed to `main` branch (commits 1-12 minimum)

---

## STEP 0 — Bruin AI Enhance (between commits 12 and 13)

> This enriches your asset metadata with auto-generated descriptions,
> which dramatically improves AI Analyst response quality.

```bash
# Run from your project root
bruin ai enhance .
```

This command will:

- Analyze all your SQL and Python assets
- Auto-generate column descriptions where missing
- Enhance lineage metadata
- Update asset files in-place

```bash
# Review what changed
git diff

# Commit the enriched metadata
git add -A
git commit -m "chore: enrich asset metadata with bruin ai enhance

- Auto-generated column descriptions for staging and analytics assets
- Enhanced lineage metadata for AI Analyst query accuracy
- Ran bruin ai enhance across all 14 assets"
```

```bash
git push origin main
```

---

## STEP A — Register on Bruin Cloud

### A.1 — Create account

1. Go to **https://cloud.getbruin.com/register**
2. Sign up with GitHub (recommended) or email
3. Confirm your email if required

### A.2 — Connect GitHub repository

1. In Bruin Cloud dashboard, go to **Team Settings** → **Projects**
2. Click **Add Project** or **Connect Repository**
3. Authorize Bruin to access your GitHub account
4. Select the `cryptoflow-analytics` repository
5. Confirm — Bruin Cloud will now track your `main` branch

### A.3 — Verify repo is linked

- You should see `cryptoflow-analytics` listed under Projects
- The `cryptoflow` pipeline (from your `pipeline.yml`) should appear
- Status should show the pipeline detected but not yet run

---

## STEP B — Configure Connections

> Bruin Cloud needs to know WHERE to run your queries.
> For the hackathon, DuckDB is the simplest option.
> BigQuery is optional (bonus for production credibility).

### Option 1: DuckDB (simplest, recommended)

1. In Bruin Cloud → your project → **Connections**
2. Add a DuckDB connection:
   - **Name:** `duckdb-default` (must match `.bruin.yml`)
   - **Path:** `crypto.db`
3. Save

### Option 2: BigQuery (bonus, if you have a GCP project)

1. In Bruin Cloud → your project → **Connections**
2. Add a Google Cloud Platform connection:
   - **Name:** `gcp`
   - **Project ID:** your GCP project ID
   - **Service Account:** upload your key JSON or paste it
3. Save
4. You would also need to update your SQL assets to use `bq.sql` type instead of `duckdb.sql`

**For the hackathon: stick with DuckDB unless you already have BigQuery set up.**

---

## STEP C — Enable Pipeline and First Run

### C.1 — Enable the pipeline

1. In Bruin Cloud → **Pipelines** → `cryptoflow`
2. Toggle the pipeline to **Enabled**
3. The daily schedule from `pipeline.yml` will activate

### C.2 — Trigger first run manually

1. Click **Run Now** or **Trigger Run**
2. Watch the execution in the dashboard
3. Each asset should show:
   - ⏳ Pending → 🔄 Running → ✅ Success
4. Check that ALL assets reach ✅

### C.3 — Troubleshoot if needed

Common issues:

| Problem                                       | Fix                                                                                                                                         |
| --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| Python asset fails with `ModuleNotFoundError` | Ensure `pandas` and `requests` are in the Cloud environment. Check if Bruin Cloud auto-installs pip dependencies.                           |
| Connection not found                          | Verify the connection name in Cloud matches exactly what's in `.bruin.yml`                                                                  |
| API rate limit hit                            | CoinGecko free tier is 30 calls/min. If assets run in parallel, you may hit limits. Add `time.sleep(2)` between API calls in Python assets. |
| DuckDB path error                             | In Cloud, the path might differ from local. Check Cloud docs for DuckDB file handling.                                                      |
| Asset dependency fails                        | Run `bruin validate .` locally again. Ensure all `depends` references match exact asset names.                                              |

### C.4 — Verify data in Cloud

Once the run succeeds:

1. Use the Bruin Cloud query interface (if available) to spot-check:
   
   ```sql
   SELECT COUNT(*) FROM raw.coin_markets;
   SELECT regime, regime_score FROM analytics.market_regime;
   ```

2. Confirm row counts match your local DuckDB

---

## STEP D — Create AI Agent

### D.1 — Navigate to Agents

1. In Bruin Cloud → **Agents** (left sidebar)
2. Click **Create New Agent**

### D.2 — Configure the Agent

- **Name:** `CryptoFlow Analyst` (or any descriptive name)
- **Repository:** select `cryptoflow-analytics`
- **Pipeline:** select `cryptoflow`
- **Connection:** select `duckdb-default`

### D.3 — (Optional) Connect to Slack

If you want Slack screenshots in addition to Cloud UI:

1. In the Agent config, look for **Slack Integration**
2. You need:
   - A Slack workspace where you have admin/bot permissions
   - A Slack Bot Token (create via Slack API → Apps → Create New App)
   - A target channel (e.g., `#crypto-analytics`)
3. Enter the bot token and channel
4. Save

**For the hackathon: Cloud UI screenshots are sufficient. Slack is a nice bonus but not required.**

### D.4 — Verify Agent is active

- The Agent should show as **Active** or **Ready**
- You should see a chat/query interface in the Cloud UI

---

## STEP E — Test AI Analyst (Cloud UI)

### E.1 — Run verification questions first

Open the Agent in Cloud UI and ask these (from the question playbook):

```
What tables are available in this pipeline?
List all table names with their row counts.
```

Expected: 14 tables listed. If not, the Agent may not see all assets → check connection config.

```
When was the most recent data ingested?
Show the latest ingested_at timestamp from raw.coin_markets.
```

Expected: A recent timestamp. If old or NULL → re-run the pipeline.

```
How many coins are in each price tier in stg.enriched_coins?
```

Expected: 5 tiers with counts summing to ~100.

### E.2 — Run must-have screenshot questions

Once verification passes, run each question from the playbook Category 1:

**Question 1 — Market Regime:**

```
What is the current market regime? Show me the regime classification,
the score, the percentage of coins trending up across 24h/7d/30d,
and the overall narrative.
```

**Question 2 — Momentum Signals:**

```
Show me the top 10 coins by momentum score with their signal,
confidence level, and the Fear & Greed index value.
Sort by momentum score descending.
```

**Question 3 — Sentiment Distribution:**

```
How has market sentiment evolved over the last 90 days?
Show me the distribution of days spent in each sentiment zone
and what the current trend direction is.
```

**Question 4 — Top Performers:**

```
What are the top 5 biggest gainers and top 5 biggest losers
over the past 7 days? Include their price tier and percentage change.
```

### E.3 — Run LinkedIn bonus questions

From playbook Category 2 (pick the 2-3 best results):

```
Compare the average momentum score and volatility score
between Layer 1 coins, DeFi coins, and Meme coins.
Which category has the strongest buy signals right now?
```

```
Which coins have abnormally high trading volume relative
to their market cap? Show any coins with a volume-to-market-cap
ratio above 0.3 and their volatility tier.
```

```
Show me the market dominance breakdown by price tier.
What percentage of total market cap is controlled by mega-cap coins
versus all other tiers combined?
```

---

## STEP F — Capture Screenshots

### F.1 — Screenshot technique

1. Use your browser (Chrome/Firefox) in Bruin Cloud UI
2. For each question:
   - Ensure the **question text** is visible
   - Ensure the **SQL panel** is expanded (shows generated query)
   - Ensure the **result table** is fully visible
3. Crop tightly: question + SQL + results, no browser tabs/bookmarks
4. Use native screenshot: `Cmd+Shift+4` (Mac) or `Win+Shift+S` (Windows)
5. Save as PNG

### F.2 — Save with correct names

```bash
# In your project directory
docs/ai_analyst_screenshots/
├── market_regime_analysis.png      # Question 1
├── top_momentum_signals.png        # Question 2
├── sentiment_distribution.png      # Question 3
├── top_performers_7d.png           # Question 4
├── category_comparison.png         # LinkedIn bonus
├── volume_anomalies.png            # LinkedIn bonus
└── dominance_by_tier.png           # LinkedIn bonus
```

### F.3 — Commit screenshots (= Commit 17 from git-commit-guide)

```bash
git add docs/ai_analyst_screenshots/
git commit -m "docs: add Bruin AI analyst screenshots

- Market regime analysis showing current Bull/Bear classification
- Top 10 momentum signals with BUY/SELL indicators
- 90-day sentiment distribution across fear/greed zones
- 7-day top performers (gainers and losers)
- Category comparison (Layer 1 vs DeFi vs Meme)
- Volume anomaly detection results
- Market dominance by price tier"
```

---

## STEP G — (Bonus) GitHub Actions CI

> Optional but impressive. Adds automated validation on every push.

Create `.github/workflows/validate.yml`:

```yaml
name: Bruin Validate

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: bruin-data/setup-bruin@main

      - name: Validate pipeline
        run: bruin validate .

      - name: Check lineage
        run: |
          bruin lineage assets/analytics/market_regime.sql
          bruin lineage assets/analytics/momentum_signals.sql
```

```bash
mkdir -p .github/workflows
# Save the file above

git add .github/
git commit -m "ci: add GitHub Actions pipeline validation

- Validates all assets on every push to main
- Checks lineage for key analytics assets
- Uses official bruin-data/setup-bruin action"
```

---

## Complete Timeline — All Steps in Order

```
DAY 1 — Local Development
──────────────────────────
Commit 1   chore: initialize project structure
Commit 2   feat(ingestion): add coin categories seed
Commit 3   feat(ingestion): add CoinGecko market data
Commit 4   feat(ingestion): add Fear & Greed Index
Commit 5   feat(ingestion): add global market + trending
Commit 6   feat(staging): add enriched coins
Commit 7   feat(staging): add sentiment + global transforms
Commit 8   feat(analytics): market dominance + top performers
Commit 9   feat(analytics): volatility analysis
Commit 10  feat(analytics): momentum signals
Commit 11  feat(analytics): sentiment impact + market regime
Commit 12  [TAG v0.1.0] Full pipeline validation
           └─ rm crypto.db && bruin run .  ← clean test
           └─ bruin validate .
           └─ git push origin main

DAY 2 — Metadata + Cloud
─────────────────────────
Commit 12.5  chore: bruin ai enhance (metadata enrichment)
             └─ bruin ai enhance .
             └─ git push origin main

         ┌── BRUIN CLOUD WORKFLOW ──┐
         │ STEP A: Register + link GitHub repo
         │ STEP B: Configure DuckDB connection
         │ STEP C: Enable pipeline + first Cloud run
         │ STEP D: Create AI Agent
         │ STEP E: Test AI Analyst (verification questions)
         │         Then run screenshot questions
         │ STEP F: Capture 5-8 screenshots
         └──────────────────────────┘

Commit 13  docs: add Bruin glossary
Commit 14  chore: add setup script
Commit 15  docs: add comprehensive README
Commit 16  docs: add competition submission templates

DAY 3 — Polish + Publish
─────────────────────────
Commit 17  docs: add AI analyst screenshots
Commit 18  docs: update README with screenshot references
Commit G   ci: add GitHub Actions validation (bonus)
           └─ [TAG v1.0.0] Competition submission ready
           └─ git push origin main --tags

         ┌── PUBLISH ──┐
         │ Post on Slack #projects
         │ Publish LinkedIn post
         └─────────────┘
```

---

## Key Decisions Explained

**Why not deploy to Cloud earlier (e.g., after commit 5)?**
Because your pipeline is incomplete. Deploying a half-built pipeline to Cloud means:

- Failed runs in your Cloud history (looks unprofessional)
- Wasted time debugging Cloud-specific issues while still building locally
- AI Analyst can't answer interesting questions with incomplete data

**Why run `bruin ai enhance` before Cloud?**
The AI enhance command adds rich column descriptions and metadata that the AI Analyst uses to understand your schema. Without it, the Agent generates less accurate SQL and gives weaker answers. The enhanced metadata also improves the Cloud dashboard documentation.

**Why commit screenshots AFTER the README?**
Actually, looking at the timeline again: you should capture screenshots (Step F) BEFORE writing the README (commit 15), because the README references the screenshots. The updated order in the timeline above reflects this — Steps A-F happen between commits 12.5 and 15.

**Can I skip Bruin Cloud entirely?**
Technically, for the **Participation** prize, the requirement says "analysis (using the AI data analyst)." If you can demonstrate AI analysis through Bruin MCP locally (via Cursor/VS Code), that might suffice. But for **Top 3** and **Outstanding** prizes, Cloud screenshots are much more impressive and explicitly mentioned in the competition page.

---

## Troubleshooting Bruin Cloud

| Issue                                                | Solution                                                                                                      |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| Repo not appearing in Projects                       | Check GitHub app authorization. Re-authorize if needed.                                                       |
| Pipeline shows "No assets detected"                  | Ensure `pipeline.yml` is at the repo root, not in a subfolder.                                                |
| Python dependencies missing                          | Check if Bruin Cloud provides a way to specify `requirements.txt`. Add `pandas` and `requests`.               |
| AI Agent returns "I don't have access to any tables" | Verify connection name matches between Cloud config and `.bruin.yml`. Re-run pipeline.                        |
| AI Agent generates wrong SQL                         | Run `bruin ai enhance` again locally, push, and wait for Cloud to sync.                                       |
| Screenshots look empty/minimal                       | Re-run the pipeline to refresh data, then retry the AI questions.                                             |
| CoinGecko API returns 429 (rate limit)               | Free tier is 30 calls/min. If parallel execution causes issues, add rate limiting or run assets sequentially. |

---

## Video Resources

These tutorials show the exact Cloud workflow:

- **Deploying to Bruin Cloud:** https://youtu.be/uBqjLEwF8rc
- **Slack AI Analyst (stock market):** https://youtu.be/H02v3_rJhak
- **Local AI Analyst with Claude Code:** https://youtu.be/2emp-16nsZU
- **Bruin MCP + AI Agents:** https://youtu.be/224xH7h8OaQ

Watch the first two before starting the Cloud workflow. They are 15-20 minutes each and show the exact UI steps.
