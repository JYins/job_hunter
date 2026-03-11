# Job Hunter V1: My Canada Intern/Co-op Workflow

I use this repo as my daily job hunting pipeline for Canada internships/co-ops.
The goal is simple: spend less time scrolling and more time applying to high-fit roles.

## What It Does

This pipeline runs 8 stages:
1. Fetch jobs from JobSpy
2. Fetch jobs from external sources (GitHub internship repos + Internee)
3. Fetch jobs from Western Connect (semi-automatic with Duo)
4. Ingest manual links I paste from alerts
5. Normalize records into one schema
6. Deduplicate jobs
7. Score and tier jobs (A/B/C)
8. Export a daily Excel report

## Quick Start

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python scripts/run_daily.py
```

## My Daily Flow

1. Paste fresh links into `data/raw/alerts/links_today.txt` (one URL per line).
2. Run:

```bash
python scripts/run_daily.py
```

3. Open `data/processed/today_top_jobs.xlsx` and apply from Top 20 / Tier A first.

## Feedback Tracking

I track application progress directly in `jobs_master.csv`:

```bash
python scripts/update_feedback.py --status applied --job-id <JOB_ID> --notes "submitted on company portal"
python scripts/update_feedback.py --status replied --job-url https://example.com/job/123
python scripts/update_feedback.py --status interview --contains-title "Software Intern"
python scripts/update_feedback.py --status rejected --job-id <JOB_ID>
```

Statuses and notes are exported to the `feedback_summary` sheet.

## UWO Connect (Duo) Fetch

I use a semi-automatic flow for Western Connect: I complete login/MFA manually, then the script continues.

```bash
python scripts/fetch_uwo_connect.py
```

Enable it in `config/sources.yaml`:

```yaml
uwo_connect:
  enabled: true
  dashboard_url: https://connect.uwo.ca/myAccount/dashboard.htm
  headless: false
  require_manual_confirm: true
  # optional:
  # manual_wait_seconds: 120
  # extract_details: true
  # max_pages: 3
```

Session data is saved in `data/raw/uwo_connect_session`.

## Core Config Files

- `data/profile/user_profile.md`
- `data/profile/skills_master.yaml`
- `data/profile/target_companies.yaml`
- `data/profile/search_config.yaml`
- `config/sources.yaml`
- `config/company_careers.yaml`
- `config/scoring_config.yaml`

These files define my targeting strategy.

## Search & Keyword Strategy

I organize search into 5 tracks:
- SWE / Backend / Platform
- Data / ML Evaluation
- Perception / CV / Autonomous Driving
- Robotics Software
- AI Application / NLP / RAG

`search_config.yaml` stores track-level fetch queries and keyword terms.
`fetch_jobspy.py` auto-expands search terms from that profile config, so updating strategy in one place updates crawling behavior.

## Scoring Model (NLP-Structured)

```text
final_score = 0.35 * rule
            + 0.37 * nlp
            + 0.15 * freshness
            + 0.10 * company
            + 0.05 * source

nlp = 0.45 * lexical
    + 0.40 * semantic
    + 0.15 * intent
```

### Rule Score (35%)
- internship/co-op/new grad signal
- Canada location signal
- seniority penalty

### NLP Score (40%)
- `lexical`: phrase/term matching over role + skill vocabulary
- `semantic`: embedding similarity (`sentence-transformers/all-MiniLM-L6-v2`)
- `intent`: alignment to one of my 5 target tracks

The pipeline also writes interpretable columns:
- `keyword_score`
- `semantic_score`
- `intent_score`
- `nlp_score`
- `matched_track`
- `recommended_resume`

### Freshness Score (15%)
Newer jobs get a higher score so I can apply earlier.

### Company Score (10%)
Boost from company tiers in `target_companies.yaml`.

### Source Score (5%)
Official company career pages get the strongest trust boost, followed by manual links and higher-signal sources.

## Outputs

- Canonical data: `data/processed/jobs_master.csv`
- Daily report: `data/processed/today_top_jobs.xlsx`
- Logs: `logs/run_YYYYMMDD.log`

Excel sheets:
- `top_20`
- `all_scored`
- `source_summary`
- `tier_summary`
- `feedback_summary`

## Tests

```bash
pytest -q
```

## Note

This is a prioritization system, not an auto-apply bot.
I still review and submit applications manually.
