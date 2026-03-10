# Job Hunter V1: My Canada Intern/Co-op Workflow

This is my personal daily pipeline for finding better intern/co-op roles in Canada.
I built it for my own convenience so I can spend less time searching and more time applying.
The goal is simple: spend less time doom-scrolling job boards, and more time applying to high-fit roles.

It does eight things:
1. Fetches jobs from JobSpy
2. Fetches jobs from external internship sources (GitHub internship repos + Internee)
3. Fetches jobs from Western Connect (semi-automatic with Duo)
4. Ingests manually pasted alert links
5. Normalizes records into one schema
6. Deduplicates jobs
7. Scores and tiers jobs (A/B/C)
8. Exports a daily Excel report

---

## Quick Start

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python scripts/run_daily.py
```

---

## What I Do Every Day

1. Paste new job links from email alerts into `data/raw/alerts/links_today.txt` (one URL per line).
2. Run:

```bash
python scripts/run_daily.py
```

3. Open `data/processed/today_top_jobs.xlsx` and prioritize Top 20 + Tier A roles.

### Feedback Tracking (Applied / Replied / Interview / Rejected)

Use this to track application progress directly in `jobs_master.csv`:

```bash
python scripts/update_feedback.py --status applied --job-id <JOB_ID> --notes "submitted on company portal"
python scripts/update_feedback.py --status replied --job-url https://example.com/job/123
python scripts/update_feedback.py --status interview --contains-title "Software Intern"
python scripts/update_feedback.py --status rejected --job-id <JOB_ID>
```

Feedback fields are exported to Excel and summarized in the `feedback_summary` sheet.

### UWO Connect (Duo) Semi-Automatic Fetch

I added Western Connect scraping because it is one of the most useful sources for my own school-focused applications.
Because Western uses Duo, the safest workflow is semi-automatic: I complete login/MFA in the browser, then the script continues.

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
  # For non-interactive runs (no Enter prompt), use a timed wait:
  # manual_wait_seconds: 120
  # extract_details: true
  # max_pages: 3
```

The script stores browser session data in `data/raw/uwo_connect_session` and writes records to
`data/raw/alerts/uwo_connect_YYYYMMDD.jsonl`.

---

## Core Inputs to Configure

- `data/profile/user_profile.md`
- `data/profile/skills_master.yaml`
- `data/profile/target_companies.yaml`
- `data/profile/search_config.yaml`
- `config/sources.yaml`
- `config/scoring_config.yaml`

These files define what "good fit" means for me.

---

## Scoring Model (with NLP)

Final score:

```text
final_score = 0.40 * rule
            + 0.20 * keyword
            + 0.20 * semantic
            + 0.10 * freshness
            + 0.10 * company
```

### 1) Rule Score (40%)

Hard filters and intent checks:
- intern/co-op/new grad signal
- Canada location signal (city matches or Remote Canada)
- seniority exclusions (senior/staff/principal/etc.)

### 2) Keyword Score (20%)

Keyword overlap between the job text and:
- `search_config.yaml` role keywords
- `skills_master.yaml` skill inventory

More overlap means a higher score.

### 3) Semantic Score (NLP, 20%)

Uses `sentence-transformers/all-MiniLM-L6-v2`.

Pipeline:
1. Build a profile text from `user_profile.md` + structured skills.
2. Build a job text from `title + description`.
3. Encode both into embeddings.
4. Compute cosine similarity.
5. Map similarity to a stable `0..1` score.

Why this matters:
- Keyword matching misses good roles with different wording.
- Semantic matching catches jobs that are conceptually aligned, even when exact terms differ.

Runtime behavior:
- CPU by default
- Auto-uses CUDA when available
- If model load fails, semantic score gracefully falls back to `0` (pipeline still runs)

### 4) Freshness Score (10%)

Newer jobs get higher scores so I can apply earlier.

### 5) Company Score (10%)

Boosts roles from preferred companies in `target_companies.yaml`:
- `tier_a`: strongest boost
- `tier_b`: medium boost
- `tier_c`: light boost

---

## Outputs

- Canonical dataset: `data/processed/jobs_master.csv`
- Daily report: `data/processed/today_top_jobs.xlsx`
- Logs: `logs/run_YYYYMMDD.log`

Excel sheets:
- `top_20`
- `all_scored`
- `source_summary`
- `tier_summary`

---

## Tests

```bash
pytest -q
```

Current coverage includes:
- smoke test for end-to-end daily run
- dedupe behavior
- weighted scoring + tier thresholds
- semantic runtime device fallback

---

## Note

This is a prioritization workflow, not an auto-apply bot.
The system filters and ranks jobs; I still do high-quality human applications.
