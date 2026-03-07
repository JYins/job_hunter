# Job Hunter V1: My Canada Intern/Co-op Workflow

This is my personal daily pipeline for finding better intern/co-op roles in Canada.
The goal is simple: spend less time doom-scrolling job boards, and more time applying to high-fit roles.

It does six things:
1. Fetches jobs from JobSpy
2. Ingests manually pasted alert links
3. Normalizes records into one schema
4. Deduplicates jobs
5. Scores and tiers jobs (A/B/C)
6. Exports a daily Excel report

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
