# Job Hunter V1 (Canada Intern/Co-op)

Week-1 pipeline that:
- fetches jobs from JobSpy,
- ingests manual links from alert emails,
- normalizes and deduplicates jobs,
- scores jobs (rule + keyword + semantic + freshness + company),
- exports a daily Excel report with Top 20 + A/B/C tiering.

## 1) Setup

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## 2) Configure profile inputs

Review and update:
- `data/profile/user_profile.md`
- `data/profile/skills_master.yaml`
- `data/profile/target_companies.yaml`
- `data/profile/search_config.yaml`
- `config/sources.yaml`
- `config/scoring_config.yaml`

## 3) Daily manual links input

Paste links into:
- `data/raw/alerts/links_today.txt`

Contract:
- one URL per line,
- blank lines allowed,
- lines starting with `#` are ignored.

## 4) Run pipeline

```bash
python scripts/run_daily.py
```

## 5) Outputs

- Canonical dataset: `data/processed/jobs_master.csv`
- Daily report: `data/processed/today_top_jobs.xlsx`
- Logs: `logs/run_YYYYMMDD.log`

## Optional: run tests

```bash
pytest
```
