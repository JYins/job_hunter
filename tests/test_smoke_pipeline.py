from datetime import date

import pandas as pd
import yaml

from scripts.run_daily import run_pipeline


def _write_yaml(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def test_smoke_run_daily_pipeline(tmp_path):
    for rel in [
        "config",
        "data/profile",
        "data/raw/alerts",
        "data/raw/jobspy",
        "data/processed",
        "data/outputs",
        "data/reports",
        "logs",
    ]:
        (tmp_path / rel).mkdir(parents=True, exist_ok=True)

    _write_yaml(
        tmp_path / "config" / "sources.yaml",
        {
            "jobspy": {
                "enabled": False,
                "sites": ["indeed"],
                "search_terms": ["software engineer intern canada"],
                "locations": ["Toronto, ON"],
                "days_back": 7,
                "results_wanted": 10,
                "country_indeed": "canada",
            },
            "manual_links": {"enabled": True, "path": "data/raw/alerts/links_today.txt"},
        },
    )
    _write_yaml(
        tmp_path / "config" / "scoring_config.yaml",
        {
            "weights": {"rule": 0.4, "keyword": 0.2, "semantic": 0.0, "freshness": 0.2, "company": 0.2},
            "tiers": {"a_min": 0.75, "b_min": 0.55},
            "semantic": {"enabled": False, "model_name": "sentence-transformers/all-MiniLM-L6-v2"},
            "dedupe": {"title_similarity_threshold": 0.92},
        },
    )
    _write_yaml(
        tmp_path / "data" / "profile" / "search_config.yaml",
        {
            "locations": ["Toronto", "Waterloo", "Vancouver", "Montreal", "Remote Canada"],
            "job_types": ["intern", "co-op", "new grad"],
            "keywords": ["python", "robotics", "machine learning"],
            "exclude": ["senior", "staff", "principal"],
        },
    )
    _write_yaml(
        tmp_path / "data" / "profile" / "skills_master.yaml",
        {
            "languages": ["python", "c++"],
            "ml": ["pytorch", "computer vision"],
            "robotics": ["ros", "perception"],
            "software": ["git", "linux"],
        },
    )
    _write_yaml(tmp_path / "data" / "profile" / "target_companies.yaml", {"tier_a": ["Acme Robotics"], "tier_b": []})
    (tmp_path / "data" / "profile" / "user_profile.md").write_text("robotics intern profile", encoding="utf-8")
    (tmp_path / "data" / "raw" / "alerts" / "links_today.txt").write_text(
        "\n".join(
            [
                "https://example.com/job/robotics-intern",
                "https://example.com/job/ml-intern",
            ]
        ),
        encoding="utf-8",
    )

    run_pipeline(base_dir=tmp_path, run_date=date(2026, 3, 7))

    report = tmp_path / "data" / "processed" / "today_top_jobs.xlsx"
    master = tmp_path / "data" / "processed" / "jobs_master.csv"
    log_path = tmp_path / "logs" / "run_20260307.log"
    assert report.exists()
    assert master.exists()
    assert log_path.exists()
    log_text = log_path.read_text(encoding="utf-8")
    assert "Source summary: fetched=" in log_text
    assert "3-run acceptance gate" in log_text

    df = pd.read_csv(master)
    assert len(df) == 2
    assert df["tier"].notna().all()
    assert df["recommended_resume"].notna().all()

    top_df = pd.read_excel(report, sheet_name="top_20")
    assert top_df["final_score"].is_monotonic_decreasing
