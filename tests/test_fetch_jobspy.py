from datetime import date
import json
import sys
import types

import pandas as pd
import yaml

from scripts.fetch_jobspy import fetch_jobspy_jobs


def _write_yaml(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _read_jsonl(path):
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def _prepare_sources(tmp_path, site_settings):
    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "raw" / "jobspy").mkdir(parents=True, exist_ok=True)
    (tmp_path / "logs").mkdir(parents=True, exist_ok=True)
    _write_yaml(
        tmp_path / "config" / "sources.yaml",
        {
            "jobspy": {
                "enabled": True,
                "site_settings": site_settings,
                "search_terms": ["software engineer intern canada"],
                "locations": ["Toronto, ON"],
                "days_back": 7,
                "results_wanted": 10,
                "country_indeed": "canada",
            }
        },
    )


def test_fetch_jobspy_site_failure_does_not_break_pipeline(tmp_path, monkeypatch):
    _prepare_sources(
        tmp_path,
        [
            {"name": "indeed", "enabled": True, "priority": 1},
            {"name": "linkedin", "enabled": True, "priority": 2},
        ],
    )
    monkeypatch.setitem(sys.modules, "jobspy", types.SimpleNamespace())

    def fake_safe_scrape(site_name, **kwargs):
        if site_name == "linkedin":
            raise RuntimeError("rate limited")
        return pd.DataFrame(
            [
                {
                    "title": "Software Intern",
                    "company": "Acme",
                    "location": "Toronto, ON",
                    "job_url": "https://example.com/indeed",
                    "site": "indeed",
                }
            ]
        )

    monkeypatch.setattr("scripts.fetch_jobspy._safe_scrape", fake_safe_scrape)
    result = fetch_jobspy_jobs(base_dir=tmp_path, run_date=date(2026, 3, 7))

    assert result["count"] == 1
    assert result["source_summary"] == {"indeed": 1}
    assert result["failure_summary"] == {"linkedin": 1}
    assert len(result["fetch_attempts"]) == 2

    rows = _read_jsonl(tmp_path / "data" / "raw" / "jobspy" / "jobspy_20260307.jsonl")
    assert len(rows) == 1
    assert rows[0]["source"] == "indeed"


def test_fetch_jobspy_multi_source_source_field_preserved(tmp_path, monkeypatch):
    _prepare_sources(
        tmp_path,
        [
            {"name": "indeed", "enabled": True, "priority": 1},
            {"name": "glassdoor", "enabled": True, "priority": 2},
        ],
    )
    monkeypatch.setitem(sys.modules, "jobspy", types.SimpleNamespace())

    def fake_safe_scrape(site_name, **kwargs):
        if site_name == "indeed":
            return pd.DataFrame(
                [{"title": "Role A", "company": "Acme", "location": "Toronto", "job_url": "https://a", "site": "indeed"}]
            )
        return pd.DataFrame(
            [{"title": "Role B", "company": "Beta", "location": "Toronto", "job_url": "https://b"}]
        )

    monkeypatch.setattr("scripts.fetch_jobspy._safe_scrape", fake_safe_scrape)
    result = fetch_jobspy_jobs(base_dir=tmp_path, run_date=date(2026, 3, 7))

    assert result["count"] == 2
    rows = _read_jsonl(tmp_path / "data" / "raw" / "jobspy" / "jobspy_20260307.jsonl")
    sources = {row["source"] for row in rows}
    assert sources == {"indeed", "glassdoor"}
