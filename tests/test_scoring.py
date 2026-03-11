from datetime import date

import pandas as pd
import yaml

from scripts.score_jobs import score_jobs


def _write_yaml(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def test_score_weights_and_tiers(tmp_path):
    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "profile").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "processed").mkdir(parents=True, exist_ok=True)
    (tmp_path / "logs").mkdir(parents=True, exist_ok=True)

    _write_yaml(
        tmp_path / "config" / "scoring_config.yaml",
        {
            "weights": {"rule": 1.0, "keyword": 0.0, "semantic": 0.0, "freshness": 0.0, "company": 0.0},
            "tiers": {"a_min": 0.8, "b_min": 0.4},
            "semantic": {"enabled": False, "model_name": "sentence-transformers/all-MiniLM-L6-v2"},
        },
    )
    _write_yaml(
        tmp_path / "data" / "profile" / "search_config.yaml",
        {"locations": ["Toronto"], "job_types": ["intern"], "keywords": ["python"], "exclude": ["senior"]},
    )
    _write_yaml(
        tmp_path / "data" / "profile" / "skills_master.yaml",
        {"languages": ["python"], "ml": ["pytorch"], "robotics": ["ros"], "software": ["linux"]},
    )
    _write_yaml(tmp_path / "data" / "profile" / "target_companies.yaml", {"tier_a": ["Acme"]})
    (tmp_path / "data" / "profile" / "user_profile.md").write_text("profile", encoding="utf-8")

    rows = [
        {
            "job_id": "1",
            "title": "Software Intern",
            "company": "Acme",
            "location": "Toronto, Canada",
            "job_url": "https://example.com/a",
            "apply_url": "https://example.com/a",
            "source": "test",
            "date_posted": "2026-03-07",
            "date_found": "2026-03-07",
            "description": "python intern role",
            "keywords": "",
            "search_term": "intern",
            "is_intern_like": False,
            "is_canada_like": False,
            "is_senior_like": False,
            "rule_score": 0.0,
            "keyword_score": 0.0,
            "semantic_score": 0.0,
            "freshness_score": 0.0,
            "company_score": 0.0,
            "final_score": 0.0,
            "tier": "",
            "recommended_resume": "",
        },
        {
            "job_id": "2",
            "title": "Senior Engineer",
            "company": "OtherCo",
            "location": "Seattle, WA",
            "job_url": "https://example.com/b",
            "apply_url": "https://example.com/b",
            "source": "test",
            "date_posted": "2026-03-07",
            "date_found": "2026-03-07",
            "description": "senior role",
            "keywords": "",
            "search_term": "intern",
            "is_intern_like": False,
            "is_canada_like": False,
            "is_senior_like": False,
            "rule_score": 0.0,
            "keyword_score": 0.0,
            "semantic_score": 0.0,
            "freshness_score": 0.0,
            "company_score": 0.0,
            "final_score": 0.0,
            "tier": "",
            "recommended_resume": "",
        },
    ]
    pd.DataFrame(rows).to_csv(tmp_path / "data" / "processed" / "jobs_master.csv", index=False)

    score_jobs(base_dir=tmp_path, run_date=date(2026, 3, 7))
    result = pd.read_csv(tmp_path / "data" / "processed" / "jobs_master.csv")

    first = result.loc[result["job_id"] == 1].iloc[0]
    second = result.loc[result["job_id"] == 2].iloc[0]

    assert abs(float(first["final_score"]) - 1.0) < 1e-6
    assert abs(float(second["final_score"]) - 0.0625) < 1e-6
    assert first["tier"] == "A"
    assert second["tier"] == "C"


def test_score_nlp_intent_track_and_resume_recommendation(tmp_path):
    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "profile").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "processed").mkdir(parents=True, exist_ok=True)
    (tmp_path / "logs").mkdir(parents=True, exist_ok=True)

    _write_yaml(
        tmp_path / "config" / "scoring_config.yaml",
        {
            "weights": {"rule": 0.0, "nlp": 1.0, "freshness": 0.0, "company": 0.0},
            "nlp": {"lexical_weight": 0.0, "semantic_weight": 0.0, "intent_weight": 1.0},
            "tiers": {"a_min": 0.8, "b_min": 0.4},
            "semantic": {"enabled": False, "model_name": "sentence-transformers/all-MiniLM-L6-v2"},
        },
    )
    _write_yaml(
        tmp_path / "data" / "profile" / "search_config.yaml",
        {
            "locations": ["Toronto"],
            "job_types": ["intern"],
            "keywords": ["perception"],
            "exclude": ["senior"],
            "search_tracks": [
                {
                    "key": "perception_cv_av",
                    "resume_key": "pc_2",
                    "target_share": 1.0,
                    "role_terms": ["perception engineer", "computer vision engineer"],
                    "skill_terms": ["lidar", "sensor fusion"],
                    "keywords": ["autonomous driving"],
                }
            ],
        },
    )
    _write_yaml(
        tmp_path / "data" / "profile" / "skills_master.yaml",
        {"languages": ["python"], "ml": ["pytorch"], "robotics": ["ros"], "software": ["linux"]},
    )
    _write_yaml(tmp_path / "data" / "profile" / "target_companies.yaml", {"tier_a": []})
    (tmp_path / "data" / "profile" / "user_profile.md").write_text("profile", encoding="utf-8")

    row = {
        "job_id": "1",
        "title": "Perception Engineer Intern",
        "company": "Acme",
        "location": "Toronto, Canada",
        "job_url": "https://example.com/a",
        "apply_url": "https://example.com/a",
        "source": "test",
        "date_posted": "2026-03-07",
        "date_found": "2026-03-07",
        "description": "computer vision, lidar, and autonomous driving perception stack",
        "keywords": "",
        "search_term": "intern",
        "is_intern_like": False,
        "is_canada_like": False,
        "is_senior_like": False,
        "rule_score": 0.0,
        "keyword_score": 0.0,
        "semantic_score": 0.0,
        "intent_score": 0.0,
        "nlp_score": 0.0,
        "freshness_score": 0.0,
        "company_score": 0.0,
        "final_score": 0.0,
        "tier": "",
        "matched_track": "",
        "recommended_resume": "",
    }
    pd.DataFrame([row]).to_csv(tmp_path / "data" / "processed" / "jobs_master.csv", index=False)

    score_jobs(base_dir=tmp_path, run_date=date(2026, 3, 7))
    result = pd.read_csv(tmp_path / "data" / "processed" / "jobs_master.csv")
    scored = result.iloc[0]

    assert float(scored["intent_score"]) > 0.5
    assert abs(float(scored["nlp_score"]) - float(scored["intent_score"])) < 1e-9
    assert scored["matched_track"] == "perception_cv_av"
    assert scored["recommended_resume"] == "pc_2"


def test_score_official_company_source_gets_extra_weight(tmp_path):
    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "profile").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "processed").mkdir(parents=True, exist_ok=True)
    (tmp_path / "logs").mkdir(parents=True, exist_ok=True)

    _write_yaml(
        tmp_path / "config" / "scoring_config.yaml",
        {
            "weights": {"rule": 0.0, "nlp": 0.0, "freshness": 0.0, "company": 0.5, "source": 0.5},
            "tiers": {"a_min": 0.8, "b_min": 0.4},
            "semantic": {"enabled": False, "model_name": "sentence-transformers/all-MiniLM-L6-v2"},
            "source": {
                "unknown_score": 0.2,
                "default_score": 0.4,
                "prefix_scores": {"company_careers": 1.0},
                "exact_scores": {"linkedin": 0.4},
            },
        },
    )
    _write_yaml(
        tmp_path / "data" / "profile" / "search_config.yaml",
        {"locations": ["Toronto"], "job_types": ["intern"], "keywords": ["python"], "exclude": ["senior"]},
    )
    _write_yaml(tmp_path / "data" / "profile" / "skills_master.yaml", {"languages": ["python"]})
    _write_yaml(tmp_path / "data" / "profile" / "target_companies.yaml", {"tier_a": ["Acme"]})
    (tmp_path / "data" / "profile" / "user_profile.md").write_text("profile", encoding="utf-8")

    rows = [
        {
            "job_id": "1",
            "title": "Software Intern",
            "company": "Acme",
            "location": "Toronto, Canada",
            "job_url": "https://example.com/a",
            "apply_url": "https://example.com/a",
            "source": "company_careers:Acme",
            "date_posted": "2026-03-07",
            "date_found": "2026-03-07",
            "description": "python intern role",
            "keywords": "",
            "search_term": "intern",
        },
        {
            "job_id": "2",
            "title": "Software Intern",
            "company": "Acme",
            "location": "Toronto, Canada",
            "job_url": "https://example.com/b",
            "apply_url": "https://example.com/b",
            "source": "linkedin",
            "date_posted": "2026-03-07",
            "date_found": "2026-03-07",
            "description": "python intern role",
            "keywords": "",
            "search_term": "intern",
        },
    ]
    pd.DataFrame(rows).to_csv(tmp_path / "data" / "processed" / "jobs_master.csv", index=False)

    score_jobs(base_dir=tmp_path, run_date=date(2026, 3, 7))
    result = pd.read_csv(tmp_path / "data" / "processed" / "jobs_master.csv").sort_values("job_id")

    official = result.loc[result["job_id"] == 1].iloc[0]
    external = result.loc[result["job_id"] == 2].iloc[0]

    assert abs(float(official["source_score"]) - 1.0) < 1e-9
    assert abs(float(external["source_score"]) - 0.4) < 1e-9
    assert float(official["final_score"]) > float(external["final_score"])
