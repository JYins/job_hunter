import pandas as pd

from scripts.dedupe_jobs import dedupe_jobs_df
from scripts.pipeline_utils import CANONICAL_COLUMNS


def _base_row(**overrides):
    row = {
        "job_id": "",
        "title": "",
        "company": "",
        "location": "",
        "job_url": "",
        "apply_url": "",
        "source": "test",
        "date_posted": "",
        "date_found": "2026-03-07",
        "description": "",
        "keywords": "",
        "search_term": "test",
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
    }
    row.update(overrides)
    return row


def test_dedupe_url_and_composite():
    rows = [
        _base_row(
            title="Software Engineer Intern",
            company="Acme",
            location="Toronto, ON",
            job_url="https://example.com/jobs/1",
        ),
        _base_row(
            title="Software Engineer Intern",
            company="Acme",
            location="Toronto, ON",
            job_url="https://example.com/jobs/1",  # exact URL duplicate
        ),
        _base_row(
            title="Software Engineer Internship",
            company="Acme",
            location="Toronto, ON",
            job_url="",  # composite duplicate
        ),
        _base_row(
            title="ML Intern",
            company="Acme",
            location="Toronto, ON",
            job_url="https://example.com/jobs/2",
        ),
    ]
    df = pd.DataFrame(rows, columns=CANONICAL_COLUMNS)
    deduped = dedupe_jobs_df(df, similarity_threshold=0.9)
    assert len(deduped) == 2
