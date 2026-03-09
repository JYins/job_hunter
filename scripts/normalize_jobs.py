from __future__ import annotations

import argparse
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from scripts.pipeline_utils import (
        CANONICAL_COLUMNS,
        compute_job_id,
        ensure_required_dirs,
        get_project_root,
        init_logger,
        load_yaml,
        normalize_space,
        normalize_url,
        read_jsonl,
    )
except ModuleNotFoundError:
    from pipeline_utils import (  # type: ignore
        CANONICAL_COLUMNS,
        compute_job_id,
        ensure_required_dirs,
        get_project_root,
        init_logger,
        load_yaml,
        normalize_space,
        normalize_url,
        read_jsonl,
    )


def _first_value(record: dict[str, Any], keys: list[str]) -> str:
    for key in keys:
        value = record.get(key)
        if value is None:
            continue
        text = normalize_space(str(value))
        if text:
            return text
    return ""


def _infer_keywords(title: str, description: str, search_keywords: list[str]) -> str:
    text = f"{title} {description}".lower()
    found = [kw for kw in search_keywords if kw.lower() in text]
    return ", ".join(sorted(set(found)))


def _collect_input_files(root: Path, run_date: date) -> list[Path]:
    stamp = f"{run_date:%Y%m%d}"
    jobspy_path = root / "data" / "raw" / "jobspy" / f"jobspy_{stamp}.jsonl"
    alert_path = root / "data" / "raw" / "alerts" / f"alerts_manual_{stamp}.jsonl"
    external_path = root / "data" / "raw" / "alerts" / f"external_sources_{stamp}.jsonl"
    files = [path for path in [jobspy_path, alert_path, external_path] if path.exists()]
    return files


def normalize_jobs(base_dir: str | Path | None = None, run_date: date | None = None, logger=None) -> dict[str, Any]:
    root = get_project_root(base_dir)
    ensure_required_dirs(root)
    run_date = run_date or datetime.now().date()
    stamp = f"{run_date:%Y%m%d}"

    if logger is None:
        logger = init_logger("job_hunter.normalize_jobs", root / "logs" / f"run_{stamp}.log")

    search_cfg = load_yaml(root / "data" / "profile" / "search_config.yaml")
    search_keywords = [str(item).strip() for item in search_cfg.get("keywords", []) if str(item).strip()]
    input_files = _collect_input_files(root, run_date)
    if not input_files:
        logger.info("No raw input files found for %s.", stamp)

    records: list[dict[str, Any]] = []
    for input_file in input_files:
        raw_records = read_jsonl(input_file)
        logger.info("Normalizing %s records from %s", len(raw_records), input_file)
        records.extend(raw_records)

    normalized_rows: list[dict[str, Any]] = []
    for raw in records:
        title = _first_value(raw, ["title", "job_title", "position"])
        company = _first_value(raw, ["company", "company_name", "employer"])
        location = _first_value(raw, ["location", "city"])
        job_url = normalize_url(_first_value(raw, ["job_url", "url", "job_link"]))
        apply_url = normalize_url(_first_value(raw, ["apply_url", "application_url", "url", "job_link", "job_url"]))
        source = _first_value(raw, ["source", "site"]) or "unknown"
        date_posted = _first_value(raw, ["date_posted", "posted_date", "date"])
        date_found = _first_value(raw, ["date_found"]) or run_date.isoformat()
        description = _first_value(raw, ["description", "job_description", "summary"])
        search_term = _first_value(raw, ["search_term"]) or "unknown"
        keywords = _first_value(raw, ["keywords"]) or _infer_keywords(title, description, search_keywords)

        normalized_rows.append(
            {
                "job_id": compute_job_id(job_url, company, title, location),
                "title": title,
                "company": company,
                "location": location,
                "job_url": job_url,
                "apply_url": apply_url,
                "source": source,
                "date_posted": date_posted,
                "date_found": date_found,
                "description": description,
                "keywords": keywords,
                "search_term": search_term,
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
                "feedback_status": "new",
                "feedback_updated_at": "",
                "applied_at": "",
                "replied_at": "",
                "interview_at": "",
                "rejected_at": "",
                "feedback_notes": "",
            }
        )

    master_path = root / "data" / "processed" / "jobs_master.csv"
    new_df = pd.DataFrame(normalized_rows)
    if master_path.exists():
        existing = pd.read_csv(master_path)
        merged = pd.concat([existing, new_df], ignore_index=True)
    else:
        merged = new_df

    for column in CANONICAL_COLUMNS:
        if column not in merged.columns:
            merged[column] = ""
    merged = merged[CANONICAL_COLUMNS]

    for text_col in ["title", "company", "location", "source", "job_url", "apply_url", "search_term", "keywords"]:
        merged[text_col] = merged[text_col].fillna("").astype(str).apply(normalize_space)
    merged["description"] = merged["description"].fillna("").astype(str)
    merged["date_posted"] = merged["date_posted"].fillna("").astype(str)
    merged["date_found"] = merged["date_found"].fillna(run_date.isoformat()).astype(str)
    merged["job_id"] = merged.apply(
        lambda row: compute_job_id(
            str(row.get("job_url", "")),
            str(row.get("company", "")),
            str(row.get("title", "")),
            str(row.get("location", "")),
        ),
        axis=1,
    )

    merged.to_csv(master_path, index=False)
    logger.info("Normalized dataset now has %s rows -> %s", len(merged), master_path)
    return {"count": int(len(merged)), "path": str(master_path)}


def main() -> int:
    parser = argparse.ArgumentParser(description="Normalize raw jobs into canonical jobs_master.csv.")
    parser.add_argument("--base-dir", type=str, default=None, help="Project root directory.")
    args = parser.parse_args()
    normalize_jobs(base_dir=args.base_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
