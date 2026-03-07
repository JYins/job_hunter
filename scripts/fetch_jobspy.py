from __future__ import annotations

import argparse
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from scripts.pipeline_utils import (
        ensure_required_dirs,
        get_project_root,
        init_logger,
        load_yaml,
        write_jsonl,
    )
except ModuleNotFoundError:
    from pipeline_utils import (  # type: ignore
        ensure_required_dirs,
        get_project_root,
        init_logger,
        load_yaml,
        write_jsonl,
    )


def _safe_scrape(
    site_names: list[str],
    search_term: str,
    location: str,
    results_wanted: int,
    hours_old: int,
    country_indeed: str,
):
    from jobspy import scrape_jobs

    kwargs = {
        "site_name": site_names,
        "search_term": search_term,
        "location": location,
        "results_wanted": results_wanted,
        "hours_old": hours_old,
        "country_indeed": country_indeed,
    }

    try:
        return scrape_jobs(**kwargs)
    except TypeError:
        kwargs.pop("country_indeed", None)
        return scrape_jobs(**kwargs)


def fetch_jobspy_jobs(base_dir: str | Path | None = None, run_date: date | None = None, logger=None) -> dict[str, Any]:
    root = get_project_root(base_dir)
    ensure_required_dirs(root)
    run_date = run_date or datetime.now().date()

    if logger is None:
        log_path = root / "logs" / f"run_{run_date:%Y%m%d}.log"
        logger = init_logger("job_hunter.fetch_jobspy", log_path)

    sources_cfg = load_yaml(root / "config" / "sources.yaml")
    jobspy_cfg = sources_cfg.get("jobspy", {})
    enabled = bool(jobspy_cfg.get("enabled", True))
    output_path = root / "data" / "raw" / "jobspy" / f"jobspy_{run_date:%Y%m%d}.jsonl"

    if not enabled:
        logger.info("JobSpy fetch is disabled in config/sources.yaml.")
        write_jsonl(output_path, [])
        return {"count": 0, "path": str(output_path)}

    try:
        import jobspy  # noqa: F401
    except ImportError:
        logger.warning("python-jobspy is not installed. Skipping JobSpy fetch.")
        write_jsonl(output_path, [])
        return {"count": 0, "path": str(output_path)}

    site_names = [str(item).strip() for item in jobspy_cfg.get("sites", ["indeed"]) if str(item).strip()]
    search_terms = [str(item).strip() for item in jobspy_cfg.get("search_terms", []) if str(item).strip()]
    locations = [str(item).strip() for item in jobspy_cfg.get("locations", ["Canada"]) if str(item).strip()]
    days_back = int(jobspy_cfg.get("days_back", 7))
    results_wanted = int(jobspy_cfg.get("results_wanted", 100))
    country_indeed = str(jobspy_cfg.get("country_indeed", "canada")).lower()
    hours_old = max(24, days_back * 24)

    if not search_terms:
        logger.warning("No JobSpy search_terms configured. Skipping fetch.")
        write_jsonl(output_path, [])
        return {"count": 0, "path": str(output_path)}

    frames: list[pd.DataFrame] = []
    for term in search_terms:
        for location in locations:
            logger.info("Fetching JobSpy jobs for term='%s' location='%s'.", term, location)
            try:
                df = _safe_scrape(
                    site_names=site_names,
                    search_term=term,
                    location=location,
                    results_wanted=results_wanted,
                    hours_old=hours_old,
                    country_indeed=country_indeed,
                )
            except Exception as exc:
                logger.warning("JobSpy fetch failed for term='%s' location='%s': %s", term, location, exc)
                continue

            if not isinstance(df, pd.DataFrame) or df.empty:
                continue

            df = df.copy()
            df["search_term"] = term
            df["query_location"] = location
            frames.append(df)

    if not frames:
        logger.info("No JobSpy records fetched.")
        write_jsonl(output_path, [])
        return {"count": 0, "path": str(output_path)}

    merged = pd.concat(frames, ignore_index=True)
    records: list[dict[str, Any]] = []
    date_found = run_date.isoformat()
    for record in merged.to_dict(orient="records"):
        item = dict(record)
        item["source"] = str(item.get("site") or item.get("source") or "jobspy")
        item["date_found"] = date_found
        records.append(item)

    write_jsonl(output_path, records)
    logger.info("Fetched %s JobSpy records -> %s", len(records), output_path)
    return {"count": len(records), "path": str(output_path)}


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch jobs from JobSpy and save raw JSONL.")
    parser.add_argument("--base-dir", type=str, default=None, help="Project root directory.")
    args = parser.parse_args()
    fetch_jobspy_jobs(base_dir=args.base_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
