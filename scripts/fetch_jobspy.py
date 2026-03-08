from __future__ import annotations

import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    site_name: str,
    search_term: str,
    location: str,
    results_wanted: int,
    hours_old: int,
    country_indeed: str,
):
    from jobspy import scrape_jobs

    kwargs = {
        "site_name": site_name,
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


def _parse_site_settings(jobspy_cfg: dict[str, Any]) -> list[dict[str, Any]]:
    settings = jobspy_cfg.get("site_settings", [])
    parsed: list[dict[str, Any]] = []

    if isinstance(settings, list):
        for index, entry in enumerate(settings):
            if isinstance(entry, str):
                name = entry.strip().lower()
                if name:
                    parsed.append({"name": name, "enabled": True, "priority": index + 1})
                continue
            if not isinstance(entry, dict):
                continue
            name = str(entry.get("name", "")).strip().lower()
            if not name:
                continue
            enabled = bool(entry.get("enabled", True))
            priority = int(entry.get("priority", index + 1))
            parsed.append({"name": name, "enabled": enabled, "priority": priority})

    if parsed:
        parsed.sort(key=lambda item: (item["priority"], item["name"]))
        return parsed

    fallback_sites = [str(item).strip().lower() for item in jobspy_cfg.get("sites", ["indeed"]) if str(item).strip()]
    return [
        {"name": site, "enabled": True, "priority": index + 1}
        for index, site in enumerate(fallback_sites)
    ]


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

    site_settings = _parse_site_settings(jobspy_cfg)
    enabled_sites = [item for item in site_settings if item["enabled"]]
    search_terms = [str(item).strip() for item in jobspy_cfg.get("search_terms", []) if str(item).strip()]
    locations = [str(item).strip() for item in jobspy_cfg.get("locations", ["Canada"]) if str(item).strip()]
    days_back = int(jobspy_cfg.get("days_back", 7))
    results_wanted = int(jobspy_cfg.get("results_wanted", 100))
    country_indeed = str(jobspy_cfg.get("country_indeed", "canada")).lower()
    hours_old = max(24, days_back * 24)
    parallel_site_workers = max(1, int(jobspy_cfg.get("parallel_site_workers", 3)))
    stop_after_consecutive_empty = max(1, int(jobspy_cfg.get("stop_after_consecutive_empty", 3)))
    stop_after_consecutive_error = max(1, int(jobspy_cfg.get("stop_after_consecutive_error", 2)))
    min_attempts_before_pause = max(1, int(jobspy_cfg.get("min_attempts_before_pause", 10)))

    if not enabled_sites:
        logger.warning("No enabled JobSpy sites configured. Skipping fetch.")
        write_jsonl(output_path, [])
        return {
            "count": 0,
            "path": str(output_path),
            "source_summary": {},
            "failure_summary": {},
            "fetch_attempts": [],
        }

    if not search_terms:
        logger.warning("No JobSpy search_terms configured. Skipping fetch.")
        write_jsonl(output_path, [])
        return {
            "count": 0,
            "path": str(output_path),
            "source_summary": {},
            "failure_summary": {},
            "fetch_attempts": [],
        }

    frames: list[pd.DataFrame] = []
    source_summary: defaultdict[str, int] = defaultdict(int)
    failure_summary: defaultdict[str, int] = defaultdict(int)
    fetch_attempts: list[dict[str, Any]] = []
    site_state: dict[str, dict[str, int | bool]] = {
        site["name"]: {
            "consecutive_empty": 0,
            "consecutive_error": 0,
            "attempts": 0,
            "has_success": False,
            "paused": False,
        }
        for site in enabled_sites
    }

    for term in search_terms:
        for location in locations:
            active_sites = [site for site in enabled_sites if not bool(site_state[site["name"]]["paused"])]
            if not active_sites:
                logger.warning("All sites paused early due to repeated empty/error responses.")
                break

            futures: dict[Any, str] = {}
            with ThreadPoolExecutor(max_workers=min(parallel_site_workers, len(active_sites))) as executor:
                for site in active_sites:
                    site_name = site["name"]
                    logger.info("Fetching JobSpy jobs for site='%s' term='%s' location='%s'.", site_name, term, location)
                    future = executor.submit(
                        _safe_scrape,
                        site_name=site_name,
                        search_term=term,
                        location=location,
                        results_wanted=results_wanted,
                        hours_old=hours_old,
                        country_indeed=country_indeed,
                    )
                    futures[future] = site_name

                for future in as_completed(futures):
                    site_name = futures[future]
                    state = site_state[site_name]
                    state["attempts"] = int(state["attempts"]) + 1
                    try:
                        df = future.result()
                    except Exception as exc:
                        failure_summary[site_name] += 1
                        state["consecutive_error"] = int(state["consecutive_error"]) + 1
                        fetch_attempts.append(
                            {
                                "site": site_name,
                                "search_term": term,
                                "location": location,
                                "count": 0,
                                "error": str(exc),
                            }
                        )
                        logger.warning(
                            "JobSpy fetch failed for site='%s' term='%s' location='%s': %s",
                            site_name,
                            term,
                            location,
                            exc,
                        )
                        if (
                            int(state["attempts"]) >= min_attempts_before_pause
                            and
                            int(state["consecutive_error"]) >= stop_after_consecutive_error
                            and not bool(state["has_success"])
                        ):
                            state["paused"] = True
                            logger.warning(
                                "Pausing site='%s' for this run after %s consecutive errors with no successful results.",
                                site_name,
                                state["consecutive_error"],
                            )
                        continue

                    count = 0
                    if isinstance(df, pd.DataFrame) and not df.empty:
                        df = df.copy()
                        df["search_term"] = term
                        df["query_location"] = location
                        if "site" not in df.columns:
                            df["site"] = site_name
                        else:
                            df["site"] = df["site"].fillna(site_name).astype(str).replace({"": site_name, "nan": site_name})
                        df["source"] = df["site"]
                        frames.append(df)
                        count = len(df)
                        source_summary[site_name] += count
                        state["has_success"] = True
                        state["consecutive_empty"] = 0
                        state["consecutive_error"] = 0
                    else:
                        state["consecutive_empty"] = int(state["consecutive_empty"]) + 1
                        if (
                            int(state["attempts"]) >= min_attempts_before_pause
                            and
                            int(state["consecutive_empty"]) >= stop_after_consecutive_empty
                            and not bool(state["has_success"])
                        ):
                            state["paused"] = True
                            logger.info(
                                "Pausing site='%s' for this run after %s consecutive empty responses.",
                                site_name,
                                state["consecutive_empty"],
                            )

                    fetch_attempts.append(
                        {
                            "site": site_name,
                            "search_term": term,
                            "location": location,
                            "count": count,
                            "error": "",
                        }
                    )

    if not frames:
        logger.info("No JobSpy records fetched.")
        write_jsonl(output_path, [])
        logger.info("Fetch source summary: %s", dict(source_summary))
        logger.info("Fetch failure summary: %s", dict(failure_summary))
        return {
            "count": 0,
            "path": str(output_path),
            "source_summary": dict(source_summary),
            "failure_summary": dict(failure_summary),
            "fetch_attempts": fetch_attempts,
        }

    merged = pd.concat(frames, ignore_index=True)
    records: list[dict[str, Any]] = []
    date_found = run_date.isoformat()
    for record in merged.to_dict(orient="records"):
        item = dict(record)
        source_value = "jobspy"
        for candidate in [item.get("site"), item.get("source")]:
            if pd.isna(candidate):
                continue
            text = str(candidate).strip()
            if text and text.lower() != "nan":
                source_value = text
                break
        item["source"] = source_value
        item["date_found"] = date_found
        records.append(item)

    write_jsonl(output_path, records)
    logger.info("Fetched %s JobSpy records -> %s", len(records), output_path)
    logger.info("Fetch source summary: %s", dict(source_summary))
    logger.info("Fetch failure summary: %s", dict(failure_summary))
    return {
        "count": len(records),
        "path": str(output_path),
        "source_summary": dict(source_summary),
        "failure_summary": dict(failure_summary),
        "fetch_attempts": fetch_attempts,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch jobs from JobSpy and save raw JSONL.")
    parser.add_argument("--base-dir", type=str, default=None, help="Project root directory.")
    args = parser.parse_args()
    fetch_jobspy_jobs(base_dir=args.base_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
