from __future__ import annotations

import argparse
import traceback
from datetime import date, datetime
from pathlib import Path
from time import perf_counter
from typing import Any, Callable

try:
    from scripts.dedupe_jobs import dedupe_jobs
    from scripts.export_daily_reports import export_daily_reports
    from scripts.fetch_jobspy import fetch_jobspy_jobs
    from scripts.ingest_manual_links import ingest_manual_links
    from scripts.normalize_jobs import normalize_jobs
    from scripts.pipeline_utils import ensure_required_dirs, get_project_root, init_logger
    from scripts.score_jobs import score_jobs
except ModuleNotFoundError:
    from dedupe_jobs import dedupe_jobs  # type: ignore
    from export_daily_reports import export_daily_reports  # type: ignore
    from fetch_jobspy import fetch_jobspy_jobs  # type: ignore
    from ingest_manual_links import ingest_manual_links  # type: ignore
    from normalize_jobs import normalize_jobs  # type: ignore
    from pipeline_utils import ensure_required_dirs, get_project_root, init_logger  # type: ignore
    from score_jobs import score_jobs  # type: ignore


def run_pipeline(base_dir: str | Path | None = None, run_date: date | None = None) -> dict[str, Any]:
    root = get_project_root(base_dir)
    ensure_required_dirs(root)
    run_date = run_date or datetime.now().date()
    stamp = f"{run_date:%Y%m%d}"
    logger = init_logger("job_hunter.run_daily", root / "logs" / f"run_{stamp}.log")

    logger.info("Starting daily pipeline for %s", run_date.isoformat())
    stage_results: dict[str, dict[str, Any]] = {}
    stage_timings: dict[str, float] = {}

    stages: list[tuple[str, Callable[..., dict[str, Any]]]] = [
        ("ingest_manual_links", ingest_manual_links),
        ("fetch_jobspy", fetch_jobspy_jobs),
        ("normalize_jobs", normalize_jobs),
        ("dedupe_jobs", dedupe_jobs),
        ("score_jobs", score_jobs),
        ("export_daily_reports", export_daily_reports),
    ]

    for stage_name, stage_fn in stages:
        started = perf_counter()
        try:
            result = stage_fn(base_dir=root, run_date=run_date, logger=logger)
            elapsed = perf_counter() - started
            stage_results[stage_name] = result
            stage_timings[stage_name] = elapsed
            logger.info(
                "Stage '%s' completed in %.2fs (count=%s, path=%s)",
                stage_name,
                elapsed,
                result.get("count", "n/a"),
                result.get("path", "n/a"),
            )
        except Exception as exc:
            logger.error("Stage '%s' failed: %s", stage_name, exc)
            logger.error(traceback.format_exc())
            raise

    total_time = sum(stage_timings.values())
    logger.info("Pipeline completed in %.2fs", total_time)
    logger.info(
        "Summary counts: manual_links=%s, fetched=%s, normalized=%s, deduped=%s, scored=%s, exported=%s",
        stage_results.get("ingest_manual_links", {}).get("count", 0),
        stage_results.get("fetch_jobspy", {}).get("count", 0),
        stage_results.get("normalize_jobs", {}).get("count", 0),
        stage_results.get("dedupe_jobs", {}).get("count", 0),
        stage_results.get("score_jobs", {}).get("count", 0),
        stage_results.get("export_daily_reports", {}).get("count", 0),
    )
    return {
        "date": run_date.isoformat(),
        "total_time_seconds": total_time,
        "stages": stage_results,
        "timings": stage_timings,
        "log_path": str(root / "logs" / f"run_{stamp}.log"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the full daily job-hunter pipeline.")
    parser.add_argument("--base-dir", type=str, default=None, help="Project root directory.")
    args = parser.parse_args()
    run_pipeline(base_dir=args.base_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
