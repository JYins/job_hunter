from __future__ import annotations

import argparse
import json
import traceback
from datetime import date, datetime
from pathlib import Path
from time import perf_counter
from typing import Any, Callable

try:
    from scripts.dedupe_jobs import dedupe_jobs
    from scripts.export_daily_reports import export_daily_reports
    from scripts.fetch_external_sources import fetch_external_sources
    from scripts.fetch_jobspy import fetch_jobspy_jobs
    from scripts.fetch_uwo_connect import fetch_uwo_connect
    from scripts.ingest_manual_links import ingest_manual_links
    from scripts.normalize_jobs import normalize_jobs
    from scripts.pipeline_utils import ensure_required_dirs, get_project_root, init_logger
    from scripts.score_jobs import score_jobs
except ModuleNotFoundError:
    from dedupe_jobs import dedupe_jobs  # type: ignore
    from export_daily_reports import export_daily_reports  # type: ignore
    from fetch_external_sources import fetch_external_sources  # type: ignore
    from fetch_jobspy import fetch_jobspy_jobs  # type: ignore
    from fetch_uwo_connect import fetch_uwo_connect  # type: ignore
    from ingest_manual_links import ingest_manual_links  # type: ignore
    from normalize_jobs import normalize_jobs  # type: ignore
    from pipeline_utils import ensure_required_dirs, get_project_root, init_logger  # type: ignore
    from score_jobs import score_jobs  # type: ignore


def _summarize_deduped_sources(master_path: Path) -> dict[str, int]:
    if not master_path.exists():
        return {}
    try:
        import pandas as pd
    except ImportError:
        return {}
    df = pd.read_csv(master_path)
    if df.empty or "source" not in df.columns:
        return {}
    summary = (
        df.groupby("source", dropna=False)["job_id"]
        .count()
        .sort_values(ascending=False)
        .to_dict()
    )
    return {str(key): int(value) for key, value in summary.items()}


def _append_run_metrics(root: Path, record: dict[str, Any]) -> None:
    history_path = root / "data" / "outputs" / "pipeline_runs.jsonl"
    history_path.parent.mkdir(parents=True, exist_ok=True)
    with history_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False))
        handle.write("\n")


def _load_recent_metrics(root: Path, n: int = 3) -> list[dict[str, Any]]:
    history_path = root / "data" / "outputs" / "pipeline_runs.jsonl"
    if not history_path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with history_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                rows.append(parsed)
    return rows[-n:]


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
        ("fetch_external_sources", fetch_external_sources),
        ("fetch_uwo_connect", fetch_uwo_connect),
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
    jobspy_fetch_result = stage_results.get("fetch_jobspy", {})
    external_fetch_result = stage_results.get("fetch_external_sources", {})
    uwo_fetch_result = stage_results.get("fetch_uwo_connect", {})
    fetched_source_summary: dict[str, int] = {}
    fetch_failure_summary: dict[str, int] = {}
    for candidate in [jobspy_fetch_result, external_fetch_result, uwo_fetch_result]:
        if not isinstance(candidate, dict):
            continue
        for source, count in candidate.get("source_summary", {}).items():
            fetched_source_summary[str(source)] = int(fetched_source_summary.get(str(source), 0)) + int(count)
        for source, count in candidate.get("failure_summary", {}).items():
            fetch_failure_summary[str(source)] = int(fetch_failure_summary.get(str(source), 0)) + int(count)
    deduped_source_summary = _summarize_deduped_sources(root / "data" / "processed" / "jobs_master.csv")

    metrics_record = {
        "date": run_date.isoformat(),
        "fetched_total": int(stage_results.get("fetch_jobspy", {}).get("count", 0)),
        "external_fetched_total": int(stage_results.get("fetch_external_sources", {}).get("count", 0)),
        "uwo_fetched_total": int(stage_results.get("fetch_uwo_connect", {}).get("count", 0)),
        "deduped_total": int(stage_results.get("dedupe_jobs", {}).get("count", 0)),
        "fetched_source_summary": fetched_source_summary,
        "deduped_source_summary": deduped_source_summary,
        "fetch_failure_summary": fetch_failure_summary,
        "active_sources": int(sum(1 for count in deduped_source_summary.values() if int(count) > 0)),
    }
    _append_run_metrics(root, metrics_record)
    recent_metrics = _load_recent_metrics(root, n=3)
    quality_gate_passed = (
        len(recent_metrics) == 3
        and all(int(item.get("active_sources", 0)) >= 2 for item in recent_metrics)
        and all(int(item.get("deduped_total", 0)) >= 27 for item in recent_metrics)
    )

    logger.info("Pipeline completed in %.2fs", total_time)
    logger.info(
        "Source summary: fetched=%s, deduped=%s, failures=%s",
        fetched_source_summary,
        deduped_source_summary,
        fetch_failure_summary,
    )
    logger.info(
        "3-run acceptance gate (>=2 active sources and >=27 deduped each run): %s",
        "PASS" if quality_gate_passed else "NOT_YET",
    )
    logger.info(
        "Summary counts: manual_links=%s, fetched=%s, normalized=%s, deduped=%s, scored=%s, exported=%s",
        stage_results.get("ingest_manual_links", {}).get("count", 0),
        int(stage_results.get("fetch_jobspy", {}).get("count", 0))
        + int(stage_results.get("fetch_external_sources", {}).get("count", 0))
        + int(stage_results.get("fetch_uwo_connect", {}).get("count", 0)),
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
        "source_summary": {
            "fetched": fetched_source_summary,
            "deduped": deduped_source_summary,
            "failures": fetch_failure_summary,
        },
        "quality_gate": {
            "window": 3,
            "criteria": ">=2 active sources and >=27 deduped rows per run",
            "passed": quality_gate_passed,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the full daily job-hunter pipeline.")
    parser.add_argument("--base-dir", type=str, default=None, help="Project root directory.")
    args = parser.parse_args()
    run_pipeline(base_dir=args.base_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
