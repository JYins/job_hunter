from __future__ import annotations

import argparse
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from scripts.pipeline_utils import ensure_required_dirs, get_project_root, init_logger
except ModuleNotFoundError:
    from pipeline_utils import ensure_required_dirs, get_project_root, init_logger  # type: ignore


def export_daily_reports(base_dir: str | Path | None = None, run_date: date | None = None, logger=None) -> dict[str, Any]:
    root = get_project_root(base_dir)
    ensure_required_dirs(root)
    run_date = run_date or datetime.now().date()

    if logger is None:
        logger = init_logger("job_hunter.export_reports", root / "logs" / f"run_{run_date:%Y%m%d}.log")

    master_path = root / "data" / "processed" / "jobs_master.csv"
    output_path = root / "data" / "processed" / "today_top_jobs.xlsx"
    if not master_path.exists():
        logger.info("No jobs_master.csv found at %s. Nothing to export.", master_path)
        return {"count": 0, "path": str(output_path)}

    df = pd.read_csv(master_path)
    if df.empty:
        logger.info("jobs_master.csv is empty. Exporting empty report.")

    if "final_score" not in df.columns:
        df["final_score"] = 0.0
    if "source" not in df.columns:
        df["source"] = "unknown"
    if "tier" not in df.columns:
        df["tier"] = "C"
    if "feedback_status" not in df.columns:
        df["feedback_status"] = "new"

    df["final_score"] = pd.to_numeric(df["final_score"], errors="coerce").fillna(0.0)
    sorted_df = df.sort_values(by=["final_score", "date_found"], ascending=[False, False]).reset_index(drop=True)
    top20 = sorted_df.head(20).copy()

    source_summary = (
        sorted_df.groupby("source", dropna=False)
        .agg(job_count=("job_id", "count"), avg_score=("final_score", "mean"))
        .reset_index()
        .sort_values("job_count", ascending=False)
    )
    source_summary["avg_score"] = source_summary["avg_score"].round(4)
    total_jobs = max(int(source_summary["job_count"].sum()), 1)
    source_summary["contribution_pct"] = ((source_summary["job_count"] / total_jobs) * 100.0).round(2)

    tier_summary = (
        sorted_df.groupby("tier", dropna=False).agg(job_count=("job_id", "count")).reset_index().sort_values("tier")
    )
    feedback_summary = (
        sorted_df.groupby("feedback_status", dropna=False)
        .agg(job_count=("job_id", "count"), avg_score=("final_score", "mean"))
        .reset_index()
        .sort_values("job_count", ascending=False)
    )
    feedback_summary["avg_score"] = feedback_summary["avg_score"].round(4)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        top20.to_excel(writer, sheet_name="top_20", index=False)
        sorted_df.to_excel(writer, sheet_name="all_scored", index=False)
        source_summary.to_excel(writer, sheet_name="source_summary", index=False)
        tier_summary.to_excel(writer, sheet_name="tier_summary", index=False)
        feedback_summary.to_excel(writer, sheet_name="feedback_summary", index=False)

    logger.info("Exported report with %s rows -> %s", len(sorted_df), output_path)
    return {"count": int(len(sorted_df)), "path": str(output_path)}


def main() -> int:
    parser = argparse.ArgumentParser(description="Export daily Excel reports from jobs_master.csv.")
    parser.add_argument("--base-dir", type=str, default=None, help="Project root directory.")
    args = parser.parse_args()
    export_daily_reports(base_dir=args.base_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
