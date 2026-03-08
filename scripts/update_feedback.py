from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from scripts.pipeline_utils import CANONICAL_COLUMNS, get_project_root, init_logger, normalize_space, normalize_url
except ModuleNotFoundError:
    from pipeline_utils import CANONICAL_COLUMNS, get_project_root, init_logger, normalize_space, normalize_url  # type: ignore


VALID_STATUSES = {"applied", "replied", "interview", "rejected"}
STATUS_TIME_COLUMN = {
    "applied": "applied_at",
    "replied": "replied_at",
    "interview": "interview_at",
    "rejected": "rejected_at",
}


def _normalize_values(values: list[str] | None, *, is_url: bool = False) -> set[str]:
    if not values:
        return set()
    result: set[str] = set()
    for item in values:
        text = normalize_url(item) if is_url else normalize_space(item)
        text = text.strip()
        if text:
            result.add(text)
    return result


def update_feedback(
    status: str,
    job_ids: list[str] | None = None,
    job_urls: list[str] | None = None,
    contains_title: str | None = None,
    notes: str | None = None,
    base_dir: str | Path | None = None,
) -> dict[str, Any]:
    if status not in VALID_STATUSES:
        raise ValueError(f"Unsupported status '{status}'. Expected one of {sorted(VALID_STATUSES)}")

    root = get_project_root(base_dir)
    log_path = root / "logs" / f"run_{datetime.now():%Y%m%d}.log"
    logger = init_logger("job_hunter.update_feedback", log_path)

    master_path = root / "data" / "processed" / "jobs_master.csv"
    if not master_path.exists():
        logger.info("No jobs_master.csv found at %s.", master_path)
        return {"count": 0, "path": str(master_path)}

    df = pd.read_csv(master_path)
    if df.empty:
        logger.info("jobs_master.csv is empty.")
        return {"count": 0, "path": str(master_path)}

    for column in CANONICAL_COLUMNS:
        if column not in df.columns:
            df[column] = ""
    for column in [
        "feedback_status",
        "feedback_updated_at",
        "applied_at",
        "replied_at",
        "interview_at",
        "rejected_at",
        "feedback_notes",
    ]:
        df[column] = df[column].fillna("").astype(str)

    id_set = _normalize_values(job_ids, is_url=False)
    url_set = _normalize_values(job_urls, is_url=True)
    title_query = normalize_space(contains_title or "").lower()

    if not id_set and not url_set and not title_query:
        raise ValueError("Provide at least one selector: --job-id, --job-url, or --contains-title.")

    mask = pd.Series(False, index=df.index)
    if id_set:
        mask = mask | df["job_id"].fillna("").astype(str).apply(normalize_space).isin(id_set)
    if url_set:
        mask = mask | df["job_url"].fillna("").astype(str).apply(normalize_url).isin(url_set)
    if title_query:
        mask = mask | df["title"].fillna("").astype(str).str.lower().str.contains(title_query, regex=False)

    matched = int(mask.sum())
    if matched == 0:
        logger.info("No rows matched feedback selector.")
        return {"count": 0, "path": str(master_path)}

    now_iso = datetime.now().isoformat(timespec="seconds")
    df.loc[mask, "feedback_status"] = status
    df.loc[mask, "feedback_updated_at"] = now_iso
    time_col = STATUS_TIME_COLUMN[status]
    empty_time_mask = mask & (df[time_col].fillna("").astype(str).str.strip() == "")
    df.loc[empty_time_mask, time_col] = now_iso
    if notes:
        df.loc[mask, "feedback_notes"] = str(notes).strip()

    df = df[CANONICAL_COLUMNS]
    df.to_csv(master_path, index=False)
    logger.info("Updated feedback to '%s' for %s jobs -> %s", status, matched, master_path)
    return {"count": matched, "path": str(master_path), "status": status}


def main() -> int:
    parser = argparse.ArgumentParser(description="Update application feedback labels in jobs_master.csv.")
    parser.add_argument("--status", required=True, choices=sorted(VALID_STATUSES))
    parser.add_argument("--job-id", action="append", default=None, help="Target job_id (repeatable).")
    parser.add_argument("--job-url", action="append", default=None, help="Target job_url (repeatable).")
    parser.add_argument("--contains-title", type=str, default=None, help="Match rows where title contains this text.")
    parser.add_argument("--notes", type=str, default=None, help="Optional feedback notes.")
    parser.add_argument("--base-dir", type=str, default=None, help="Project root directory.")
    args = parser.parse_args()
    update_feedback(
        status=args.status,
        job_ids=args.job_id,
        job_urls=args.job_url,
        contains_title=args.contains_title,
        notes=args.notes,
        base_dir=args.base_dir,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
