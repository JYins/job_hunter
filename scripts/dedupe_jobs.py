from __future__ import annotations

import argparse
from datetime import date, datetime
from difflib import SequenceMatcher
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
        normalize_text_key,
        normalize_url,
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
        normalize_text_key,
        normalize_url,
    )


def _normalize_title(title: str) -> str:
    value = normalize_text_key(title)
    replacements = {
        "internship": "intern",
        "co op": "coop",
        "co-op": "coop",
        "new graduate": "new grad",
    }
    for old, new in replacements.items():
        value = value.replace(old, new)
    return normalize_space(value)


def dedupe_jobs_df(df: pd.DataFrame, similarity_threshold: float = 0.92) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    clean = df.copy()
    clean["job_url_norm"] = clean["job_url"].fillna("").astype(str).apply(normalize_url).str.lower()
    clean["title_norm"] = clean["title"].fillna("").astype(str).apply(_normalize_title)
    clean["company_norm"] = clean["company"].fillna("").astype(str).apply(normalize_text_key)
    clean["location_norm"] = clean["location"].fillna("").astype(str).apply(normalize_text_key)

    with_url = clean[clean["job_url_norm"] != ""]
    without_url = clean[clean["job_url_norm"] == ""]
    with_url = with_url.drop_duplicates(subset=["job_url_norm"], keep="first")
    clean = pd.concat([with_url, without_url], ignore_index=True)

    has_composite_key = (
        (clean["company_norm"] != "") | (clean["title_norm"] != "") | (clean["location_norm"] != "")
    )
    with_composite_key = clean[has_composite_key].drop_duplicates(
        subset=["company_norm", "title_norm", "location_norm"], keep="first"
    )
    without_composite_key = clean[~has_composite_key]
    clean = pd.concat([with_composite_key, without_composite_key], ignore_index=True)

    kept_indexes: list[int] = []
    for _, group in clean.groupby(["company_norm", "location_norm"], dropna=False):
        local_kept: list[int] = []
        for idx, row in group.iterrows():
            candidate = str(row.get("title_norm", ""))
            if not candidate:
                local_kept.append(idx)
                continue
            is_duplicate = False
            for kept_idx in local_kept:
                existing = str(group.loc[kept_idx, "title_norm"])
                ratio = SequenceMatcher(None, candidate, existing).ratio()
                if ratio >= similarity_threshold:
                    is_duplicate = True
                    break
            if not is_duplicate:
                local_kept.append(idx)
        kept_indexes.extend(local_kept)

    deduped = clean.loc[sorted(set(kept_indexes))].copy()
    deduped = deduped.drop(columns=["job_url_norm", "title_norm", "company_norm", "location_norm"], errors="ignore")
    deduped["job_id"] = deduped.apply(
        lambda row: compute_job_id(
            str(row.get("job_url", "")),
            str(row.get("company", "")),
            str(row.get("title", "")),
            str(row.get("location", "")),
        ),
        axis=1,
    )
    for column in CANONICAL_COLUMNS:
        if column not in deduped.columns:
            deduped[column] = ""
    return deduped[CANONICAL_COLUMNS].reset_index(drop=True)


def dedupe_jobs(base_dir: str | Path | None = None, run_date: date | None = None, logger=None) -> dict[str, Any]:
    root = get_project_root(base_dir)
    ensure_required_dirs(root)
    run_date = run_date or datetime.now().date()

    if logger is None:
        logger = init_logger("job_hunter.dedupe_jobs", root / "logs" / f"run_{run_date:%Y%m%d}.log")

    scoring_cfg = load_yaml(root / "config" / "scoring_config.yaml")
    threshold = float(scoring_cfg.get("dedupe", {}).get("title_similarity_threshold", 0.92))
    master_path = root / "data" / "processed" / "jobs_master.csv"
    if not master_path.exists():
        logger.info("No jobs_master.csv found at %s. Nothing to dedupe.", master_path)
        return {"count": 0, "path": str(master_path)}

    df = pd.read_csv(master_path)
    before = len(df)
    deduped = dedupe_jobs_df(df, similarity_threshold=threshold)
    deduped.to_csv(master_path, index=False)
    logger.info("Deduped jobs: %s -> %s rows", before, len(deduped))
    return {"count": int(len(deduped)), "path": str(master_path)}


def main() -> int:
    parser = argparse.ArgumentParser(description="Dedupe canonical jobs_master.csv records.")
    parser.add_argument("--base-dir", type=str, default=None, help="Project root directory.")
    args = parser.parse_args()
    dedupe_jobs(base_dir=args.base_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
