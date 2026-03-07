from __future__ import annotations

import argparse
from datetime import date, datetime
from pathlib import Path
from typing import Any

try:
    from scripts.pipeline_utils import (
        ensure_required_dirs,
        get_project_root,
        init_logger,
        load_yaml,
        normalize_url,
        write_jsonl,
    )
except ModuleNotFoundError:
    from pipeline_utils import (  # type: ignore
        ensure_required_dirs,
        get_project_root,
        init_logger,
        load_yaml,
        normalize_url,
        write_jsonl,
    )


def _read_links(path: Path) -> list[str]:
    if not path.exists():
        return []
    links: list[str] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        links.append(line)
    deduped = sorted({normalize_url(link) for link in links if normalize_url(link)})
    return deduped


def ingest_manual_links(base_dir: str | Path | None = None, run_date: date | None = None, logger=None) -> dict[str, Any]:
    root = get_project_root(base_dir)
    ensure_required_dirs(root)
    run_date = run_date or datetime.now().date()

    if logger is None:
        logger = init_logger("job_hunter.ingest_manual_links", root / "logs" / f"run_{run_date:%Y%m%d}.log")

    sources_cfg = load_yaml(root / "config" / "sources.yaml")
    manual_cfg = sources_cfg.get("manual_links", {})
    enabled = bool(manual_cfg.get("enabled", True))
    input_rel = str(manual_cfg.get("path", "data/raw/alerts/links_today.txt"))
    input_path = (root / input_rel).resolve()
    output_path = root / "data" / "raw" / "alerts" / f"alerts_manual_{run_date:%Y%m%d}.jsonl"

    if not enabled:
        logger.info("Manual link ingestion disabled in config/sources.yaml.")
        write_jsonl(output_path, [])
        return {"count": 0, "path": str(output_path)}

    links = _read_links(input_path)
    if not links:
        logger.info("No manual links found at %s.", input_path)
        write_jsonl(output_path, [])
        return {"count": 0, "path": str(output_path)}

    records = []
    date_found = run_date.isoformat()
    for link in links:
        records.append(
            {
                "title": "",
                "company": "",
                "location": "",
                "job_url": link,
                "apply_url": link,
                "source": "manual_alert",
                "date_posted": "",
                "date_found": date_found,
                "description": "",
                "keywords": "",
                "search_term": "manual_link",
            }
        )

    write_jsonl(output_path, records)
    logger.info("Ingested %s manual links -> %s", len(records), output_path)
    return {"count": len(records), "path": str(output_path)}


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest manually pasted job alert links.")
    parser.add_argument("--base-dir", type=str, default=None, help="Project root directory.")
    args = parser.parse_args()
    ingest_manual_links(base_dir=args.base_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
