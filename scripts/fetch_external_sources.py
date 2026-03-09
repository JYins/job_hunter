from __future__ import annotations

import argparse
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

try:
    from scripts.pipeline_utils import ensure_required_dirs, get_project_root, init_logger, load_yaml, normalize_url, write_jsonl
except ModuleNotFoundError:
    from pipeline_utils import ensure_required_dirs, get_project_root, init_logger, load_yaml, normalize_url, write_jsonl  # type: ignore


MARKDOWN_LINK_RE = re.compile(r"\[[^\]]+\]\((https?://[^)\s]+)\)")
HREF_RE = re.compile(r'href=["\']([^"\']+)["\']', re.IGNORECASE)


def _fetch_text(url: str, timeout: int = 20) -> str:
    request = Request(url, headers={"User-Agent": "job-hunter-bot/1.0"})
    with urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace")


def _extract_markdown_links(markdown_text: str) -> list[str]:
    links = [match.group(1).strip() for match in MARKDOWN_LINK_RE.finditer(markdown_text)]
    cleaned: list[str] = []
    for link in links:
        link_norm = normalize_url(link)
        if not link_norm:
            continue
        lower = link_norm.lower()
        if "github.com/" in lower:
            continue
        cleaned.append(link_norm)
    return sorted(set(cleaned))


def _extract_internee_links(base_url: str, html: str) -> list[str]:
    links: list[str] = []
    for match in HREF_RE.finditer(html):
        raw = match.group(1).strip()
        if not raw:
            continue
        full = normalize_url(urljoin(base_url, raw))
        if not full:
            continue
        lower = full.lower()
        if "internee.ca" not in lower:
            continue
        if "/job" not in lower and "intern" not in lower:
            continue
        links.append(full)
    return sorted(set(links))


def _fetch_repo_links(repo: str, logger) -> list[str]:
    branches = ["main", "master"]
    for branch in branches:
        url = f"https://raw.githubusercontent.com/{repo}/{branch}/README.md"
        try:
            content = _fetch_text(url)
            links = _extract_markdown_links(content)
            logger.info("Fetched %s links from GitHub repo=%s branch=%s", len(links), repo, branch)
            return links
        except URLError:
            continue
        except Exception as exc:
            logger.warning("Failed parsing GitHub repo=%s branch=%s: %s", repo, branch, exc)
            return []
    logger.warning("Could not fetch README for GitHub repo=%s", repo)
    return []


def fetch_external_sources(base_dir: str | Path | None = None, run_date: date | None = None, logger=None) -> dict[str, Any]:
    root = get_project_root(base_dir)
    ensure_required_dirs(root)
    run_date = run_date or datetime.now().date()

    if logger is None:
        logger = init_logger("job_hunter.fetch_external", root / "logs" / f"run_{run_date:%Y%m%d}.log")

    cfg = load_yaml(root / "config" / "sources.yaml")
    external_cfg = cfg.get("external_sources", {})
    enabled = bool(external_cfg.get("enabled", False))
    output_path = root / "data" / "raw" / "alerts" / f"external_sources_{run_date:%Y%m%d}.jsonl"
    if not enabled:
        write_jsonl(output_path, [])
        logger.info("External source fetch is disabled in config/sources.yaml.")
        return {"count": 0, "path": str(output_path), "source_summary": {}, "failure_summary": {}}

    records: list[dict[str, Any]] = []
    source_summary: dict[str, int] = {}
    failure_summary: dict[str, int] = {}
    date_found = run_date.isoformat()

    github_cfg = external_cfg.get("github_watch", {})
    if bool(github_cfg.get("enabled", True)):
        repos = [str(item).strip() for item in github_cfg.get("repos", []) if str(item).strip()]
        for repo in repos:
            source_name = f"github_repo:{repo}"
            try:
                links = _fetch_repo_links(repo, logger=logger)
            except Exception as exc:
                failure_summary[source_name] = failure_summary.get(source_name, 0) + 1
                logger.warning("GitHub watch fetch failed for %s: %s", repo, exc)
                continue
            source_summary[source_name] = len(links)
            for link in links:
                records.append(
                    {
                        "title": "",
                        "company": "",
                        "location": "Canada",
                        "job_url": link,
                        "apply_url": link,
                        "source": source_name,
                        "date_posted": "",
                        "date_found": date_found,
                        "description": "",
                        "keywords": "",
                        "search_term": "github_watch",
                    }
                )

    internee_cfg = external_cfg.get("internee", {})
    if bool(internee_cfg.get("enabled", True)):
        internee_url = str(internee_cfg.get("url", "https://internee.ca")).strip()
        source_name = "internee_ca"
        try:
            html = _fetch_text(internee_url)
            links = _extract_internee_links(internee_url, html)
            source_summary[source_name] = len(links)
            for link in links:
                records.append(
                    {
                        "title": "",
                        "company": "",
                        "location": "Canada",
                        "job_url": link,
                        "apply_url": link,
                        "source": source_name,
                        "date_posted": "",
                        "date_found": date_found,
                        "description": "",
                        "keywords": "",
                        "search_term": "internee_watch",
                    }
                )
        except Exception as exc:
            failure_summary[source_name] = failure_summary.get(source_name, 0) + 1
            logger.warning("Internee fetch failed for %s: %s", internee_url, exc)

    deduped: dict[str, dict[str, Any]] = {}
    for record in records:
        url_key = normalize_url(str(record.get("job_url", "")))
        if not url_key:
            continue
        deduped[url_key] = record

    final_records = list(deduped.values())
    write_jsonl(output_path, final_records)
    logger.info("Fetched %s records from external sources -> %s", len(final_records), output_path)
    logger.info("External source summary: %s", source_summary)
    logger.info("External failure summary: %s", failure_summary)
    return {
        "count": len(final_records),
        "path": str(output_path),
        "source_summary": source_summary,
        "failure_summary": failure_summary,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch jobs from external watch sources.")
    parser.add_argument("--base-dir", type=str, default=None, help="Project root directory.")
    args = parser.parse_args()
    fetch_external_sources(base_dir=args.base_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
