from __future__ import annotations

import argparse
import json
import re
from datetime import date, datetime
from http.client import IncompleteRead
from html import unescape
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.parse import parse_qs, urljoin, urlparse
from urllib.request import Request, urlopen

try:
    from scripts.pipeline_utils import (
        ensure_required_dirs,
        get_project_root,
        init_logger,
        load_yaml,
        normalize_text_key,
        normalize_url,
        write_jsonl,
    )
except ModuleNotFoundError:
    from pipeline_utils import (  # type: ignore
        ensure_required_dirs,
        get_project_root,
        init_logger,
        load_yaml,
        normalize_text_key,
        normalize_url,
        write_jsonl,
    )


MARKDOWN_LINK_RE = re.compile(r"\[[^\]]+\]\((https?://[^)\s]+)\)")
HREF_RE = re.compile(r'href=["\']([^"\']+)["\']', re.IGNORECASE)
ANCHOR_RE = re.compile(r"<a\b[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", re.IGNORECASE | re.DOTALL)
HTML_TAG_RE = re.compile(r"<[^>]+>")
OFFICIAL_CAREER_LINK_HINTS = (
    "/job/",
    "/jobs/",
    "jobid=",
    "gh_jid",
    "requisition",
    "posting",
    "position",
    "opportunit",
    "intern",
    "student",
    "new-grad",
    "newgrad",
    "co-op",
    "coop",
)
OFFICIAL_CAREER_TITLE_HINTS = (
    "intern",
    "internship",
    "co-op",
    "coop",
    "student",
    "new grad",
    "graduate",
    "university",
    "campus",
)
ATS_DOMAIN_HINTS = (
    "ashbyhq.com",
    "app.ashbyhq.com",
    "greenhouse.io",
    "boards.greenhouse.io",
    "lever.co",
    "jobs.lever.co",
    "api.lever.co",
    "workdayjobs.com",
    "myworkdayjobs.com",
    "smartrecruiters.com",
    "jobvite.com",
    "ashbyhq.com",
    "icims.com",
    "workable.com",
    "bamboohr.com",
    "successfactors.com",
    "eightfold.ai",
    "applytojob.com",
    "ultipro.com",
)


def _fetch_text(url: str, timeout: int = 20) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-CA,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            return response.read().decode("utf-8", errors="replace")
    except IncompleteRead as exc:
        return exc.partial.decode("utf-8", errors="replace")


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


def _clean_anchor_text(value: str) -> str:
    text = HTML_TAG_RE.sub(" ", value or "")
    text = unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _clean_raw_url(value: str) -> str:
    text = unescape(str(value or ""))
    text = text.replace("\\/", "/")
    text = re.sub(r"\\u002[Ff]", "/", text)
    text = re.sub(r"\\u003[Aa]", ":", text)
    text = re.sub(r"[\"'<>]+$", "", text)
    text = text.strip().strip("),;\\")
    return text


def _normalize_domain(value: str) -> str:
    domain = str(value or "").strip().lower()
    domain = re.sub(r"^https?://", "", domain)
    return domain.strip("/")


def _domain_matches(domain: str, expected: str) -> bool:
    normalized = _normalize_domain(expected)
    return bool(normalized) and (domain == normalized or domain.endswith(f".{normalized}"))


def _is_company_job_link(url: str, anchor_text: str, allowed_domains: list[str]) -> bool:
    parsed = urlparse(url)
    domain = (parsed.netloc or "").lower()
    if not domain:
        return False

    normalized_allowed = [_normalize_domain(item) for item in allowed_domains if _normalize_domain(item)]
    is_allowed_domain = any(_domain_matches(domain, item) for item in normalized_allowed)
    is_ats_domain = any(_domain_matches(domain, item) for item in ATS_DOMAIN_HINTS)
    if not is_allowed_domain and not is_ats_domain:
        return False

    text = f"{url} {anchor_text}".lower()
    has_early_career_signal = any(token in text for token in OFFICIAL_CAREER_TITLE_HINTS)
    has_job_signal = any(token in text for token in OFFICIAL_CAREER_LINK_HINTS)
    if has_early_career_signal:
        return True
    if is_ats_domain and has_job_signal:
        return True
    return is_allowed_domain and has_job_signal and bool(
        re.search(r"(job|intern|student|graduate|coop|co-op|opportunity)", text)
    )


def _extract_company_career_links(
    base_url: str,
    html: str,
    allowed_domains: list[str],
    max_links: int,
) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []
    seen: set[str] = set()
    for match in ANCHOR_RE.finditer(html):
        raw_href = match.group(1).strip()
        if not raw_href:
            continue
        full_url = normalize_url(urljoin(base_url, raw_href))
        if not full_url or full_url in seen:
            continue
        title = _clean_anchor_text(match.group(2))
        if not _is_company_job_link(full_url, title, allowed_domains):
            continue
        seen.add(full_url)
        links.append({"url": full_url, "title": title})
        if len(links) >= max_links:
            break
    return links


def _extract_secondary_board_urls(base_url: str, html: str, allowed_domains: list[str]) -> list[str]:
    urls: list[str] = []
    allowed = [_normalize_domain(item) for item in allowed_domains if _normalize_domain(item)]
    candidates = [match.group(1) for match in HREF_RE.finditer(html)]
    candidates.extend(re.findall(r"https?://[^\s\"'<>]+", html))

    for raw in candidates:
        cleaned = _clean_raw_url(raw)
        if not cleaned:
            continue
        full_url = normalize_url(urljoin(base_url, cleaned))
        if not full_url:
            continue
        lower = full_url.lower()
        parsed = urlparse(full_url)
        domain = (parsed.netloc or "").lower()
        if not domain:
            continue
        if not any(_domain_matches(domain, item) for item in allowed + list(ATS_DOMAIN_HINTS)):
            continue
        if any(
            token in lower
            for token in [
                "jobs.lever.co/",
                "api.lever.co/v0/postings/",
                "api.greenhouse.io/v1/boards/",
                "boards.greenhouse.io/",
                "job-boards.greenhouse.io/",
                "jobs.ashbyhq.com/",
                "app.ashbyhq.com/",
                "applytojob.com/apply",
                "recruiting.ultipro.com/",
                "opportunitydetail",
                "/api/pcsx/search",
                "careers.qualcomm.com/careers",
                "app.eightfold.ai/careers",
            ]
        ):
            urls.append(full_url)

    return list(dict.fromkeys(urls))


def _build_external_record(
    *,
    title: str,
    company: str,
    location: str,
    job_url: str,
    source_name: str,
    date_found: str,
    date_posted: str = "",
    description: str = "",
    keywords: str = "",
    search_term: str = "company_career_watch",
) -> dict[str, Any]:
    return {
        "title": title,
        "company": company,
        "location": location or "Canada",
        "job_url": job_url,
        "apply_url": job_url,
        "source": source_name,
        "date_posted": date_posted,
        "date_found": date_found,
        "description": description,
        "keywords": keywords,
        "search_term": search_term,
    }


def _fetch_json(url: str, timeout: int = 20) -> Any:
    text = _fetch_text(url, timeout=timeout)
    return json.loads(text)


def _derive_lever_board_name(board_url: str) -> str:
    parsed = urlparse(board_url)
    parts = [part for part in parsed.path.split("/") if part]
    if parsed.netloc.lower() == "api.lever.co" and len(parts) >= 3 and parts[0] == "v0" and parts[1] == "postings":
        return parts[2]
    return parts[0] if parts else ""


def _fetch_lever_jobs(board_url: str, company: str, source_name: str, date_found: str, max_links: int) -> list[dict[str, Any]]:
    board_name = _derive_lever_board_name(board_url)
    if not board_name:
        return []
    api_url = f"https://api.lever.co/v0/postings/{board_name}?mode=json"
    payload = _fetch_json(api_url)
    if not isinstance(payload, list):
        return []

    records: list[dict[str, Any]] = []
    for item in payload[:max_links]:
        if not isinstance(item, dict):
            continue
        job_url = normalize_url(str(item.get("hostedUrl", "") or ""))
        if not job_url:
            continue
        categories = item.get("categories", {}) if isinstance(item.get("categories", {}), dict) else {}
        location = str(categories.get("location", "") or "Canada")
        title = str(item.get("text", "") or "")
        description = str(item.get("descriptionPlain", "") or "")
        created_at = item.get("createdAt")
        date_posted = ""
        if created_at is not None:
            try:
                date_posted = datetime.fromtimestamp(float(created_at) / 1000.0).date().isoformat()
            except Exception:
                date_posted = ""
        records.append(
            _build_external_record(
                title=title,
                company=company,
                location=location,
                job_url=job_url,
                source_name=source_name,
                date_found=date_found,
                date_posted=date_posted,
                description=description,
            )
        )
    return records


def _derive_greenhouse_board_name(board_url: str) -> str:
    parsed = urlparse(board_url)
    parts = [part for part in parsed.path.split("/") if part]
    host = parsed.netloc.lower()
    if host == "api.greenhouse.io" and len(parts) >= 4 and parts[0] == "v1" and parts[1] == "boards":
        return parts[2]
    if host in {"boards.greenhouse.io", "job-boards.greenhouse.io"} and parts:
        return parts[0]
    return ""


def _fetch_greenhouse_jobs(board_url: str, company: str, source_name: str, date_found: str, max_links: int) -> list[dict[str, Any]]:
    board_name = _derive_greenhouse_board_name(board_url)
    if not board_name:
        return []
    api_url = f"https://api.greenhouse.io/v1/boards/{board_name}/jobs"
    payload = _fetch_json(api_url)
    jobs = payload.get("jobs", []) if isinstance(payload, dict) else []
    if not isinstance(jobs, list):
        return []

    records: list[dict[str, Any]] = []
    for item in jobs[:max_links]:
        if not isinstance(item, dict):
            continue
        job_url = normalize_url(str(item.get("absolute_url", "") or ""))
        if not job_url:
            continue
        location_data = item.get("location", {}) if isinstance(item.get("location", {}), dict) else {}
        location = str(location_data.get("name", "") or "Canada")
        records.append(
            _build_external_record(
                title=str(item.get("title", "") or ""),
                company=str(item.get("company_name", "") or company),
                location=location,
                job_url=job_url,
                source_name=source_name,
                date_found=date_found,
                date_posted=str(item.get("updated_at", "") or item.get("first_published", "") or ""),
            )
        )
    return records


def _decode_json_fragment(value: str) -> str:
    try:
        return json.loads(f"\"{value}\"")
    except Exception:
        return value


def _extract_anchor_title(anchor_html: str, fallback_text: str = "") -> str:
    for pattern in [r"<h[1-6][^>]*>(.*?)</h[1-6]>", r"<strong[^>]*>(.*?)</strong>", r"<span[^>]*>(.*?)</span>"]:
        match = re.search(pattern, anchor_html, flags=re.IGNORECASE | re.DOTALL)
        if match:
            text = _clean_anchor_text(match.group(1))
            if text:
                return text
    return _clean_anchor_text(anchor_html) or fallback_text


def _fetch_ashby_jobs(board_url: str, company: str, source_name: str, date_found: str, max_links: int) -> list[dict[str, Any]]:
    parsed = urlparse(board_url)
    base_url = board_url.split("?", 1)[0]
    if base_url.endswith("/embed"):
        base_url = base_url[: -len("/embed")]
    if not parsed.query:
        embed_url = f"{base_url}?embed=js"
    elif "embed=" in parsed.query:
        embed_url = board_url
    else:
        separator = "&" if "?" in board_url else "?"
        embed_url = f"{board_url}{separator}embed=js"

    html = _fetch_text(embed_url)
    jobs_match = re.search(r'(\[\{"id":"[0-9a-f\-]+","title":"(?:\\.|[^"]+)".*?\}\])', html, flags=re.S | re.I)
    if jobs_match:
        try:
            jobs = json.loads(jobs_match.group(1))
        except Exception:
            jobs = []
        records: list[dict[str, Any]] = []
        for item in jobs:
            if not isinstance(item, dict) or not bool(item.get("isListed", True)):
                continue
            job_id = str(item.get("id", "") or "")
            if not job_id:
                continue
            job_url = normalize_url(f"{base_url}/{job_id}")
            location_parts = [str(item.get("locationName", "") or "").strip()]
            if isinstance(item.get("secondaryLocations", []), list):
                for secondary in item.get("secondaryLocations", []):
                    if isinstance(secondary, dict):
                        secondary_name = str(secondary.get("locationName", "") or "").strip()
                        if secondary_name:
                            location_parts.append(secondary_name)
            location = " | ".join(part for part in location_parts if part) or "Canada"
            records.append(
                _build_external_record(
                    title=str(item.get("title", "") or ""),
                    company=company,
                    location=location,
                    job_url=job_url,
                    source_name=source_name,
                    date_found=date_found,
                    date_posted=str(item.get("publishedDate", "") or ""),
                    description=" ".join(
                        part
                        for part in [
                            str(item.get("departmentName", "") or ""),
                            str(item.get("workplaceType", "") or ""),
                            str(item.get("employmentType", "") or ""),
                        ]
                        if part
                    ),
                )
            )
            if len(records) >= max_links:
                break
        if records:
            return records

    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    for match in ANCHOR_RE.finditer(html):
        href = _clean_raw_url(match.group(1))
        if not href:
            continue
        full_url = normalize_url(urljoin(embed_url, href))
        if not full_url or full_url in seen:
            continue
        lower = full_url.lower()
        if "ashby_jid=" not in lower and "jobs.ashbyhq.com" not in lower:
            continue
        seen.add(full_url)
        records.append(
            _build_external_record(
                title=_extract_anchor_title(match.group(2)),
                company=company,
                location="Canada",
                job_url=full_url,
                source_name=source_name,
                date_found=date_found,
            )
        )
        if len(records) >= max_links:
            break
    return records


def _fetch_applytojob_jobs(board_url: str, company: str, source_name: str, date_found: str, max_links: int) -> list[dict[str, Any]]:
    html = _fetch_text(board_url)
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    for match in ANCHOR_RE.finditer(html):
        href = _clean_raw_url(match.group(1))
        full_url = normalize_url(urljoin(board_url, href))
        if not full_url or full_url in seen or "/apply/" not in full_url.lower():
            continue
        seen.add(full_url)
        fallback_title = ""
        slug_parts = [part for part in urlparse(full_url).path.split("/") if part]
        if slug_parts:
            fallback_title = re.sub(r"[-_]+", " ", slug_parts[-1]).strip()
        records.append(
            _build_external_record(
                title=_extract_anchor_title(match.group(2), fallback_text=fallback_title),
                company=company,
                location="Canada",
                job_url=full_url,
                source_name=source_name,
                date_found=date_found,
            )
        )
        if len(records) >= max_links:
            break
    return records


def _fetch_ultipro_jobs(board_url: str, company: str, source_name: str, date_found: str, max_links: int) -> list[dict[str, Any]]:
    html = _fetch_text(board_url)
    board_root = board_url.split("?", 1)[0].rstrip("/") + "/"
    marker = "pageSize:"
    marker_index = html.find(marker)
    if marker_index != -1:
        end_index = html.rfind("]", 0, marker_index)
        if end_index != -1:
            depth = 0
            start_index = -1
            for pos in range(end_index, -1, -1):
                char = html[pos]
                if char == "]":
                    depth += 1
                elif char == "[":
                    depth -= 1
                    if depth == 0:
                        start_index = pos
                        break
            if start_index != -1:
                candidate = html[start_index : end_index + 1]
                try:
                    jobs = json.loads(candidate)
                except Exception:
                    jobs = []
                records: list[dict[str, Any]] = []
                for item in jobs:
                    if not isinstance(item, dict):
                        continue
                    opportunity_id = str(item.get("Id", "") or "")
                    title = str(item.get("Title", "") or "")
                    if not opportunity_id or not title:
                        continue
                    location_value = "Canada"
                    locations = item.get("Locations", []) if isinstance(item.get("Locations", []), list) else []
                    if locations and isinstance(locations[0], dict):
                        address = locations[0].get("Address", {}) if isinstance(locations[0].get("Address", {}), dict) else {}
                        city = str(address.get("City", "") or "").strip()
                        state = address.get("State", {}) if isinstance(address.get("State", {}), dict) else {}
                        state_code = str(state.get("Code", "") or "").strip()
                        location_value = ", ".join(part for part in [city, state_code, "Canada"] if part) or "Canada"
                    job_url = normalize_url(
                        urljoin(
                            board_root,
                            f"OpportunityDetail?opportunityId={opportunity_id}",
                        )
                    )
                    records.append(
                        _build_external_record(
                            title=title,
                            company=company,
                            location=location_value,
                            job_url=job_url,
                            source_name=source_name,
                            date_found=date_found,
                            date_posted=str(item.get("PostedDate", "") or "")[:10],
                            description=str(item.get("BriefDescription", "") or ""),
                        )
                    )
                    if len(records) >= max_links:
                        break
                if records:
                    return records

    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    for match in ANCHOR_RE.finditer(html):
        href = _clean_raw_url(match.group(1))
        full_url = normalize_url(urljoin(board_url, href))
        if not full_url or full_url in seen or "opportunitydetail" not in full_url.lower():
            continue
        seen.add(full_url)
        records.append(
            _build_external_record(
                title=_extract_anchor_title(match.group(2)),
                company=company,
                location="Canada",
                job_url=full_url,
                source_name=source_name,
                date_found=date_found,
            )
        )
        if len(records) >= max_links:
            break
    return records


def _fetch_pcsx_jobs(board_url: str, company: str, source_name: str, date_found: str, max_links: int) -> list[dict[str, Any]]:
    parsed = urlparse(board_url)
    origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else "https://careers.qualcomm.com"
    query = parse_qs(parsed.query)
    domain = str(query.get("domain", ["qualcomm.com"])[0] or "qualcomm.com")
    records: list[dict[str, Any]] = []
    start = 0
    page_size = 10

    while len(records) < max_links:
        api_url = f"{origin}/api/pcsx/search?domain={domain}&query=&location=&start={start}&sort_by=timestamp"
        payload = _fetch_json(api_url)
        data = payload.get("data", {}) if isinstance(payload, dict) else {}
        positions = data.get("positions", []) if isinstance(data, dict) else []
        if not isinstance(positions, list) or not positions:
            break
        for item in positions:
            if not isinstance(item, dict):
                continue
            position_url = normalize_url(urljoin(origin, str(item.get("positionUrl", "") or "")))
            if not position_url:
                continue
            locations = item.get("locations", []) if isinstance(item.get("locations", []), list) else []
            location = str(locations[0] if locations else "Canada")
            date_posted = ""
            posted_ts = item.get("postedTs")
            if posted_ts:
                try:
                    date_posted = datetime.fromtimestamp(float(posted_ts)).date().isoformat()
                except Exception:
                    date_posted = ""
            records.append(
                _build_external_record(
                    title=str(item.get("name", "") or ""),
                    company=company,
                    location=location,
                    job_url=position_url,
                    source_name=source_name,
                    date_found=date_found,
                    date_posted=date_posted,
                    description=str(item.get("department", "") or ""),
                )
            )
            if len(records) >= max_links:
                break
        if len(positions) < page_size:
            break
        start += len(positions)
    return records


def _fetch_board_jobs(board_url: str, company: str, source_name: str, date_found: str, max_links: int) -> list[dict[str, Any]]:
    lower = board_url.lower()
    if "jobs.lever.co/" in lower or "api.lever.co/v0/postings/" in lower:
        return _fetch_lever_jobs(board_url, company, source_name, date_found, max_links)
    if "api.greenhouse.io/v1/boards/" in lower or "boards.greenhouse.io/" in lower or "job-boards.greenhouse.io/" in lower:
        return _fetch_greenhouse_jobs(board_url, company, source_name, date_found, max_links)
    if "jobs.ashbyhq.com/" in lower:
        return _fetch_ashby_jobs(board_url, company, source_name, date_found, max_links)
    if "applytojob.com/apply" in lower:
        return _fetch_applytojob_jobs(board_url, company, source_name, date_found, max_links)
    if "recruiting.ultipro.com/" in lower:
        return _fetch_ultipro_jobs(board_url, company, source_name, date_found, max_links)
    if "careers.qualcomm.com" in lower or "app.eightfold.ai/careers" in lower:
        return _fetch_pcsx_jobs(board_url, company, source_name, date_found, max_links)
    return []


def _load_company_career_targets(root: Path, external_cfg: dict[str, Any], target_cfg: dict[str, Any]) -> list[dict[str, Any]]:
    company_careers_cfg = external_cfg.get("company_careers", {})
    if not bool(company_careers_cfg.get("enabled", False)):
        return []

    config_rel_path = str(company_careers_cfg.get("config_path", "config/company_careers.yaml")).strip()
    config = load_yaml(root / config_rel_path)
    entries = config.get("companies", [])
    if not isinstance(entries, list):
        return []

    include_tiers = company_careers_cfg.get("include_tiers", [])
    include_tier_keys = {str(item).strip() for item in include_tiers if str(item).strip()}
    active_companies: set[str] = set()
    for tier_name, companies in target_cfg.items():
        if include_tier_keys and str(tier_name) not in include_tier_keys:
            continue
        if not isinstance(companies, list):
            continue
        for company in companies:
            key = normalize_text_key(str(company))
            if key:
                active_companies.add(key)

    selected: list[dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        company = str(entry.get("company", "")).strip()
        aliases = [str(item).strip() for item in entry.get("aliases", []) if str(item).strip()]
        match_keys = {normalize_text_key(company)}
        match_keys.update(normalize_text_key(alias) for alias in aliases if normalize_text_key(alias))
        if active_companies and not match_keys.intersection(active_companies):
            continue
        selected.append(entry)
    return selected


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
    target_cfg = load_yaml(root / "data" / "profile" / "target_companies.yaml")
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

    company_targets = _load_company_career_targets(root=root, external_cfg=external_cfg, target_cfg=target_cfg)
    company_careers_cfg = external_cfg.get("company_careers", {})
    max_links_per_company = max(1, int(company_careers_cfg.get("max_links_per_company", 30)))
    for target in company_targets:
        company = str(target.get("company", "")).strip()
        career_url = str(target.get("career_url", "")).strip()
        if not company or not career_url:
            continue
        source_name = f"company_careers:{company}"
        allowed_domains = [str(item).strip() for item in target.get("allowed_domains", []) if str(item).strip()]
        allowed_domains.append(urlparse(career_url).netloc)
        try:
            html = _fetch_text(career_url)
            primary_links = _extract_company_career_links(
                base_url=career_url,
                html=html,
                allowed_domains=allowed_domains,
                max_links=max_links_per_company,
            )
            records_for_company: list[dict[str, Any]] = []
            for link in primary_links:
                job_url = link["url"]
                records_for_company.append(
                    _build_external_record(
                        title=link.get("title", ""),
                        company=company,
                        location="Canada",
                        job_url=job_url,
                        source_name=source_name,
                        date_found=date_found,
                    )
                )
            secondary_urls = _extract_secondary_board_urls(career_url, html, allowed_domains=allowed_domains)
            for board_url in secondary_urls:
                if len(records_for_company) >= max_links_per_company:
                    break
                remaining = max_links_per_company - len(records_for_company)
                records_for_company.extend(
                    _fetch_board_jobs(
                        board_url=board_url,
                        company=company,
                        source_name=source_name,
                        date_found=date_found,
                        max_links=remaining,
                    )
                )
            source_summary[source_name] = len(records_for_company)
            records.extend(records_for_company)
        except Exception as exc:
            failure_summary[source_name] = failure_summary.get(source_name, 0) + 1
            logger.warning("Company career fetch failed for %s (%s): %s", company, career_url, exc)

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
