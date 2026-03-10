from __future__ import annotations

import argparse
import hashlib
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

try:
    from scripts.pipeline_utils import ensure_required_dirs, get_project_root, init_logger, load_yaml, normalize_space, normalize_url, write_jsonl
except ModuleNotFoundError:
    from pipeline_utils import ensure_required_dirs, get_project_root, init_logger, load_yaml, normalize_space, normalize_url, write_jsonl  # type: ignore


def _is_summary_row(lower_row: str) -> bool:
    tokens = [
        "featured postings",
        "new posting since last login",
        "postings added in the last",
        "application deadlines today",
        "application deadlines in the next",
        "job no longer available",
        "app status | tags | job title",
        "new graduate opportunities",
        "dashboard",
        "interviews",
        "documents",
        "appointments",
    ]
    return any(token in lower_row for token in tokens)


def _parse_rows_from_dom(page) -> list[dict[str, Any]]:
    payload = page.evaluate(
        """
        () => {
          const out = [];
          const trs = Array.from(document.querySelectorAll('table tr'));
          for (const tr of trs) {
            const cells = Array.from(tr.querySelectorAll('th,td'))
              .map(c => (c.innerText || c.textContent || '').replace(/\\s+/g, ' ').trim())
              .filter(Boolean);
            if (!cells.length) continue;

            let postingId = '';
            let viewOnclick = '';
            const actions = Array.from(tr.querySelectorAll('a,button,input[type="button"],input[type="submit"]'));
            for (const el of actions) {
              const cls = (el.className || '').toString();
              let m = cls.match(/np-view-btn-(\\d+)/) || cls.match(/np-apply-btn-(\\d+)/);
              if (m) {
                postingId = m[1];
                viewOnclick = (el.getAttribute('onclick') || '').toString();
                break;
              }
              const onclick = (el.getAttribute('onclick') || '').toString();
              m = onclick.match(/postingId':'(\\d+)'/);
              if (m) {
                postingId = m[1];
                viewOnclick = onclick;
                break;
              }
            }

            out.push({
              cells,
              rowText: cells.join(' | '),
              postingId,
              viewOnclick,
            });
          }
          return out;
        }
        """
    )
    rows: list[dict[str, Any]] = []
    for item in payload:
        cells = [normalize_space(str(x)) for x in item.get("cells", []) if normalize_space(str(x))]
        row_text = normalize_space(str(item.get("rowText", "")))
        posting_id = normalize_space(str(item.get("postingId", "")))
        view_onclick = str(item.get("viewOnclick", "") or "")
        if cells:
            rows.append({"cells": cells, "row_text": row_text, "posting_id": posting_id, "view_onclick": view_onclick})
    return rows


def _build_row_record(cells: list[str], row_text: str, detail_url: str, run_date: date) -> dict[str, Any] | None:
    lower_row = row_text.lower()
    if _is_summary_row(lower_row):
        return None
    if row_text.lower().startswith("0 |"):
        return None

    title = ""
    company = ""
    location = "London, ON"
    date_posted = ""

    if len(cells) >= 7 and any("deadline" in cell.lower() or cell.lower().startswith("new") for cell in cells[:3]):
        title = cells[2]
        company = cells[3] if len(cells) > 3 else ""
        location = cells[-2] if len(cells) >= 2 else "London, ON"
        date_posted = cells[-1]
    else:
        title = next(
            (cell for cell in cells if len(cell) > 3 and not any(x in cell.lower() for x in ["view", "details", "apply"])),
            cells[0],
        )
        company = cells[1] if len(cells) > 1 else ""
        location = cells[2] if len(cells) > 2 else "London, ON"
        for cell in cells:
            if any(char.isdigit() for char in cell) and any(sep in cell for sep in ["/", "-", ","]):
                date_posted = cell
                break

    if not title or title.lower() in {"job postings", "internship/co-op", "undergraduate and graduate"}:
        return None

    return {
        "title": title,
        "company": company,
        "location": location or "London, ON",
        "job_url": detail_url,
        "apply_url": detail_url,
        "source": "uwo_connect",
        "date_posted": date_posted,
        "date_found": run_date.isoformat(),
        "description": row_text,
        "keywords": "",
        "search_term": "uwo_connect",
    }


def _capture_detail_for_posting(page, context, posting_id: str, view_onclick: str, logger) -> tuple[str, str]:
    if not posting_id:
        return "", ""

    before_url = normalize_url(page.url)

    try:
        with context.expect_page(timeout=4500) as new_page_info:
            if view_onclick:
                page.evaluate(
                    """
                    (code) => {
                      try {
                        eval(code);
                        return true;
                      } catch (e) {
                        return false;
                      }
                    }
                    """,
                    view_onclick,
                )
            else:
                action = page.locator(f".np-view-btn-{posting_id}, .np-apply-btn-{posting_id}").first
                if action.count() == 0:
                    return "", ""
                action.click(timeout=3000)
        detail_page = new_page_info.value
        detail_page.wait_for_load_state("domcontentloaded", timeout=12000)
        detail_page.wait_for_timeout(800)
        detail_url = normalize_url(detail_page.url) or f"{before_url}#posting-{posting_id}"
        detail_text = normalize_space(detail_page.locator("body").inner_text(timeout=6000))[:24000]
        detail_page.close()
        return detail_url, detail_text
    except Exception:
        pass

    # Fallback: same-tab navigation
    try:
        if view_onclick:
            page.evaluate(
                """
                (code) => {
                  try {
                    eval(code);
                    return true;
                  } catch (e) {
                    return false;
                  }
                }
                """,
                view_onclick,
            )
        else:
            action = page.locator(f".np-view-btn-{posting_id}, .np-apply-btn-{posting_id}").first
            if action.count() == 0:
                return "", ""
            action.click(timeout=3000)
        page.wait_for_timeout(1000)
    except Exception as exc:
        logger.warning("Failed clicking posting %s: %s", posting_id, exc)
        return "", ""

    current_url = normalize_url(page.url)
    if current_url and current_url != before_url and "jobposting/jobs.htm" not in current_url.lower():
        detail_text = normalize_space(page.locator("body").inner_text(timeout=6000))[:24000]
        try:
            page.go_back(wait_until="domcontentloaded", timeout=10000)
            page.wait_for_timeout(800)
        except Exception as exc:
            logger.warning("Failed returning from detail page for posting %s: %s", posting_id, exc)
        return current_url, detail_text

    return "", ""


def _extract_table_records(
    page,
    context,
    run_date: date,
    page_index: int,
    extract_details: bool,
    max_rows: int,
    max_detail_records: int,
    logger,
) -> list[dict[str, Any]]:
    rows = _parse_rows_from_dom(page)
    records: list[dict[str, Any]] = []
    row_seen: set[str] = set()
    detail_count = 0

    for row in rows:
        if len(records) >= max_rows:
            break

        cells = row["cells"]
        row_text = row["row_text"]
        posting_id = row["posting_id"]
        view_onclick = row.get("view_onclick", "")
        default_url = normalize_url(f"{page.url}#posting-{posting_id or hashlib.sha1(row_text.encode('utf-8')).hexdigest()[:12]}")

        record = _build_row_record(cells=cells, row_text=row_text, detail_url=default_url, run_date=run_date)
        if record is None:
            continue

        if extract_details and posting_id and detail_count < max_detail_records:
            detail_url, detail_text = _capture_detail_for_posting(
                page=page,
                context=context,
                posting_id=posting_id,
                view_onclick=view_onclick,
                logger=logger,
            )
            if detail_url:
                record["job_url"] = detail_url
                record["apply_url"] = detail_url
            if detail_text:
                record["description"] = detail_text
            detail_count += 1

        row_key = "|".join(
            [
                str(record["title"]),
                str(record["company"]),
                str(record["location"]),
                str(record["date_posted"]),
                posting_id,
                str(page_index),
            ]
        )
        if row_key in row_seen:
            continue
        row_seen.add(row_key)
        records.append(record)

    return records


def _page_signature(page) -> str:
    text = page.evaluate(
        """
        () => {
          const rows = Array.from(document.querySelectorAll('table tr')).slice(0, 40);
          return rows.map(r => (r.innerText || '').replace(/\\s+/g, ' ').trim()).join(' || ');
        }
        """
    )
    return hashlib.sha1(normalize_space(str(text)).encode("utf-8")).hexdigest()


def _goto_page_number(page, target_page: int) -> bool:
    # Western Connect pager links look like: loadPostingTable(..., '2', ...)
    candidates = page.locator(f"a:has-text('{target_page}')")
    count = candidates.count()
    for idx in range(count):
        item = candidates.nth(idx)
        try:
            onclick = normalize_space(item.get_attribute("onclick") or "")
            if "loadPostingTable" not in onclick:
                continue
            item.click(timeout=3000)
            page.wait_for_timeout(1400)
            return True
        except Exception:
            continue
    return False


def _open_rich_posting_bucket(page, logger) -> bool:
    # If we are on a summary bucket page (counts only), click into a real postings bucket first.
    clicked = page.evaluate(
        """
        () => {
          const tables = Array.from(document.querySelectorAll('table'));
          for (const table of tables) {
            const rows = Array.from(table.querySelectorAll('tr'));
            for (const tr of rows) {
              const cells = Array.from(tr.querySelectorAll('th,td'))
                .map(c => (c.innerText || c.textContent || '').replace(/\\s+/g, ' ').trim())
                .filter(Boolean);
              const trText = (tr.innerText || '').replace(/\\s+/g, ' ').trim().toLowerCase();
              let hasCount = false;
              let looksLikeBucket = false;
              let explicitBucket = /new graduate opportunities|postings added in the last 7 days|application deadlines/.test(trText);

              if (cells.length >= 2) {
                const first = cells[0] || '';
                const second = (cells[1] || '').toLowerCase();
                hasCount = /^\\d+$/.test(first) && Number(first) > 0;
                looksLikeBucket = /opportunit|postings|deadline/.test(second);
              } else {
                const m = trText.match(/^(\\d+)\\s*\\|\\s*(.+)$/);
                if (m) {
                  hasCount = Number(m[1]) > 0;
                  looksLikeBucket = /opportunit|postings|deadline/.test(m[2] || '');
                }
              }

              if (!(explicitBucket || (hasCount && looksLikeBucket))) continue;
              const target =
                tr.querySelector('a,button,input[type="button"],input[type="submit"]') ||
                tr.querySelector('[onclick]') ||
                tr.querySelector('td');
              if (!target) continue;
              target.click();
              return true;
            }
          }
          return false;
        }
        """
    )
    if bool(clicked):
        logger.info("UWO Connect summary bucket detected. Clicked into postings list bucket.")
        page.wait_for_timeout(1800)
        return True
    return False


def _force_load_posting_table(page, logger) -> bool:
    loaded = page.evaluate(
        """
        () => {
          if (typeof loadPostingTable !== 'function') return false;
          loadPostingTable('', 'ID', 'Reverse', '1', 'advanced', '', null);
          return true;
        }
        """
    )
    if bool(loaded):
        logger.info("Forced loadPostingTable(...) to reset UWO listing to page 1.")
        page.wait_for_timeout(1800)
        return True
    return False


def _ensure_listing_view(page, logger) -> None:
    rows = _parse_rows_from_dom(page)
    if len(rows) >= 10:
        return

    back_locator = page.locator("a:has-text('Back to'), button:has-text('Back to')").first
    try:
        if back_locator.count() > 0 and back_locator.is_visible():
            back_locator.click(timeout=2500)
            page.wait_for_timeout(1500)
            logger.info("Clicked 'Back to...' to return from detail page to postings list.")
    except Exception:
        pass


def fetch_uwo_connect(base_dir: str | Path | None = None, run_date: date | None = None, logger=None) -> dict[str, Any]:
    root = get_project_root(base_dir)
    ensure_required_dirs(root)
    run_date = run_date or datetime.now().date()
    stamp = f"{run_date:%Y%m%d}"

    if logger is None:
        logger = init_logger("job_hunter.fetch_uwo_connect", root / "logs" / f"run_{stamp}.log")

    sources_cfg = load_yaml(root / "config" / "sources.yaml")
    uwo_cfg = sources_cfg.get("uwo_connect", {})
    enabled = bool(uwo_cfg.get("enabled", False))
    output_path = root / "data" / "raw" / "alerts" / f"uwo_connect_{stamp}.jsonl"
    if not enabled:
        write_jsonl(output_path, [])
        logger.info("UWO Connect fetch is disabled in config/sources.yaml.")
        return {"count": 0, "path": str(output_path), "source_summary": {}, "failure_summary": {}}

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.warning("playwright is not installed. Install with: pip install playwright && playwright install chromium")
        write_jsonl(output_path, [])
        return {"count": 0, "path": str(output_path), "source_summary": {}, "failure_summary": {"uwo_connect": 1}}

    dashboard_url = str(uwo_cfg.get("dashboard_url", "https://connect.uwo.ca/myAccount/dashboard.htm")).strip()
    jobs_url = str(uwo_cfg.get("jobs_url", "")).strip()
    headless = bool(uwo_cfg.get("headless", False))
    require_manual_confirm = bool(uwo_cfg.get("require_manual_confirm", True))
    manual_wait_seconds = max(0, int(uwo_cfg.get("manual_wait_seconds", 0)))
    user_data_rel = str(uwo_cfg.get("user_data_dir", "data/raw/uwo_connect_session"))
    user_data_dir = (root / user_data_rel).resolve()
    max_pages = max(1, int(uwo_cfg.get("max_pages", 3)))
    max_rows_per_page = max(1, int(uwo_cfg.get("max_rows_per_page", 120)))
    extract_details = bool(uwo_cfg.get("extract_details", True))
    max_detail_records = max(0, int(uwo_cfg.get("max_detail_records", 25)))

    if require_manual_confirm and manual_wait_seconds <= 0 and not sys.stdin.isatty():
        logger.warning("UWO Connect requires manual confirmation, but stdin is non-interactive. Skipping this run.")
        write_jsonl(output_path, [])
        return {"count": 0, "path": str(output_path), "source_summary": {}, "failure_summary": {"uwo_connect": 1}}

    records: list[dict[str, Any]] = []
    failure_summary: dict[str, int] = {}
    user_data_dir.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as playwright:
        context = playwright.chromium.launch_persistent_context(user_data_dir=str(user_data_dir), headless=headless)
        try:
            page = context.pages[0] if context.pages else context.new_page()
            page.goto(dashboard_url, wait_until="domcontentloaded", timeout=60000)

            if manual_wait_seconds > 0:
                print(
                    "\n[UWO Connect] Please complete Western login + Duo and open the job postings table.\n"
                    f"Waiting {manual_wait_seconds} seconds before scraping..."
                )
                page.wait_for_timeout(manual_wait_seconds * 1000)
            elif require_manual_confirm:
                print(
                    "\n[UWO Connect] Please complete Western login + Duo in the opened browser,\n"
                    "navigate to your Internship/Co-op job postings page, then press Enter here to continue..."
                )
                input()

            if jobs_url:
                page.goto(jobs_url, wait_until="domcontentloaded", timeout=60000)

            _ensure_listing_view(page, logger=logger)
            initial_rows = _parse_rows_from_dom(page)
            initial_posting_rows = [row for row in initial_rows if row.get("posting_id")]
            if len(initial_posting_rows) < 10:
                _open_rich_posting_bucket(page, logger=logger)

            seen_signatures: set[str] = set()
            for page_index in range(1, max_pages + 1):
                page.wait_for_timeout(1000)
                signature = _page_signature(page)
                if signature in seen_signatures:
                    logger.info("UWO Connect repeated page at index=%s. Stopping pagination.", page_index)
                    break
                seen_signatures.add(signature)

                page_records = _extract_table_records(
                    page=page,
                    context=context,
                    run_date=run_date,
                    page_index=page_index,
                    extract_details=extract_details,
                    max_rows=max_rows_per_page,
                    max_detail_records=max_detail_records,
                    logger=logger,
                )
                logger.info("UWO Connect page %s -> extracted %s rows", page_index, len(page_records))
                records.extend(page_records)

                next_page = page_index + 1
                if next_page > max_pages:
                    break
                if not _goto_page_number(page, next_page):
                    break
        except Exception as exc:
            failure_summary["uwo_connect"] = failure_summary.get("uwo_connect", 0) + 1
            logger.warning("UWO Connect fetch failed: %s", exc)
        finally:
            context.close()

    deduped: dict[str, dict[str, Any]] = {}
    for record in records:
        job_url = normalize_url(str(record.get("job_url", "")))
        if not job_url:
            continue
        record["job_url"] = job_url
        record["apply_url"] = normalize_url(str(record.get("apply_url", ""))) or job_url
        deduped[job_url] = record

    final_records = list(deduped.values())
    write_jsonl(output_path, final_records)
    logger.info("Fetched %s UWO Connect records -> %s", len(final_records), output_path)
    return {
        "count": len(final_records),
        "path": str(output_path),
        "source_summary": {"uwo_connect": len(final_records)},
        "failure_summary": failure_summary,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch UWO Connect postings with manual login confirmation.")
    parser.add_argument("--base-dir", type=str, default=None, help="Project root directory.")
    args = parser.parse_args()
    fetch_uwo_connect(base_dir=args.base_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
