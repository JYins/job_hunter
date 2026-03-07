from __future__ import annotations

import hashlib
import json
import logging
import re
from pathlib import Path
from typing import Any

import yaml

CANONICAL_COLUMNS = [
    "job_id",
    "title",
    "company",
    "location",
    "job_url",
    "apply_url",
    "source",
    "date_posted",
    "date_found",
    "description",
    "keywords",
    "search_term",
    "is_intern_like",
    "is_canada_like",
    "is_senior_like",
    "rule_score",
    "keyword_score",
    "semantic_score",
    "freshness_score",
    "company_score",
    "final_score",
    "tier",
    "recommended_resume",
]


def get_project_root(base_dir: str | Path | None = None) -> Path:
    if base_dir:
        return Path(base_dir).resolve()
    return Path(__file__).resolve().parents[1]


def ensure_required_dirs(root: Path) -> None:
    required = [
        root / "config",
        root / "data" / "profile",
        root / "data" / "raw" / "alerts",
        root / "data" / "raw" / "jobspy",
        root / "data" / "processed",
        root / "data" / "outputs",
        root / "data" / "reports",
        root / "logs",
    ]
    for path in required:
        path.mkdir(parents=True, exist_ok=True)


def init_logger(name: str, log_file: Path) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if logger.handlers:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            handler.close()

    log_file.parent.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger


def load_yaml(path: Path, default: dict[str, Any] | None = None) -> dict[str, Any]:
    if not path.exists():
        return default or {}
    content = yaml.safe_load(path.read_text(encoding="utf-8"))
    if content is None:
        return default or {}
    if not isinstance(content, dict):
        raise ValueError(f"Expected YAML object in {path}, got {type(content).__name__}")
    return content


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, default=str))
            handle.write("\n")


def normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def normalize_text_key(value: str) -> str:
    value = normalize_space(value).lower()
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def tokenize(value: str) -> set[str]:
    text = normalize_text_key(value)
    if not text:
        return set()
    return set(text.split())


def normalize_url(url: str) -> str:
    url = normalize_space(url)
    if not url:
        return ""
    return url.rstrip("/")


def compute_job_id(job_url: str, company: str, title: str, location: str) -> str:
    seed = normalize_url(job_url)
    if not seed:
        seed = "|".join(
            [
                normalize_text_key(company),
                normalize_text_key(title),
                normalize_text_key(location),
            ]
        )
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
