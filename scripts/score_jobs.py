from __future__ import annotations

import argparse
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from scripts.pipeline_utils import (
        CANONICAL_COLUMNS,
        ensure_required_dirs,
        get_project_root,
        init_logger,
        load_yaml,
        normalize_text_key,
    )
except ModuleNotFoundError:
    from pipeline_utils import (  # type: ignore
        CANONICAL_COLUMNS,
        ensure_required_dirs,
        get_project_root,
        init_logger,
        load_yaml,
        normalize_text_key,
    )


def select_torch_device(torch_module=None) -> str:
    module = torch_module
    if module is None:
        try:
            import torch as module  # type: ignore
        except Exception:
            return "cpu"
    try:
        if bool(module.cuda.is_available()):
            return "cuda"
    except Exception:
        return "cpu"
    return "cpu"


def _safe_load_model(model_name: str, device: str, logger):
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        logger.warning("sentence-transformers is not installed; semantic score set to 0.")
        return None

    try:
        return SentenceTransformer(model_name, device=device)
    except Exception as exc:
        logger.warning("Failed to load semantic model '%s': %s", model_name, exc)
        return None


def _flatten_skills(skills_cfg: dict[str, Any]) -> list[str]:
    terms: list[str] = []
    for value in skills_cfg.values():
        if isinstance(value, list):
            terms.extend(str(item).strip().lower() for item in value if str(item).strip())
    return sorted(set(terms))


def _build_profile_text(root: Path, skills_cfg: dict[str, Any]) -> str:
    profile_md = (root / "data" / "profile" / "user_profile.md").read_text(encoding="utf-8")
    skills_flat = _flatten_skills(skills_cfg)
    return profile_md + "\n\nskills: " + ", ".join(skills_flat)


def _compute_rule_score(title: str, description: str, location: str, search_cfg: dict[str, Any]) -> tuple[float, bool, bool, bool]:
    text = normalize_text_key(f"{title} {description}")
    location_key = normalize_text_key(location)

    intern_terms = [normalize_text_key(item) for item in search_cfg.get("job_types", [])]
    exclude_terms = [normalize_text_key(item) for item in search_cfg.get("exclude", [])]
    preferred_locations = [normalize_text_key(item) for item in search_cfg.get("locations", [])]

    is_intern_like = any(term and term in text for term in intern_terms)
    is_senior_like = any(term and term in text for term in exclude_terms)
    is_canada_like = "canada" in location_key or "remote" in location_key
    if not is_canada_like:
        is_canada_like = any(loc and loc in location_key for loc in preferred_locations)

    score = 0.0
    score += 0.45 if is_intern_like else 0.15
    score += 0.35 if is_canada_like else 0.10
    score += 0.20 if not is_senior_like else 0.0
    if is_senior_like:
        score *= 0.25
    return min(max(score, 0.0), 1.0), is_intern_like, is_canada_like, is_senior_like


def _compute_keyword_score(title: str, description: str, search_cfg: dict[str, Any], skills_cfg: dict[str, Any]) -> float:
    keywords = [normalize_text_key(item) for item in search_cfg.get("keywords", [])]
    skills = [normalize_text_key(item) for item in _flatten_skills(skills_cfg)]
    full_text = normalize_text_key(f"{title} {description}")

    matched: set[str] = set()
    for term in keywords + skills:
        if term and term in full_text:
            matched.add(term)

    denominator = max(1, min(20, len(set(keywords + skills))))
    return min(len(matched) / denominator, 1.0)


def _compute_freshness_score(date_posted: str, run_date: date) -> float:
    if not date_posted or not str(date_posted).strip():
        return 0.50
    posted = pd.to_datetime(date_posted, errors="coerce")
    if pd.isna(posted):
        return 0.50
    age_days = max(0, (run_date - posted.date()).days)
    if age_days <= 1:
        return 1.0
    if age_days <= 3:
        return 0.85
    if age_days <= 7:
        return 0.70
    if age_days <= 14:
        return 0.45
    return 0.20


def _build_company_tiers(target_cfg: dict[str, Any]) -> dict[str, float]:
    tier_scores = {"tier_a": 1.0, "tier_b": 0.7, "tier_c": 0.4}
    output: dict[str, float] = {}
    for tier_name, companies in target_cfg.items():
        score = tier_scores.get(tier_name, 0.2)
        if not isinstance(companies, list):
            continue
        for company in companies:
            key = normalize_text_key(str(company))
            if key:
                output[key] = score
    return output


def _compute_company_score(company: str, company_tiers: dict[str, float]) -> float:
    key = normalize_text_key(company)
    if not key:
        return 0.10
    return company_tiers.get(key, 0.10)


def _recommend_resume(title: str, description: str) -> str:
    text = normalize_text_key(f"{title} {description}")
    categories = {
        "robotics": ["robotics", "ros", "autonomy", "perception", "sensor fusion", "embedded"],
        "ml": ["machine learning", "deep learning", "computer vision", "nlp", "pytorch", "model"],
        "swe": ["software", "backend", "frontend", "full stack", "api", "developer", "python", "c++"],
    }
    scores = {key: 0 for key in categories}
    for category, terms in categories.items():
        for term in terms:
            if normalize_text_key(term) in text:
                scores[category] += 1
    best = max(scores.items(), key=lambda item: item[1])[0]
    if scores[best] == 0:
        return "swe"
    return best


def _compute_semantic_scores(texts: list[str], profile_text: str, model, logger) -> list[float]:
    if not texts:
        return []
    if model is None:
        return [0.0 for _ in texts]
    try:
        from sentence_transformers import util

        job_embeddings = model.encode(texts, convert_to_tensor=True, normalize_embeddings=True)
        profile_embedding = model.encode([profile_text], convert_to_tensor=True, normalize_embeddings=True)
        cosine = util.cos_sim(job_embeddings, profile_embedding).squeeze(-1).tolist()
        if isinstance(cosine, float):
            cosine = [cosine]
        scores = [max(0.0, min((float(value) + 1.0) / 2.0, 1.0)) for value in cosine]
        return scores
    except Exception as exc:
        logger.warning("Semantic scoring failed: %s", exc)
        return [0.0 for _ in texts]


def score_jobs(base_dir: str | Path | None = None, run_date: date | None = None, logger=None) -> dict[str, Any]:
    root = get_project_root(base_dir)
    ensure_required_dirs(root)
    run_date = run_date or datetime.now().date()

    if logger is None:
        logger = init_logger("job_hunter.score_jobs", root / "logs" / f"run_{run_date:%Y%m%d}.log")

    master_path = root / "data" / "processed" / "jobs_master.csv"
    if not master_path.exists():
        logger.info("No jobs_master.csv found at %s. Nothing to score.", master_path)
        return {"count": 0, "path": str(master_path)}

    df = pd.read_csv(master_path)
    if df.empty:
        logger.info("jobs_master.csv is empty. Nothing to score.")
        return {"count": 0, "path": str(master_path)}

    for column in CANONICAL_COLUMNS:
        if column not in df.columns:
            df[column] = ""

    search_cfg = load_yaml(root / "data" / "profile" / "search_config.yaml")
    skills_cfg = load_yaml(root / "data" / "profile" / "skills_master.yaml")
    target_cfg = load_yaml(root / "data" / "profile" / "target_companies.yaml")
    scoring_cfg = load_yaml(root / "config" / "scoring_config.yaml")

    weights = scoring_cfg.get("weights", {})
    rule_w = float(weights.get("rule", 0.40))
    keyword_w = float(weights.get("keyword", 0.20))
    semantic_w = float(weights.get("semantic", 0.20))
    freshness_w = float(weights.get("freshness", 0.10))
    company_w = float(weights.get("company", 0.10))

    semantic_cfg = scoring_cfg.get("semantic", {})
    semantic_enabled = bool(semantic_cfg.get("enabled", True))
    model_name = str(semantic_cfg.get("model_name", "sentence-transformers/all-MiniLM-L6-v2"))
    device = select_torch_device()
    logger.info("Semantic runtime device: %s", device)

    model = None
    if semantic_enabled and semantic_w > 0:
        model = _safe_load_model(model_name=model_name, device=device, logger=logger)

    profile_text = _build_profile_text(root, skills_cfg)
    semantic_texts = [f"{row.title} {row.description}" for row in df.itertuples(index=False)]
    semantic_scores = _compute_semantic_scores(semantic_texts, profile_text, model, logger)
    if len(semantic_scores) != len(df):
        semantic_scores = [0.0 for _ in range(len(df))]

    company_tiers = _build_company_tiers(target_cfg)

    rule_scores: list[float] = []
    keyword_scores: list[float] = []
    freshness_scores: list[float] = []
    company_scores: list[float] = []
    intern_flags: list[bool] = []
    canada_flags: list[bool] = []
    senior_flags: list[bool] = []
    recommendations: list[str] = []

    for row in df.itertuples(index=False):
        title = str(getattr(row, "title", "") or "")
        description = str(getattr(row, "description", "") or "")
        location = str(getattr(row, "location", "") or "")
        company = str(getattr(row, "company", "") or "")
        date_posted = str(getattr(row, "date_posted", "") or "")

        rule_score, is_intern_like, is_canada_like, is_senior_like = _compute_rule_score(
            title=title,
            description=description,
            location=location,
            search_cfg=search_cfg,
        )
        keyword_score = _compute_keyword_score(title=title, description=description, search_cfg=search_cfg, skills_cfg=skills_cfg)
        freshness_score = _compute_freshness_score(date_posted=date_posted, run_date=run_date)
        company_score = _compute_company_score(company=company, company_tiers=company_tiers)
        recommendation = _recommend_resume(title=title, description=description)

        rule_scores.append(rule_score)
        keyword_scores.append(keyword_score)
        freshness_scores.append(freshness_score)
        company_scores.append(company_score)
        intern_flags.append(is_intern_like)
        canada_flags.append(is_canada_like)
        senior_flags.append(is_senior_like)
        recommendations.append(recommendation)

    df["rule_score"] = rule_scores
    df["keyword_score"] = keyword_scores
    df["semantic_score"] = semantic_scores
    df["freshness_score"] = freshness_scores
    df["company_score"] = company_scores
    df["is_intern_like"] = intern_flags
    df["is_canada_like"] = canada_flags
    df["is_senior_like"] = senior_flags
    df["recommended_resume"] = recommendations

    df["final_score"] = (
        df["rule_score"] * rule_w
        + df["keyword_score"] * keyword_w
        + df["semantic_score"] * semantic_w
        + df["freshness_score"] * freshness_w
        + df["company_score"] * company_w
    ).clip(0.0, 1.0)

    tier_cfg = scoring_cfg.get("tiers", {})
    a_min = float(tier_cfg.get("a_min", 0.75))
    b_min = float(tier_cfg.get("b_min", 0.55))

    def assign_tier(score: float) -> str:
        if score >= a_min:
            return "A"
        if score >= b_min:
            return "B"
        return "C"

    df["tier"] = df["final_score"].astype(float).apply(assign_tier)

    for column in CANONICAL_COLUMNS:
        if column not in df.columns:
            df[column] = ""
    df = df[CANONICAL_COLUMNS]
    df.to_csv(master_path, index=False)
    logger.info("Scored %s jobs -> %s", len(df), master_path)
    return {"count": int(len(df)), "path": str(master_path)}


def main() -> int:
    parser = argparse.ArgumentParser(description="Score jobs in jobs_master.csv.")
    parser.add_argument("--base-dir", type=str, default=None, help="Project root directory.")
    args = parser.parse_args()
    score_jobs(base_dir=args.base_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
