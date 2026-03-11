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


def _normalize_match_text(value: str) -> str:
    text = str(value or "").lower()
    replacements = {
        "c++": "cpp",
        "c#": "csharp",
        "node.js": "nodejs",
        "co-op": "coop",
        "co op": "coop",
        "full-stack": "full stack",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    text = normalize_text_key(text)
    return f" {text} " if text else " "


def _build_term_key(term: str) -> str:
    return _normalize_match_text(term)


def _has_term(full_text_key: str, term_key: str) -> bool:
    if not term_key.strip():
        return False
    return term_key in full_text_key


def _normalize_weight_map(raw_weights: dict[str, float], default_weights: dict[str, float]) -> dict[str, float]:
    values = {key: float(raw_weights.get(key, default)) for key, default in default_weights.items()}
    positive = {key: max(0.0, value) for key, value in values.items()}
    total = sum(positive.values())
    if total <= 0:
        fallback_total = float(sum(default_weights.values()))
        if fallback_total <= 0:
            equal = 1.0 / max(1, len(default_weights))
            return {key: equal for key in default_weights}
        return {key: float(value) / fallback_total for key, value in default_weights.items()}
    return {key: value / total for key, value in positive.items()}


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


def _collect_search_tracks(search_cfg: dict[str, Any]) -> list[dict[str, Any]]:
    tracks = search_cfg.get("search_tracks", [])
    if not isinstance(tracks, list):
        return []
    output: list[dict[str, Any]] = []
    for track in tracks:
        if not isinstance(track, dict):
            continue
        key = str(track.get("key", "")).strip()
        resume_key = str(track.get("resume_key", "")).strip()
        role_terms = [str(item).strip() for item in track.get("role_terms", []) if str(item).strip()]
        skill_terms = [str(item).strip() for item in track.get("skill_terms", []) if str(item).strip()]
        keywords = [str(item).strip() for item in track.get("keywords", []) if str(item).strip()]
        weight = float(track.get("target_share", 0.0))
        output.append(
            {
                "key": key,
                "resume_key": resume_key,
                "weight": max(0.0, weight),
                "role_terms": role_terms,
                "skill_terms": skill_terms,
                "keywords": keywords,
            }
        )
    if not output:
        return []
    total_weight = sum(item["weight"] for item in output)
    if total_weight <= 0:
        uniform = 1.0 / len(output)
        for item in output:
            item["weight"] = uniform
        return output
    for item in output:
        item["weight"] = float(item["weight"]) / total_weight
    return output


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


def _compute_keyword_score(
    title: str,
    description: str,
    search_cfg: dict[str, Any],
    skills_cfg: dict[str, Any],
    search_tracks: list[dict[str, Any]],
) -> float:
    role_terms: list[str] = [str(item).strip() for item in search_cfg.get("keywords", []) if str(item).strip()]
    role_terms.extend(str(item).strip() for item in search_cfg.get("keyword_query_seeds", []) if str(item).strip())
    for track in search_tracks:
        role_terms.extend(track.get("role_terms", []))
        role_terms.extend(track.get("keywords", []))

    skill_terms = _flatten_skills(skills_cfg)
    for track in search_tracks:
        skill_terms.extend(track.get("skill_terms", []))

    role_terms = sorted(set(term.lower() for term in role_terms if term), key=len, reverse=True)
    skill_terms = sorted(set(term.lower() for term in skill_terms if term), key=len, reverse=True)

    full_text_key = _normalize_match_text(f"{title} {description}")
    title_key = _normalize_match_text(title)

    role_hits = 0
    title_hits = 0
    for term in role_terms:
        term_key = _build_term_key(term)
        if _has_term(full_text_key, term_key):
            role_hits += 1
            if _has_term(title_key, term_key):
                title_hits += 1

    skill_hits = 0
    for term in skill_terms:
        term_key = _build_term_key(term)
        if _has_term(full_text_key, term_key):
            skill_hits += 1

    role_den = max(1, min(35, len(role_terms)))
    skill_den = max(1, min(35, len(skill_terms)))
    role_score = role_hits / role_den
    skill_score = skill_hits / skill_den
    title_bonus = min(0.20, title_hits * 0.08)

    return min(1.0, max(0.0, role_score * 0.60 + skill_score * 0.40 + title_bonus))


def _compute_intent_score(title: str, description: str, search_tracks: list[dict[str, Any]]) -> tuple[float, str, str]:
    if not search_tracks:
        return 0.0, "", ""

    title_key = _normalize_match_text(title)
    full_text_key = _normalize_match_text(f"{title} {description}")

    best_score = 0.0
    best_track = ""
    best_resume = ""

    for track in search_tracks:
        role_terms = [str(item).strip() for item in track.get("role_terms", []) if str(item).strip()]
        skill_terms = [str(item).strip() for item in track.get("skill_terms", []) if str(item).strip()]
        keywords = [str(item).strip() for item in track.get("keywords", []) if str(item).strip()]
        if not role_terms and not skill_terms and not keywords:
            continue

        role_hits_title = 0
        role_hits_all = 0
        for term in role_terms:
            term_key = _build_term_key(term)
            if _has_term(full_text_key, term_key):
                role_hits_all += 1
                if _has_term(title_key, term_key):
                    role_hits_title += 1

        secondary_terms = sorted(set(skill_terms + keywords), key=len, reverse=True)
        secondary_hits = 0
        for term in secondary_terms:
            term_key = _build_term_key(term)
            if _has_term(full_text_key, term_key):
                secondary_hits += 1

        role_den = max(1, min(10, len(role_terms)))
        secondary_den = max(1, min(12, len(secondary_terms)))
        role_coverage = role_hits_all / role_den
        title_coverage = role_hits_title / role_den
        secondary_coverage = secondary_hits / secondary_den

        alignment = min(1.0, role_coverage * 0.45 + title_coverage * 0.40 + secondary_coverage * 0.15)
        priority = float(track.get("weight", 0.0))
        weighted_score = min(1.0, alignment * (0.70 + 0.30 * priority))

        if weighted_score > best_score:
            best_score = weighted_score
            best_track = str(track.get("key", ""))
            best_resume = str(track.get("resume_key", ""))

    return min(1.0, max(0.0, best_score)), best_track, best_resume


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


def _build_company_tiers(target_cfg: dict[str, Any], scoring_cfg: dict[str, Any]) -> tuple[dict[str, float], float]:
    company_cfg = scoring_cfg.get("company", {}) if isinstance(scoring_cfg.get("company", {}), dict) else {}
    tier_scores_cfg = company_cfg.get("tier_scores", {})
    default_tier_scores = {"tier_a": 1.0, "tier_b": 0.80, "tier_c": 0.60, "tier_d": 0.40}
    if isinstance(tier_scores_cfg, dict):
        for key, value in tier_scores_cfg.items():
            try:
                default_tier_scores[str(key)] = float(value)
            except Exception:
                continue

    unknown_score = float(company_cfg.get("unknown_score", 0.10))
    output: dict[str, float] = {}
    for tier_name, companies in target_cfg.items():
        score = float(default_tier_scores.get(str(tier_name), unknown_score))
        if not isinstance(companies, list):
            continue
        for company in companies:
            key = normalize_text_key(str(company))
            if key:
                output[key] = score
    return output, unknown_score


def _compute_company_score(company: str, company_tiers: dict[str, float], unknown_score: float) -> float:
    key = normalize_text_key(company)
    if not key:
        return unknown_score
    return company_tiers.get(key, unknown_score)


def _compute_source_score(source: str, scoring_cfg: dict[str, Any]) -> float:
    source_cfg = scoring_cfg.get("source", {}) if isinstance(scoring_cfg.get("source", {}), dict) else {}
    default_score = float(source_cfg.get("default_score", 0.55))
    unknown_score = float(source_cfg.get("unknown_score", default_score))
    source_key = str(source or "").strip().lower()
    if not source_key:
        return unknown_score

    exact_scores = source_cfg.get("exact_scores", {})
    if isinstance(exact_scores, dict):
        for key, value in exact_scores.items():
            if source_key == str(key).strip().lower():
                try:
                    return max(0.0, min(float(value), 1.0))
                except Exception:
                    return default_score

    prefix_scores = source_cfg.get("prefix_scores", {})
    if isinstance(prefix_scores, dict):
        for prefix, value in prefix_scores.items():
            prefix_key = str(prefix).strip().lower()
            if source_key == prefix_key or source_key.startswith(f"{prefix_key}:"):
                try:
                    return max(0.0, min(float(value), 1.0))
                except Exception:
                    return default_score

    return max(0.0, min(default_score, 1.0))


def _recommend_resume(title: str, description: str, resume_hint: str = "") -> str:
    if resume_hint:
        return resume_hint
    text = normalize_text_key(f"{title} {description}")
    categories = {
        "rbo_2": ["robotics", "ros", "autonomy", "embedded", "controls", "simulink"],
        "pc_2": ["perception", "lidar", "computer vision", "sensor fusion", "autonomous driving"],
        "ae_2": ["rag", "nlp", "information retrieval", "evaluation", "llm", "langchain"],
        "m1": ["software", "backend", "platform", "developer", "api", "python", "c++", "sql"],
    }
    scores = {key: 0 for key in categories}
    for category, terms in categories.items():
        for term in terms:
            if normalize_text_key(term) in text:
                scores[category] += 1
    best = max(scores.items(), key=lambda item: item[1])[0]
    if scores[best] == 0:
        return "m1"
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
    legacy_keyword_w = float(weights.get("keyword", 0.20))
    legacy_semantic_w = float(weights.get("semantic", 0.20))
    nlp_w = float(weights.get("nlp", legacy_keyword_w + legacy_semantic_w))
    freshness_w = float(weights.get("freshness", 0.10))
    company_w = float(weights.get("company", 0.10))
    source_w = float(weights.get("source", 0.0))
    normalized_final_weights = _normalize_weight_map(
        {"rule": rule_w, "nlp": nlp_w, "freshness": freshness_w, "company": company_w, "source": source_w},
        {"rule": 0.40, "nlp": 0.40, "freshness": 0.10, "company": 0.10, "source": 0.0},
    )
    rule_w = normalized_final_weights["rule"]
    nlp_w = normalized_final_weights["nlp"]
    freshness_w = normalized_final_weights["freshness"]
    company_w = normalized_final_weights["company"]
    source_w = normalized_final_weights["source"]

    nlp_cfg = scoring_cfg.get("nlp", {}) if isinstance(scoring_cfg.get("nlp", {}), dict) else {}
    explicit_nlp_component_cfg = any(
        key in nlp_cfg for key in ["lexical_weight", "semantic_weight", "intent_weight"]
    )
    if explicit_nlp_component_cfg or "nlp" in weights:
        nlp_component_weights = _normalize_weight_map(
            {
                "lexical": float(nlp_cfg.get("lexical_weight", 0.45)),
                "semantic": float(nlp_cfg.get("semantic_weight", 0.40)),
                "intent": float(nlp_cfg.get("intent_weight", 0.15)),
            },
            {"lexical": 0.45, "semantic": 0.40, "intent": 0.15},
        )
    else:
        legacy_total = legacy_keyword_w + legacy_semantic_w
        if legacy_total <= 0:
            legacy_total = 1.0
        nlp_component_weights = _normalize_weight_map(
            {
                "lexical": legacy_keyword_w / legacy_total,
                "semantic": legacy_semantic_w / legacy_total,
                "intent": 0.0,
            },
            {"lexical": 0.50, "semantic": 0.50, "intent": 0.0},
        )

    semantic_cfg = scoring_cfg.get("semantic", {})
    semantic_enabled = bool(semantic_cfg.get("enabled", True))
    model_name = str(semantic_cfg.get("model_name", "sentence-transformers/all-MiniLM-L6-v2"))
    device = select_torch_device()
    logger.info("Semantic runtime device: %s", device)
    logger.info(
        "Scoring weights: rule=%.3f nlp=%.3f freshness=%.3f company=%.3f source=%.3f | nlp_components=%s",
        rule_w,
        nlp_w,
        freshness_w,
        company_w,
        source_w,
        nlp_component_weights,
    )

    model = None
    if semantic_enabled and nlp_component_weights.get("semantic", 0.0) > 0:
        model = _safe_load_model(model_name=model_name, device=device, logger=logger)

    profile_text = _build_profile_text(root, skills_cfg)
    semantic_texts = [f"{row.title} {row.description}" for row in df.itertuples(index=False)]
    semantic_scores = _compute_semantic_scores(semantic_texts, profile_text, model, logger)
    if len(semantic_scores) != len(df):
        semantic_scores = [0.0 for _ in range(len(df))]

    search_tracks = _collect_search_tracks(search_cfg)
    company_tiers, unknown_company_score = _build_company_tiers(target_cfg, scoring_cfg=scoring_cfg)

    rule_scores: list[float] = []
    keyword_scores: list[float] = []
    intent_scores: list[float] = []
    nlp_scores: list[float] = []
    freshness_scores: list[float] = []
    company_scores: list[float] = []
    source_scores: list[float] = []
    intern_flags: list[bool] = []
    canada_flags: list[bool] = []
    senior_flags: list[bool] = []
    matched_tracks: list[str] = []
    recommendations: list[str] = []

    for idx, row in enumerate(df.itertuples(index=False)):
        title = str(getattr(row, "title", "") or "")
        description = str(getattr(row, "description", "") or "")
        location = str(getattr(row, "location", "") or "")
        company = str(getattr(row, "company", "") or "")
        source = str(getattr(row, "source", "") or "")
        date_posted = str(getattr(row, "date_posted", "") or "")

        rule_score, is_intern_like, is_canada_like, is_senior_like = _compute_rule_score(
            title=title,
            description=description,
            location=location,
            search_cfg=search_cfg,
        )
        keyword_score = _compute_keyword_score(
            title=title,
            description=description,
            search_cfg=search_cfg,
            skills_cfg=skills_cfg,
            search_tracks=search_tracks,
        )
        intent_score, matched_track, resume_hint = _compute_intent_score(
            title=title,
            description=description,
            search_tracks=search_tracks,
        )
        semantic_score = float(semantic_scores[idx]) if idx < len(semantic_scores) else 0.0
        nlp_score = (
            keyword_score * nlp_component_weights.get("lexical", 0.0)
            + semantic_score * nlp_component_weights.get("semantic", 0.0)
            + intent_score * nlp_component_weights.get("intent", 0.0)
        )
        freshness_score = _compute_freshness_score(date_posted=date_posted, run_date=run_date)
        company_score = _compute_company_score(
            company=company,
            company_tiers=company_tiers,
            unknown_score=unknown_company_score,
        )
        source_score = _compute_source_score(source=source, scoring_cfg=scoring_cfg)
        recommendation = _recommend_resume(title=title, description=description, resume_hint=resume_hint)

        rule_scores.append(rule_score)
        keyword_scores.append(keyword_score)
        intent_scores.append(intent_score)
        nlp_scores.append(min(1.0, max(0.0, nlp_score)))
        freshness_scores.append(freshness_score)
        company_scores.append(company_score)
        source_scores.append(source_score)
        intern_flags.append(is_intern_like)
        canada_flags.append(is_canada_like)
        senior_flags.append(is_senior_like)
        matched_tracks.append(matched_track)
        recommendations.append(recommendation)

    df["rule_score"] = rule_scores
    df["keyword_score"] = keyword_scores
    df["semantic_score"] = semantic_scores
    df["intent_score"] = intent_scores
    df["nlp_score"] = nlp_scores
    df["freshness_score"] = freshness_scores
    df["company_score"] = company_scores
    df["source_score"] = source_scores
    df["is_intern_like"] = intern_flags
    df["is_canada_like"] = canada_flags
    df["is_senior_like"] = senior_flags
    df["matched_track"] = matched_tracks
    df["recommended_resume"] = recommendations

    df["final_score"] = (
        df["rule_score"] * rule_w
        + df["nlp_score"] * nlp_w
        + df["freshness_score"] * freshness_w
        + df["company_score"] * company_w
        + df["source_score"] * source_w
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
