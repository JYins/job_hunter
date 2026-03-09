from datetime import date
import json

import yaml

from scripts.fetch_external_sources import fetch_external_sources


def _write_yaml(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _read_jsonl(path):
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def test_fetch_external_sources_collects_github_and_internee(tmp_path, monkeypatch):
    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "raw" / "alerts").mkdir(parents=True, exist_ok=True)
    (tmp_path / "logs").mkdir(parents=True, exist_ok=True)

    _write_yaml(
        tmp_path / "config" / "sources.yaml",
        {
            "external_sources": {
                "enabled": True,
                "github_watch": {
                    "enabled": True,
                    "repos": ["foo/bar"],
                },
                "internee": {"enabled": True, "url": "https://internee.ca"},
            }
        },
    )

    def fake_fetch_text(url: str, timeout: int = 20):
        if "raw.githubusercontent.com" in url:
            return "[Role A](https://example.com/a)\n[Role B](https://example.com/b)\n[Internal](https://github.com/foo/bar)"
        return '<a href="/jobs/123">Job</a><a href="https://internee.ca/job/999">Job2</a>'

    monkeypatch.setattr("scripts.fetch_external_sources._fetch_text", fake_fetch_text)
    result = fetch_external_sources(base_dir=tmp_path, run_date=date(2026, 3, 8))

    assert result["count"] == 4
    assert result["source_summary"]["github_repo:foo/bar"] == 2
    assert result["source_summary"]["internee_ca"] == 2

    rows = _read_jsonl(tmp_path / "data" / "raw" / "alerts" / "external_sources_20260308.jsonl")
    sources = {row["source"] for row in rows}
    assert "github_repo:foo/bar" in sources
    assert "internee_ca" in sources
