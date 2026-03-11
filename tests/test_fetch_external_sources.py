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
                "company_careers": {"enabled": False},
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


def test_fetch_external_sources_collects_company_career_links(tmp_path, monkeypatch):
    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "raw" / "alerts").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "profile").mkdir(parents=True, exist_ok=True)
    (tmp_path / "logs").mkdir(parents=True, exist_ok=True)

    _write_yaml(
        tmp_path / "config" / "sources.yaml",
        {
            "external_sources": {
                "enabled": True,
                "github_watch": {"enabled": False, "repos": []},
                "internee": {"enabled": False, "url": "https://internee.ca"},
                "company_careers": {
                    "enabled": True,
                    "config_path": "config/company_careers.yaml",
                    "max_links_per_company": 10,
                },
            }
        },
    )
    _write_yaml(tmp_path / "data" / "profile" / "target_companies.yaml", {"tier_a": ["Acme"], "tier_b": []})
    _write_yaml(
        tmp_path / "config" / "company_careers.yaml",
        {
            "companies": [
                {
                    "company": "Acme",
                    "career_url": "https://careers.acme.com/jobs",
                    "allowed_domains": ["careers.acme.com"],
                }
            ]
        },
    )

    def fake_fetch_text(url: str, timeout: int = 20):
        return """
        <html>
          <body>
            <a href="/jobs/software-engineer-intern">Software Engineer Intern</a>
            <a href="/careers">Careers Home</a>
            <a href="https://external.example.com/jobs/1">Offsite</a>
          </body>
        </html>
        """

    monkeypatch.setattr("scripts.fetch_external_sources._fetch_text", fake_fetch_text)
    result = fetch_external_sources(base_dir=tmp_path, run_date=date(2026, 3, 8))

    assert result["count"] == 1
    assert result["source_summary"]["company_careers:Acme"] == 1

    rows = _read_jsonl(tmp_path / "data" / "raw" / "alerts" / "external_sources_20260308.jsonl")
    assert rows[0]["company"] == "Acme"
    assert rows[0]["title"] == "Software Engineer Intern"
    assert rows[0]["job_url"] == "https://careers.acme.com/jobs/software-engineer-intern"
    assert rows[0]["source"] == "company_careers:Acme"


def test_fetch_external_sources_follows_secondary_lever_board(tmp_path, monkeypatch):
    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "raw" / "alerts").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "profile").mkdir(parents=True, exist_ok=True)
    (tmp_path / "logs").mkdir(parents=True, exist_ok=True)

    _write_yaml(
        tmp_path / "config" / "sources.yaml",
        {
            "external_sources": {
                "enabled": True,
                "github_watch": {"enabled": False, "repos": []},
                "internee": {"enabled": False, "url": "https://internee.ca"},
                "company_careers": {
                    "enabled": True,
                    "config_path": "config/company_careers.yaml",
                    "max_links_per_company": 10,
                },
            }
        },
    )
    _write_yaml(tmp_path / "data" / "profile" / "target_companies.yaml", {"tier_a": ["Acme"], "tier_b": []})
    _write_yaml(
        tmp_path / "config" / "company_careers.yaml",
        {
            "companies": [
                {
                    "company": "Acme",
                    "career_url": "https://careers.acme.com",
                    "allowed_domains": ["careers.acme.com", "jobs.lever.co"],
                }
            ]
        },
    )

    def fake_fetch_text(url: str, timeout: int = 20):
        if "api.lever.co" in url:
            return """
            [
              {
                "text": "Backend Engineer Intern",
                "hostedUrl": "https://jobs.lever.co/acme/123",
                "descriptionPlain": "intern role",
                "createdAt": 1772755200000,
                "categories": {"location": "Toronto, Ontario"}
              }
            ]
            """
        return '<a href="https://jobs.lever.co/acme">Open roles</a>'

    monkeypatch.setattr("scripts.fetch_external_sources._fetch_text", fake_fetch_text)
    result = fetch_external_sources(base_dir=tmp_path, run_date=date(2026, 3, 8))

    assert result["count"] == 1
    assert result["source_summary"]["company_careers:Acme"] == 1

    rows = _read_jsonl(tmp_path / "data" / "raw" / "alerts" / "external_sources_20260308.jsonl")
    assert rows[0]["title"] == "Backend Engineer Intern"
    assert rows[0]["location"] == "Toronto, Ontario"
    assert rows[0]["job_url"] == "https://jobs.lever.co/acme/123"
