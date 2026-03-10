from datetime import date

import yaml

from scripts.fetch_uwo_connect import fetch_uwo_connect


def _write_yaml(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def test_fetch_uwo_connect_disabled_writes_empty_file(tmp_path):
    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "raw" / "alerts").mkdir(parents=True, exist_ok=True)
    (tmp_path / "logs").mkdir(parents=True, exist_ok=True)

    _write_yaml(
        tmp_path / "config" / "sources.yaml",
        {
            "uwo_connect": {
                "enabled": False,
                "dashboard_url": "https://connect.uwo.ca/myAccount/dashboard.htm",
            }
        },
    )

    result = fetch_uwo_connect(base_dir=tmp_path, run_date=date(2026, 3, 9))
    assert result["count"] == 0
    output_path = tmp_path / "data" / "raw" / "alerts" / "uwo_connect_20260309.jsonl"
    assert output_path.exists()
    assert output_path.read_text(encoding="utf-8").strip() == ""
