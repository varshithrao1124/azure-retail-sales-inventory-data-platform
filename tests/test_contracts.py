"""ADF control metadata stays aligned with conf/sources.yml."""

import json
from pathlib import Path

import yaml


def test_adf_control_matches_source_catalog():
    root = Path(__file__).resolve().parents[1]
    sources = yaml.safe_load((root / "conf" / "sources.yml").read_text(encoding="utf-8"))["sources"]
    control = json.loads(
        (root / "adf" / "control" / "ingest_sources.json").read_text(encoding="utf-8")
    )
    assert [row["name"] for row in sources] == [row["sourceName"] for row in control]
    assert [row["file"] for row in sources] == [row["fileName"] for row in control]
    assert [row["load_type"] for row in sources] == [row["loadType"] for row in control]
