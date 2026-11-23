"""Documentation stays in step with the model and the pipelines."""
from __future__ import annotations

import re

from fabric_local import paths

DOCS = paths.REPO_ROOT / "docs"


def test_data_dictionary_covers_every_model_table_and_column(model):
    text = (DOCS / "data_dictionary.md").read_text(encoding="utf-8")
    for t in model.tables.values():
        assert f"### {t.name} " in text, t.name
        section = text.split(f"### {t.name} ", 1)[1].split("\n### ", 1)[0]
        for c in t.columns:
            assert re.search(rf"\b{re.escape(c)}\b", section), f"{t.name}.{c} missing in data_dictionary.md"


def test_kpi_definitions_list_every_measure(model):
    text = (DOCS / "kpi_definitions.md").read_text(encoding="utf-8")
    for name in model.measures:
        assert f"| {name} |" in text, name


def test_architecture_diagram_names_every_item():
    text = (DOCS / "architecture.md").read_text(encoding="utf-8")
    assert "```mermaid" in text
    for name in ("pl_lakehouse_daily", "pl_ingest_sources", "nb_01_bronze_ingest",
                 "nb_02_silver_clean_conform", "nb_03_gold_kpis", "lh_manufacturing", "ManufacturingKPIs"):
        assert name in text, name


def test_readme_has_honest_section_and_results_markers():
    text = (paths.REPO_ROOT / "README.md").read_text(encoding="utf-8")
    assert "<!-- results:start -->" in text and "<!-- results:end -->" in text
    assert "## Honest notes" in text
    for phrase in ("synthetic", "no live Fabric tenant"):
        assert phrase in text.lower() or phrase in text, phrase
