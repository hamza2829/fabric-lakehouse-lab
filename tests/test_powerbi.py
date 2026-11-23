"""Power BI project (PBIP): TMDL semantic model and PBIR report, checked statically."""
from __future__ import annotations

import json
import re

import pandas as pd
import pytest

from fabric_local import paths
from fabric_local.dax import DaxError, Evaluator
from fabric_local.tmdl import parse_tmdl

PBI = paths.POWERBI_DIR
EXPECTED_MEASURES = {
    "Order Lines", "Net Sales EUR", "Delivered Lines", "On-Time Lines", "On-Time Delivery %",
    "Units Produced", "Scrap Units", "Scrap Rate %", "Availability %", "Performance %",
    "Quality Rate %", "OEE %", "Complaints", "Complaint Rate %", "Inspection Pass Rate %",
}
GOLD_MODEL_TABLES = {"dim_date", "dim_material", "dim_supplier", "dim_machine",
                     "fact_orders", "fact_production", "fact_inspections", "fact_complaints"}


def test_model_has_exactly_the_15_kpi_measures(model):
    assert set(model.measures) == EXPECTED_MEASURES
    assert sum(len(t.measures) for t in model.tables.values()) == 15


def test_measures_are_documented_and_formatted(model):
    for m in model.measures.values():
        assert m.props.get("formatString"), m.name
        assert m.props.get("displayFolder") in {"Delivery", "Production", "OEE", "Quality"}, m.name
        assert m.description, m.name
        if m.name.endswith("%"):
            assert "%" in m.props["formatString"], m.name


def test_measure_references_resolve(model):
    for m in model.measures.values():
        for table, col in m.column_refs():
            assert table in model.tables and col in model.tables[table].columns, f"{m.name}: {table}[{col}]"
        for ref in m.measure_refs():
            assert ref in model.measures, f"{m.name}: [{ref}]"
        assert m.name not in m.measure_refs()


def test_ratio_measures_use_divide(model):
    for m in model.measures.values():
        if m.name.endswith("%") and m.name != "OEE %":
            assert m.expression.startswith("DIVIDE"), m.name
            assert "/" not in m.expression, m.name


def test_tables_are_direct_lake_partitions_over_gold(model):
    assert set(model.tables) == GOLD_MODEL_TABLES
    expressions = (PBI / "ManufacturingKPIs.SemanticModel/definition/expressions.tmdl").read_text(encoding="utf-8")
    assert "expression DatabaseQuery" in expressions
    for t in model.tables.values():
        (part,) = t.partitions
        assert part.kind == "entity" and part.props["mode"] == "directLake"
        assert part.props["entityName"] == t.name and part.props["expressionSource"] == "DatabaseQuery"
        for c in t.columns.values():
            assert c.data_type in {"string", "int64", "double", "dateTime", "boolean"}, f"{t.name}.{c.name}"
            assert c.source_column == c.name


def test_relationships_form_a_star(model):
    pairs = [(r.from_table, r.from_column, r.to_table, r.to_column) for r in model.relationships]
    assert len(pairs) == len(set(pairs)) == 11
    for ft, fc, tt, tc in pairs:
        assert fc in model.tables[ft].columns and tc in model.tables[tt].columns
        assert tt.startswith("dim_"), "the one side is always a dimension"
        assert model.tables[ft].columns[fc].data_type == model.tables[tt].columns[tc].data_type
    facts = {ft for ft, *_ in pairs if ft.startswith("fact_")}
    assert facts == {t for t in GOLD_MODEL_TABLES if t.startswith("fact_")}
    for fact in facts:
        assert (fact, "dim_date") in {(ft, tt) for ft, _, tt, _ in pairs}, f"{fact} needs a date relationship"


def test_date_table_is_marked(model):
    d = model.tables["dim_date"]
    assert d.props.get("dataCategory") == "Time"
    assert d.columns["date"].props.get("isKey") is True
    assert d.columns["month_name"].props.get("sortByColumn") == "month"


def test_report_visuals_reference_model_fields(model):
    visuals = sorted((PBI / "ManufacturingKPIs.Report/definition/pages").rglob("visual.json"))
    assert len(visuals) >= 15
    used = set()
    for v in visuals:
        doc = json.loads(v.read_text(encoding="utf-8"))
        for role in doc["visual"]["query"]["queryState"].values():
            for proj in role["projections"]:
                (kind, f), = proj["field"].items()
                table, prop = f["Expression"]["SourceRef"]["Entity"], f["Property"]
                if kind == "Measure":
                    assert prop in model.tables[table].measures, f"{v}: {table}.[{prop}]"
                    used.add(prop)
                else:
                    assert prop in model.tables[table].columns, f"{v}: {table}[{prop}]"
    assert used == EXPECTED_MEASURES - {"Order Lines", "Delivered Lines", "On-Time Lines"}


def test_project_files_point_at_each_other():
    pbip = json.loads((PBI / "ManufacturingKPIs.pbip").read_text(encoding="utf-8"))
    report_dir = PBI / pbip["artifacts"][0]["report"]["path"]
    pbir = json.loads((report_dir / "definition.pbir").read_text(encoding="utf-8"))
    sm_dir = (report_dir / pbir["datasetReference"]["byPath"]["path"]).resolve()
    assert (sm_dir / "definition.pbism").exists()
    pages = json.loads((report_dir / "definition/pages/pages.json").read_text(encoding="utf-8"))
    for p in pages["pageOrder"]:
        assert (report_dir / "definition/pages" / p / "page.json").exists()


def test_kpi_names_match_gold_notebook():
    """gold_kpi_summary rows (Spark SQL) and DAX measures share their names one to one."""
    text = (paths.NOTEBOOKS_DIR / "nb_03_gold_kpis.Notebook/notebook-content.py").read_text(encoding="utf-8")
    block = text[text.index("stack(15,"):text.index("AS (kpi_name")]
    names = set(re.findall(r"# MAGIC\s+'([^']+)',", block))
    assert names == EXPECTED_MEASURES


def test_tmdl_parser_reads_multiline_measures_and_flags():
    text = "table t\n\tmeasure 'A %' =\n\t\t\tVAR x = 1\n\t\t\tRETURN x\n\t\tformatString: 0%\n\n\tcolumn c\n\t\tdataType: int64\n\t\tisHidden\n"
    (t,), rels, _ = parse_tmdl(text)
    assert t.measures["A %"].expression == "VAR x = 1\nRETURN x"
    assert t.measures["A %"].props["formatString"] == "0%"
    assert t.columns["c"].props == {"dataType": "int64", "isHidden": True}
    assert rels == []


def test_dax_evaluator_on_a_tiny_model(model):
    frames = {
        "fact_orders": pd.DataFrame({"net_value_eur": [10.0, 20, 30], "is_delivered": [True, True, False],
                                     "is_on_time": pd.array([True, False, None], dtype="boolean")}),
        "fact_production": pd.DataFrame({"units_produced": [100, 200], "scrap_units": [2, 4], "good_units": [98, 196],
                                         "run_time_min": [400.0, 420], "planned_runtime_min": [480.0, 480],
                                         "ideal_run_time_min": [350.0, 380]}),
        "fact_complaints": pd.DataFrame({"complaint_id": ["c1"]}),
        "fact_inspections": pd.DataFrame({"is_pass": [True, False, True, True]}),
    }
    ev = Evaluator(model, frames)
    assert ev.measure("On-Time Delivery %") == pytest.approx(0.5)
    assert ev.measure("Complaint Rate %") == pytest.approx(0.5)
    assert ev.measure("Inspection Pass Rate %") == pytest.approx(0.75)
    assert ev.measure("OEE %") == pytest.approx((820 / 960) * (730 / 820) * (294 / 300))
    with pytest.raises(DaxError):
        ev.evaluate("SUMX ( fact_orders, fact_orders[net_value_eur] )")
