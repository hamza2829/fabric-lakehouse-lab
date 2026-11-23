"""End to end: the lakehouse built by the pipeline (bronze, silver, gold) and its reports.

These tests need ``python pipelines/run.py pl_lakehouse_daily`` and
``python -m quality.run_quality`` to have run; CI sets REQUIRE_LAKEHOUSE=1.
"""
from __future__ import annotations

import json

import pandas as pd
import pyarrow as pa
import pytest

from fabric_local import paths
from fabric_local.dax import Evaluator

pytestmark = pytest.mark.lakehouse

ENTITIES = {
    "suppliers": ("bronze_erp_suppliers", "silver_suppliers"),
    "materials": ("bronze_erp_materials", "silver_materials"),
    "orders": ("bronze_erp_orders", "silver_orders"),
    "machines": ("bronze_prod_machines", "silver_machines"),
    "machine_runs": ("bronze_prod_machine_runs", "silver_machine_runs"),
    "scrap_events": ("bronze_prod_scrap_events", "silver_scrap_events"),
    "inspections": ("bronze_qm_inspections", "silver_inspections"),
    "complaints": ("bronze_qm_complaints", "silver_complaints"),
}
REJECT_REASONS = ("missing_key", "orphan_fk", "out_of_range", "future_date")


def _stats(delta) -> dict:
    return {r["entity"]: r for r in delta("silver_load_stats").to_dict("records")}


def test_bronze_keeps_every_raw_row_as_text(delta, repo_manifest):
    for entity, (bronze, _) in ENTITIES.items():
        df = delta(bronze)
        assert len(df) == repo_manifest["tables"][entity]["rows_written"], entity
        assert {"_source_file", "_ingested_at", "_batch_id", "_load_date"} <= set(df.columns)
        business = [c for c in df.columns if not c.startswith("_")]
        assert all(pd.api.types.is_string_dtype(df[c]) or df[c].dtype == object for c in business), entity


def test_silver_row_counts_match_expectation(delta, repo_manifest):
    for entity, (_, silver) in ENTITIES.items():
        assert len(delta(silver)) == repo_manifest["tables"][entity]["expected_silver_rows"], entity


def test_load_stats_reconcile(delta):
    for entity, s in _stats(delta).items():
        assert s["bronze_rows"] == s["quarantined"] + s["duplicates_removed"] + s["silver_rows"], entity


def test_quarantine_reasons_match_injected_defects(delta, repo_manifest):
    q = delta("silver_quarantine").groupby(["entity", "dq_reason"]).size().to_dict()
    expected = {(e, r): n for e, t in repo_manifest["tables"].items()
                for r, n in t["injected"].items() if r in REJECT_REASONS}
    assert q == expected
    assert delta("silver_quarantine")["record_json"].map(json.loads).map(bool).all()


def test_duplicates_late_and_unit_fixes_match_injected(delta, repo_manifest):
    stats = _stats(delta)
    for entity, t in repo_manifest["tables"].items():
        inj = t["injected"]
        assert stats[entity]["duplicates_removed"] == inj.get("duplicates", 0), entity
        assert stats[entity]["late_flagged"] == inj.get("late_arrival", 0), entity
        assert stats[entity]["units_converted"] == inj.get("unit_mismatch", 0), entity


def test_silver_keys_are_unique_and_complete(delta):
    keys = {"silver_orders": ["order_id", "order_line"], "silver_machine_runs": ["run_id"],
            "silver_scrap_events": ["scrap_id"], "silver_inspections": ["inspection_id"],
            "silver_complaints": ["complaint_id"], "silver_materials": ["material_id"],
            "silver_suppliers": ["supplier_id"]}
    for table, cols in keys.items():
        df = delta(table)
        assert df[cols].notna().all().all(), table
        assert not df.duplicated(cols).any(), table


def test_unit_conversion_recovers_true_quantities(delta):
    orders = delta("silver_orders")
    dz = orders[orders["source_unit"] == "DZ"]
    assert len(dz) == 30 and (dz["quantity_pc"] == dz["source_quantity"] * 12).all()
    # net value was booked on the true quantity, so price x converted quantity must reproduce it
    assert ((orders["quantity_pc"] * orders["unit_price_eur"] - orders["net_value_eur"]).abs() < 0.01).all()
    scrap = delta("silver_scrap_events").merge(delta("silver_materials")[["material_id", "net_weight_kg"]], on="material_id")
    kg = scrap[scrap["source_unit"] == "KG"]
    assert len(kg) == 80
    assert ((kg["scrap_qty_pc"] * kg["net_weight_kg"] - kg["source_qty"]).abs() < 0.001).all()
    runs = delta("silver_machine_runs")
    assert set(runs["planned_runtime_min"]) <= {450.0, 480.0}


def _arrow_to_tmdl(t: pa.DataType) -> str:
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "string"
    if pa.types.is_integer(t):
        return "int64"
    if pa.types.is_floating(t):
        return "double"
    if pa.types.is_date(t) or pa.types.is_timestamp(t):
        return "dateTime"
    if pa.types.is_boolean(t):
        return "boolean"
    return str(t)


def test_tmdl_columns_match_gold_delta_schema(model):
    from deltalake import DeltaTable

    for t in model.tables.values():
        schema = DeltaTable(str(paths.table_path(t.name))).schema().to_pyarrow()
        gold = {f.name: _arrow_to_tmdl(f.type) for f in schema}
        model_cols = {c.source_column: c.data_type for c in t.columns.values()}
        assert model_cols == gold, t.name


def test_relationships_hold_in_gold_data(model, delta):
    for r in model.relationships:
        one = delta(r.to_table)[r.to_column]
        many = delta(r.from_table)[r.from_column].dropna()
        assert one.is_unique, f"{r.to_table}.{r.to_column} must be unique"
        assert set(many) <= set(one), f"orphans in {r.from_table}.{r.from_column}"


def test_dax_measures_reproduce_spark_sql_kpis(model, delta):
    frames = {t: delta(t) for t in model.tables}
    ev = Evaluator(model, frames)
    gold = dict(zip(delta("gold_kpi_summary")["kpi_name"], delta("gold_kpi_summary")["kpi_value"]))
    assert set(gold) == set(model.measures)
    for name in model.measures:
        assert ev.measure(name) == pytest.approx(gold[name], rel=1e-9), name


def test_kpis_are_plausible(delta):
    k = dict(zip(delta("gold_kpi_summary")["kpi_name"], delta("gold_kpi_summary")["kpi_value"]))
    assert 0.75 < k["On-Time Delivery %"] < 0.95
    assert 0.005 < k["Scrap Rate %"] < 0.05
    assert 0.80 < k["Availability %"] < 0.98
    assert 0.75 < k["Performance %"] < 0.98
    assert k["OEE %"] == pytest.approx(k["Availability %"] * k["Performance %"] * k["Quality Rate %"])
    assert 0.5 < k["OEE %"] < 0.9
    assert 0 < k["Complaint Rate %"] < 0.05
    assert k["Quality Rate %"] == pytest.approx(1 - k["Scrap Rate %"])
    monthly = delta("gold_kpi_monthly")
    assert sorted(monthly["year_month"]) == [f"2026-{m:02d}" for m in range(4, 10)]
    assert monthly["order_lines"].sum() == k["Order Lines"]


def test_quality_gate_passed_with_clean_silver():
    report = paths.REPORTS_DIR / "quality_report.json"
    q = json.loads(report.read_text(encoding="utf-8"))
    assert q["gate"]["passed"] and q["gate"]["silver_errors"] == 0
    assert q["gate"]["stale"] == ["bronze.complaints", "silver.complaints"]
    md = (paths.REPORTS_DIR / "quality_report.md").read_text(encoding="utf-8")
    assert "## Summary per source" in md and "| erp |" in md and "| quality |" in md


def test_readme_results_match_the_build():
    from tools.render_results import README, current_block, render

    assert current_block(README.read_text(encoding="utf-8")) == render(), \
        "README results are stale: run python tools/render_results.py --write"
