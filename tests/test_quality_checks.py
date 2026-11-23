"""Data quality checks, rules file and gate."""
from __future__ import annotations

import json
from datetime import datetime

import pandas as pd
import pytest

from quality import checks as C
from quality.run_quality import ISSUE_ORDER, QualityRun, evaluate_gate, load_rules, run_check

AS_OF = datetime(2026, 9, 30, 23, 59, 59)


def test_not_null_counts_null_and_blank():
    df = pd.DataFrame({"a": ["x", None, " ", "y"], "b": ["1", "2", "3", None]})
    assert C.not_null(df, ["a", "b"])[0] == 3


def test_unique_ignores_rows_with_null_key():
    df = pd.DataFrame({"k": ["a", "a", "b", None, None], "l": ["1", "1", "1", "1", "1"]})
    assert C.unique(df, ["k", "l"])[0] == 1


def test_range_handles_text_exclusive_bounds_and_max():
    df = pd.DataFrame({"q": ["5", "0", "-1", "abc", None, "11"]})
    assert C.value_range(df, "q", 0, 10, exclusive_min=True)[0] == 4   # 0, -1, abc, 11


def test_compare_two_columns():
    df = pd.DataFrame({"defects": ["1", "60", "5"], "sample": ["50", "50", None]})
    assert C.compare(df, "defects", "<=", "sample")[0] == 1


def test_allowed_values():
    df = pd.DataFrame({"unit": ["PC", "PCS", "pc", None]})
    assert C.allowed_values(df, "unit", ["PC"])[0] == 2


def test_referential_with_composite_key():
    child = pd.DataFrame({"o": ["A", "A", "B", None], "l": ["10", "20", "10", "10"]})
    parent = pd.DataFrame({"o": ["A", "B"], "l": [10, 10]})
    failed, _, examples = C.referential(child, ["o", "l"], parent, ["o", "l"])
    assert failed == 1 and examples[0] == {"o": "A", "l": "20"}


def test_not_in_future_and_late_arrival():
    df = pd.DataFrame({"event": ["2026-09-01T00:00:00", "2026-10-02T00:00:00"],
                       "recorded": ["2026-09-04T00:00:00", "2026-10-02T01:00:00"]})
    assert C.not_in_future(df, "event", AS_OF)[0] == 1
    assert C.late_arrival(df, "recorded", "event", 48)[0] == 1


def test_freshness_stale_and_fresh():
    stale = pd.DataFrame({"t": ["2026-09-25T00:00:00"]})
    fresh = pd.DataFrame({"t": ["2026-09-30T20:00:00", "2026-11-01T00:00:00"]})  # future rows ignored
    assert C.freshness(stale, "t", AS_OF, 72)[0] == 1
    failed, detail, _ = C.freshness(fresh, "t", AS_OF, 24)
    assert failed == 0 and "age 4.0 h" in detail


def test_rules_file_is_consistent():
    rules = load_rules()
    known = {"not_null", "unique", "range", "compare", "allowed_values", "referential",
             "not_in_future", "late_arrival", "freshness"}
    names = {e["name"] for e in rules["entity"]}
    for e in rules["entity"]:
        assert e["source"] in {"erp", "production", "quality"}
        assert e["bronze"].startswith("bronze_") and e["silver"].startswith("silver_")
        for c in e["checks"]:
            assert c["type"] in known
            assert c["issue"] in ISSUE_ORDER
            assert c["issue"] in rules["severity"]["bronze"] and c["issue"] in rules["severity"]["silver"]
            if c["type"] == "referential":
                assert c["ref"] in names
    assert 0 < rules["gate"]["max_bronze_issue_rate"] < 1


def _qr(rate_failed: int, silver_failed: int) -> QualityRun:
    qr = QualityRun("2026-09-30T23:59:59", "now")
    qr.row_counts = {("bronze", "orders"): 100, ("silver", "orders"): 90}
    qr.results = [
        C.CheckResult("unique", "orders", "erp", "bronze", "duplicate", "error", rate_failed, 100),
        C.CheckResult("unique", "orders", "erp", "silver", "duplicate", "error", silver_failed, 90),
        C.CheckResult("late_arrival", "orders", "erp", "silver", "late_arrival", "info", 7, 90),
    ]
    return qr


def test_gate_passes_under_threshold_and_fails_over_it():
    gate = {"max_bronze_issue_rate": 0.10, "max_silver_errors": 0, "fail_on_freshness": False}
    assert evaluate_gate(_qr(5, 0), gate)["passed"]
    over = evaluate_gate(_qr(15, 0), gate)
    assert not over["passed"] and "erp" in over["reasons"][0]
    silver = evaluate_gate(_qr(5, 1), gate)
    assert not silver["passed"] and "silver" in silver["reasons"][0]


def _raw_frames(out, manifest):
    def read(rel):
        p = out / rel
        if p.suffix == ".csv":
            return pd.read_csv(p, dtype=str, keep_default_na=False).replace("", None)
        rows = [json.loads(x) for x in p.read_text(encoding="utf-8").splitlines()] if p.suffix == ".jsonl" \
            else json.loads(p.read_text(encoding="utf-8"))
        return pd.DataFrame([{k: None if v is None else str(v) for k, v in r.items()} for r in rows])
    return {name: read(t["file"]) for name, t in manifest["tables"].items()}


ISSUE_FROM_MANIFEST = {"missing_key": "missing_key", "duplicate": "duplicates", "orphan_fk": "orphan_fk",
                       "out_of_range": "out_of_range", "future_date": "future_date",
                       "unit_mismatch": "unit_mismatch", "late_arrival": "late_arrival"}


@pytest.mark.parametrize("entity", ["suppliers", "materials", "orders", "machines", "machine_runs",
                                    "scrap_events", "inspections", "complaints"])
def test_bronze_rules_find_exactly_the_injected_defects(generated, entity):
    """Run the bronze rules on the raw files: every injected defect is found, nothing else."""
    out, manifest = generated
    rules = load_rules()
    frames = _raw_frames(out, manifest)
    spec = next(e for e in rules["entity"] if e["name"] == entity)
    found = {}
    for c in spec["checks"]:
        if "bronze" in c.get("layers", ["bronze", "silver"]) and c["type"] != "freshness":
            found[c["issue"]] = found.get(c["issue"], 0) + run_check(c, frames[entity], frames, AS_OF)[0]
    injected = manifest["tables"][entity]["injected"]
    expected = {k: injected.get(v, 0) for k, v in ISSUE_FROM_MANIFEST.items() if k in found}
    assert found == expected


def test_complaints_feed_is_stale_on_purpose(generated):
    out, manifest = generated
    frames = _raw_frames(out, manifest)
    failed, detail, _ = C.freshness(frames["complaints"], "received_at", AS_OF, 72)
    assert failed == 1, detail
