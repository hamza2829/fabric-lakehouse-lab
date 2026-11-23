"""The synthetic source generator: deterministic, complete, and with the promised defects."""
from __future__ import annotations

import csv
import json

import pandas as pd

from data.generate_data import AS_OF, generate


def _read(out, rel) -> pd.DataFrame:
    p = out / rel
    if p.suffix == ".csv":
        return pd.read_csv(p, dtype=str, keep_default_na=False).replace("", None)
    if p.suffix == ".jsonl":
        return pd.DataFrame([json.loads(line) for line in p.read_text(encoding="utf-8").splitlines()])
    return pd.DataFrame(json.loads(p.read_text(encoding="utf-8")))


def test_same_seed_gives_identical_files(generated, tmp_path):
    _, first = generated
    second = generate(tmp_path, 42)
    assert {k: v["sha256"] for k, v in first["tables"].items()} == {k: v["sha256"] for k, v in second["tables"].items()}


def test_other_seed_gives_other_data(generated, tmp_path):
    _, first = generated
    other = generate(tmp_path, 7)
    assert first["tables"]["orders"]["sha256"] != other["tables"]["orders"]["sha256"]


def test_three_sources_in_csv_and_json(generated):
    out, manifest = generated
    files = {t["file"] for t in manifest["tables"].values()}
    assert {f.split("/")[0] for f in files} == {"erp", "production", "quality"}
    assert any(f.endswith(".csv") for f in files)
    assert any(f.endswith(".jsonl") for f in files)
    assert any(f.endswith(".json") for f in files)
    json.loads((out / "quality" / "complaints.json").read_text(encoding="utf-8"))  # a JSON array


def test_manifest_row_counts_match_files(generated):
    out, manifest = generated
    for name, t in manifest["tables"].items():
        assert len(_read(out, t["file"])) == t["rows_written"], name


def test_expected_silver_rows_add_up(generated):
    """rows written = silver rows + rejected rows + duplicate copies, per entity."""
    _, manifest = generated
    rejecting = ("missing_key", "orphan_fk", "out_of_range", "future_date")
    for name, t in manifest["tables"].items():
        inj = t["injected"]
        rejected = sum(inj.get(k, 0) for k in rejecting)
        assert t["rows_written"] == t["expected_silver_rows"] + rejected + inj.get("duplicates", 0), name


def test_orders_contain_duplicates_and_missing_keys(generated):
    out, manifest = generated
    orders = _read(out, "erp/orders.csv")
    keyed = orders.dropna(subset=["order_id"])
    assert keyed.duplicated(["order_id", "order_line"]).sum() == manifest["tables"]["orders"]["injected"]["duplicates"]
    assert orders["material_id"].isna().sum() + orders["order_id"].isna().sum() == 35


def test_unit_mismatches_present(generated):
    out, _ = generated
    assert {"PC", "PCS", "pc", "Stk", "DZ"} <= set(_read(out, "erp/orders.csv")["unit"])
    assert set(_read(out, "production/machine_runs.csv")["time_unit"]) == {"min", "h"}
    assert set(_read(out, "production/scrap_events.jsonl")["unit"]) == {"PC", "KG"}


def test_late_and_future_records_present(generated):
    out, manifest = generated
    scrap = _read(out, "production/scrap_events.jsonl")
    lag = (pd.to_datetime(scrap["recorded_at"]) - pd.to_datetime(scrap["event_ts"])).dt.total_seconds() / 3600
    assert (lag > 48).sum() == manifest["tables"]["scrap_events"]["injected"]["late_arrival"]
    orders = _read(out, "erp/orders.csv")
    assert (pd.to_datetime(orders["order_date"]) > AS_OF).sum() == manifest["tables"]["orders"]["injected"]["future_date"]


def test_complaints_point_at_delivered_orders(generated):
    out, _ = generated
    orders = _read(out, "erp/orders.csv")
    compl = _read(out, "quality/complaints.json").dropna(subset=["order_id"])
    delivered = set(zip(orders.loc[orders["delivered_date"].notna(), "order_id"],
                        orders.loc[orders["delivered_date"].notna(), "order_line"].astype(int)))
    hits = [(o, int(line)) in delivered for o, line in zip(compl["order_id"], compl["order_line"])]
    assert sum(not h for h in hits) == 5  # exactly the injected orphans


def test_csv_files_have_headers_and_utf8(generated):
    out, _ = generated
    with open(out / "erp" / "suppliers.csv", encoding="utf-8") as fh:
        header = next(csv.reader(fh))
    assert header[0] == "supplier_id"
