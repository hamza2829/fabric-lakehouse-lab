"""Seeded synthetic data generator for the manufacturing lakehouse lab.

Writes three source systems into ``data/raw/``:

* ``erp/``        suppliers.csv, materials.csv, orders.csv           (CSV)
* ``production/`` machines.csv, machine_runs.csv, scrap_events.jsonl (CSV + JSON lines)
* ``quality/``    inspections.jsonl, complaints.json                 (JSON lines + JSON array)

Every table is generated clean first. Errors are then injected on *disjoint*
row sets, so each defective row carries exactly one defect and the expected
outcome of the silver layer is known up front. The counts are written to
``data/raw/_manifest.json``, which the tests compare with what the lakehouse
actually detected, quarantined, flagged or repaired.

Only the Python standard library is used, and the same seed always produces
byte identical files.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
from datetime import date, datetime, timedelta
from pathlib import Path

DEFAULT_SEED = 42
START = date(2026, 4, 1)
END = date(2026, 9, 30)
AS_OF = datetime(2026, 9, 30, 23, 59, 59)
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT = REPO_ROOT / "data" / "raw"

COUNTRIES = ["DE", "DE", "DE", "PL", "CZ", "AT", "IT", "NL", "FR", "TR", "CN"]
NAME_A = ["Rhein", "Ruhr", "Nord", "Süd", "Alpen", "Elbe", "Main", "Weser", "Baltic", "Danube"]
NAME_B = ["Metall", "Polymer", "Stahl", "Fasteners", "Coatings", "Castings", "Precision", "Components"]
NAME_C = ["GmbH", "AG", "KG", "s.r.o.", "Sp. z o.o.", "S.p.A.", "B.V."]
FG_FAMILIES = ["Bracket", "Housing", "Flange", "Shaft", "Gear", "Pump Cover", "Valve Body", "Bearing Seat"]
RM_ITEMS = [("Steel sheet", "KG"), ("Aluminium bar", "KG"), ("Polymer granulate", "KG"),
            ("Copper wire", "M"), ("Hex bolt M8", "PC"), ("O-ring 40mm", "PC"),
            ("Powder coating", "KG"), ("Gasket", "PC"), ("Steel tube", "M")]
MACHINE_TYPES = ["CNC mill", "CNC lathe", "Injection moulder", "Press", "Laser cutter", "Assembly cell"]
SCRAP_REASONS = ["DIM_OUT_OF_TOL", "SURFACE_DEFECT", "MATERIAL_FLAW", "SETUP_SCRAP", "TOOL_WEAR", "HANDLING_DAMAGE"]
COMPLAINT_CATEGORIES = ["Dimension", "Surface", "Function", "Packaging", "Delivery"]
SEVERITIES = ["minor", "minor", "minor", "major", "major", "critical"]
ORDER_UNIT_VARIANTS = {"PCS": 60, "pc": 40, "Stk": 40}


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


class Picker:
    """Hands out disjoint row indices so every injected defect hits a different row."""

    def __init__(self, rng: random.Random, n: int):
        self.pool = list(range(n))
        rng.shuffle(self.pool)
        self.pos = 0

    def take(self, k: int) -> list[int]:
        out = self.pool[self.pos:self.pos + k]
        if len(out) < k:
            raise ValueError("not enough clean rows to inject defects")
        self.pos += k
        return sorted(out)

    def take_where(self, k: int, pred) -> list[int]:
        """Take k unused rows that satisfy ``pred``; skipped rows stay available."""
        rest = self.pool[self.pos:]
        chosen = [i for i in rest if pred(i)][:k]
        if len(chosen) < k:
            raise ValueError("not enough eligible rows to inject defects")
        chosen_set = set(chosen)
        self.pool = self.pool[:self.pos] + chosen + [i for i in rest if i not in chosen_set]
        self.pos += k
        return sorted(chosen)


def write_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow({k: ("" if r.get(k) is None else r.get(k)) for k in fields})


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False, sort_keys=False) + "\n")


def write_json_array(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        json.dump(rows, fh, ensure_ascii=False, indent=2)
        fh.write("\n")


def shuffle_tail(rng: random.Random, rows: list[dict]) -> list[dict]:
    """Source extracts are not sorted, so duplicates land in random positions."""
    rows = list(rows)
    rng.shuffle(rows)
    return rows


# --------------------------------------------------------------------------- ERP

def gen_suppliers(rng: random.Random):
    rows = []
    for i in range(1, 41):
        name = f"{rng.choice(NAME_A)} {rng.choice(NAME_B)} {rng.choice(NAME_C)}"
        rows.append({
            "supplier_id": f"S-{i:03d}",
            "supplier_name": name,
            "country": rng.choice(COUNTRIES),
            "rating": round(rng.uniform(2.5, 5.0), 1),
            "lead_time_days": rng.randint(5, 45),
            "extracted_at": iso(datetime(2026, 9, 30, 2, 0, 0)),
        })
    # three suppliers were re-extracted: an older version must lose to the newer one
    versions = []
    for idx in rng.sample(range(len(rows)), 3):
        old = dict(rows[idx])
        old["rating"] = round(max(1.0, rows[idx]["rating"] - 1.0), 1)
        old["extracted_at"] = iso(datetime(2026, 9, 1, 2, 0, 0))
        versions.append(old)
    injected = {"duplicates": len(versions)}
    return shuffle_tail(rng, rows + versions), rows, injected


def gen_materials(rng: random.Random, suppliers: list[dict]):
    rows = []
    for i in range(30):
        fam = FG_FAMILIES[i % len(FG_FAMILIES)]
        rows.append({
            "material_id": f"FG-{1001 + i}",
            "material_name": f"{fam} {chr(65 + i // len(FG_FAMILIES))}{i % 10}",
            "material_type": "FG",
            "material_group": fam,
            "base_unit": "PC",
            "net_weight_kg": round(rng.uniform(0.25, 3.0), 3),
            "standard_cost_eur": round(rng.uniform(4, 60), 2),
            "list_price_eur": 0.0,
            "supplier_id": None,
        })
        rows[-1]["list_price_eur"] = round(rows[-1]["standard_cost_eur"] * rng.uniform(1.25, 1.8), 2)
    for i in range(90):
        item, unit = RM_ITEMS[i % len(RM_ITEMS)]
        rows.append({
            "material_id": f"RM-{2001 + i}",
            "material_name": f"{item} {i // len(RM_ITEMS) + 1:02d}",
            "material_type": "RM",
            "material_group": item.split()[0],
            "base_unit": unit,
            "net_weight_kg": None if unit != "PC" else round(rng.uniform(0.005, 0.2), 3),
            "standard_cost_eur": round(rng.uniform(0.05, 12), 2),
            "list_price_eur": None,
            "supplier_id": rng.choice(suppliers)["supplier_id"],
        })
    clean = list(rows)
    out = list(rows)
    # orphan supplier references (unused raw materials, so nothing downstream depends on them)
    for i in range(4):
        out.append({**rows[30 + i], "material_id": f"RM-{2901 + i}", "supplier_id": f"S-{901 + i}"})
    # missing primary key
    for i in range(2):
        out.append({**rows[40 + i], "material_id": None})
    # exact duplicates
    dups = [dict(r) for r in rng.sample(clean, 3)]
    out += dups
    injected = {"orphan_fk": 4, "missing_key": 2, "duplicates": 3}
    return shuffle_tail(rng, out), clean, injected


def gen_orders(rng: random.Random, fg: list[dict]):
    rows = []
    n_order = 0
    for d in daterange(START, END):
        for _ in range(rng.randint(9, 17)):
            n_order += 1
            order_id = f"SO-2026-{n_order:06d}"
            customer = f"C-{rng.randint(1, 150):04d}"
            plant = rng.choice(["P100", "P100", "P200"])
            requested = d + timedelta(days=rng.randint(7, 21))
            for line in range(1, rng.randint(1, 4) + 1):
                mat = rng.choice(fg)
                qty = rng.choice([12, 24, 36, 48, 60, 96, 120, 240, 480])
                r = rng.random()
                if r < 0.86:
                    delivered = requested - timedelta(days=rng.randint(0, 3))
                else:
                    delivered = requested + timedelta(days=rng.randint(1, 10))
                delivered_s = delivered.isoformat() if delivered <= AS_OF.date() else None
                rows.append({
                    "order_id": order_id,
                    "order_line": line * 10,
                    "customer_id": customer,
                    "material_id": mat["material_id"],
                    "plant": plant,
                    "order_date": d.isoformat(),
                    "requested_date": requested.isoformat(),
                    "delivered_date": delivered_s,
                    "quantity": qty,
                    "unit": "PC",
                    "unit_price_eur": mat["list_price_eur"],
                    "net_value_eur": round(qty * mat["list_price_eur"], 2),
                    "currency": "EUR",
                    "extracted_at": iso(datetime.combine(d, datetime.min.time()) + timedelta(hours=23)),
                })
    clean = [dict(r) for r in rows]
    pick = Picker(rng, len(rows))
    out = rows
    bad = set()
    inj = {}

    idx = pick.take(25); bad.update(idx); inj["missing_key"] = 35
    for i in idx:
        out[i]["material_id"] = None
    idx = pick.take(10); bad.update(idx)
    for i in idx:
        out[i]["order_id"] = None
    idx = pick.take(20); bad.update(idx); inj["orphan_fk"] = 20
    for k, i in enumerate(idx):
        out[i]["material_id"] = f"FG-{9901 + k % 5}"
    idx = pick.take(15); bad.update(idx); inj["out_of_range"] = 15
    for i in idx:
        out[i]["quantity"] = -out[i]["quantity"]
        out[i]["net_value_eur"] = -out[i]["net_value_eur"]
    idx = pick.take(8); bad.update(idx); inj["future_date"] = 8
    for i in idx:
        fut = date(2026, 10, rng.randint(5, 28))
        out[i]["order_date"] = fut.isoformat()
        out[i]["requested_date"] = (fut + timedelta(days=14)).isoformat()
        out[i]["delivered_date"] = None
        out[i]["extracted_at"] = iso(AS_OF - timedelta(hours=3))
    n_unit = 0
    for variant, k in ORDER_UNIT_VARIANTS.items():
        for i in pick.take(k):
            out[i]["unit"] = variant
            n_unit += 1
    for i in pick.take(30):
        out[i]["unit"] = "DZ"
        out[i]["quantity"] = out[i]["quantity"] // 12
        n_unit += 1
    inj["unit_mismatch"] = n_unit
    late = pick.take_where(50, lambda i: out[i]["order_date"] <= (END - timedelta(days=16)).isoformat())
    inj["late_arrival"] = 50
    for i in late:
        od = date.fromisoformat(out[i]["order_date"])
        out[i]["extracted_at"] = iso(datetime.combine(od, datetime.min.time()) + timedelta(days=rng.randint(4, 15), hours=23))
    dup_idx = pick.take(90); inj["duplicates"] = 90
    dups = [dict(out[i]) for i in dup_idx]
    good = [clean[i] for i in range(len(clean)) if i not in bad]
    return shuffle_tail(rng, out + dups), good, inj


# ------------------------------------------------------------------- production

def gen_machines(rng: random.Random):
    rows = []
    for i in range(1, 13):
        mtype = MACHINE_TYPES[(i - 1) % len(MACHINE_TYPES)]
        rows.append({
            "machine_id": f"M-{i:02d}",
            "machine_name": f"{mtype} {i:02d}",
            "machine_type": mtype,
            "line": f"L{(i - 1) // 3 + 1}",
            "plant": "P100" if i <= 9 else "P200",
            "ideal_cycle_time_s": rng.choice([20, 24, 30, 36, 45, 60, 72, 90]),
            "commissioned_year": rng.randint(2012, 2024),
        })
    return rows


def gen_runs(rng: random.Random, machines: list[dict], fg: list[dict]):
    rows = []
    n = 0
    shifts = [("A", 6), ("B", 14), ("C", 22)]
    for d in daterange(START, END):
        for m in machines:
            for shift, hour in rng.sample(shifts, rng.choice([1, 2, 2])):
                n += 1
                planned = rng.choice([450, 480])
                downtime = rng.randint(8, 95)
                run_time = planned - downtime
                perf = rng.uniform(0.78, 0.96)
                units = int(run_time * 60 / m["ideal_cycle_time_s"] * perf)
                start = datetime.combine(d, datetime.min.time()) + timedelta(hours=hour)
                end = start + timedelta(minutes=planned)
                if end + timedelta(minutes=30) > AS_OF:
                    continue  # the shift is still running at the snapshot time
                rows.append({
                    "run_id": f"R-{n:06d}",
                    "machine_id": m["machine_id"],
                    "material_id": rng.choice(fg)["material_id"],
                    "shift": shift,
                    "start_ts": iso(start),
                    "end_ts": iso(end),
                    "planned_runtime": planned,
                    "downtime": downtime,
                    "time_unit": "min",
                    "units_produced": units,
                    "recorded_at": iso(end + timedelta(minutes=rng.randint(2, 30))),
                })
    clean = [dict(r) for r in rows]
    pick = Picker(rng, len(rows))
    out = rows
    bad = set()
    inj = {}
    idx = pick.take(12); bad.update(idx); inj["missing_key"] = 12
    for i in idx:
        out[i]["machine_id"] = None
    idx = pick.take(8); bad.update(idx); inj["orphan_fk"] = 8
    for i in idx:
        out[i]["machine_id"] = "M-99"
    idx = pick.take(10); bad.update(idx); inj["out_of_range"] = 10
    for i in idx:
        out[i]["downtime"] = out[i]["planned_runtime"] + rng.randint(5, 60)
    idx = pick.take(5); bad.update(idx); inj["future_date"] = 5
    for i in idx:
        st = datetime(2026, 10, rng.randint(3, 25), 6, 0, 0)
        out[i]["start_ts"] = iso(st)
        out[i]["end_ts"] = iso(st + timedelta(minutes=out[i]["planned_runtime"]))
        out[i]["recorded_at"] = iso(AS_OF - timedelta(hours=1))
    idx = pick.take(30); inj["unit_mismatch"] = 30
    for i in idx:
        out[i]["time_unit"] = "h"
        out[i]["planned_runtime"] = round(out[i]["planned_runtime"] / 60, 4)
        out[i]["downtime"] = round(out[i]["downtime"] / 60, 4)
    cutoff = iso(AS_OF - timedelta(days=9))
    idx = pick.take_where(35, lambda i: out[i]["end_ts"] <= cutoff); inj["late_arrival"] = 35
    for i in idx:
        end = datetime.fromisoformat(out[i]["end_ts"])
        out[i]["recorded_at"] = iso(end + timedelta(hours=rng.randint(50, 200)))
    dup_idx = pick.take(40); inj["duplicates"] = 40
    dups = [dict(out[i]) for i in dup_idx]
    good = [clean[i] for i in range(len(clean)) if i not in bad]
    return shuffle_tail(rng, out + dups), good, inj


def gen_scrap(rng: random.Random, runs: list[dict], weights: dict[str, float]):
    rows = []
    n = 0
    for run in runs:
        k = rng.choices([0, 1, 2, 3], weights=[0.25, 0.4, 0.25, 0.1])[0]
        if k == 0:
            continue
        budget = max(1, int(run["units_produced"] * rng.uniform(0.008, 0.045)))
        k = min(k, budget)
        cuts = sorted(rng.sample(range(1, budget), k - 1)) if k > 1 else []
        qtys = [b - a for a, b in zip([0] + cuts, cuts + [budget])]
        start = datetime.fromisoformat(run["start_ts"])
        for q in qtys:
            if q <= 0:
                continue
            n += 1
            ev = start + timedelta(minutes=rng.randint(10, 440))
            rows.append({
                "scrap_id": f"SC-{n:07d}",
                "run_id": run["run_id"],
                "material_id": run["material_id"],
                "event_ts": iso(ev),
                "scrap_qty": q,
                "unit": "PC",
                "reason_code": rng.choice(SCRAP_REASONS),
                "recorded_at": iso(ev + timedelta(minutes=rng.randint(1, 60))),
            })
    clean = [dict(r) for r in rows]
    pick = Picker(rng, len(rows))
    out = rows
    bad = set()
    inj = {}
    idx = pick.take(15); bad.update(idx); inj["missing_key"] = 15
    for i in idx:
        out[i]["run_id"] = None
    idx = pick.take(20); bad.update(idx); inj["orphan_fk"] = 20
    for k, i in enumerate(idx):
        out[i]["run_id"] = f"R-9{k:05d}"
    idx = pick.take(10); bad.update(idx); inj["out_of_range"] = 10
    for i in idx:
        out[i]["scrap_qty"] = -out[i]["scrap_qty"]
    idx = pick.take(80); inj["unit_mismatch"] = 80
    for i in idx:
        out[i]["unit"] = "KG"
        out[i]["scrap_qty"] = round(out[i]["scrap_qty"] * weights[out[i]["material_id"]], 3)
    cutoff = iso(AS_OF - timedelta(days=7))
    idx = pick.take_where(40, lambda i: out[i]["event_ts"] <= cutoff); inj["late_arrival"] = 40
    for i in idx:
        ev = datetime.fromisoformat(out[i]["event_ts"])
        out[i]["recorded_at"] = iso(ev + timedelta(hours=rng.randint(49, 150)))
    dup_idx = pick.take(50); inj["duplicates"] = 50
    dups = [dict(out[i]) for i in dup_idx]
    good = [clean[i] for i in range(len(clean)) if i not in bad]
    return shuffle_tail(rng, out + dups), good, inj


# ---------------------------------------------------------------------- quality

def gen_inspections(rng: random.Random, runs: list[dict]):
    rows = []
    n = 0
    for run in runs:
        if rng.random() > 0.8:
            continue
        n += 1
        sample = rng.choice([20, 32, 50, 80])
        accept = max(1, sample // 25)
        defects = rng.choices([0, 1, 2, 3, 4], weights=[0.62, 0.22, 0.09, 0.05, 0.02])[0]
        at = datetime.fromisoformat(run["end_ts"]) + timedelta(minutes=rng.randint(10, 240))
        if at + timedelta(hours=2) > AS_OF:
            continue
        rows.append({
            "inspection_id": f"IN-{n:06d}",
            "run_id": run["run_id"],
            "material_id": run["material_id"],
            "inspected_at": iso(at),
            "inspector": f"Q{rng.randint(1, 9):02d}",
            "sample_size": sample,
            "defects_found": defects,
            "result": "PASS" if defects <= accept else "FAIL",
            "recorded_at": iso(at + timedelta(minutes=rng.randint(1, 120))),
        })
    clean = [dict(r) for r in rows]
    pick = Picker(rng, len(rows))
    out = rows
    bad = set()
    inj = {}
    idx = pick.take(8); bad.update(idx); inj["missing_key"] = 8
    for i in idx:
        out[i]["inspection_id"] = None
    idx = pick.take(10); bad.update(idx); inj["orphan_fk"] = 10
    for k, i in enumerate(idx):
        out[i]["run_id"] = f"R-8{k:05d}"
    idx = pick.take(12); bad.update(idx); inj["out_of_range"] = 12
    for i in idx:
        out[i]["defects_found"] = out[i]["sample_size"] + rng.randint(1, 20)
        out[i]["result"] = "FAIL"
    cutoff = iso(AS_OF - timedelta(days=6))
    idx = pick.take_where(25, lambda i: out[i]["inspected_at"] <= cutoff); inj["late_arrival"] = 25
    for i in idx:
        at = datetime.fromisoformat(out[i]["inspected_at"])
        out[i]["recorded_at"] = iso(at + timedelta(hours=rng.randint(49, 120)))
    dup_idx = pick.take(30); inj["duplicates"] = 30
    dups = [dict(out[i]) for i in dup_idx]
    good = [clean[i] for i in range(len(clean)) if i not in bad]
    return shuffle_tail(rng, out + dups), good, inj


def gen_complaints(rng: random.Random, orders: list[dict]):
    delivered = [o for o in orders if o["delivered_date"]]
    feed_cutoff = AS_OF - timedelta(days=4)  # the complaints feed is deliberately stale
    rows = []
    n = 0
    for o in delivered:
        if rng.random() > 0.022:
            continue
        rec = datetime.combine(date.fromisoformat(o["delivered_date"]), datetime.min.time()) + timedelta(
            days=rng.randint(3, 40), hours=rng.randint(7, 17))
        if rec > feed_cutoff:
            continue
        n += 1
        rows.append({
            "complaint_id": f"CP-{n:05d}",
            "order_id": o["order_id"],
            "order_line": o["order_line"],
            "material_id": o["material_id"],
            "received_at": iso(rec),
            "category": rng.choice(COMPLAINT_CATEGORIES),
            "severity": rng.choice(SEVERITIES),
            "qty_affected": max(1, int(o["quantity"] * rng.uniform(0.01, 0.2))),
            "status": "closed" if rec < feed_cutoff - timedelta(days=21) else rng.choice(["open", "closed"]),
        })
    clean = [dict(r) for r in rows]
    pick = Picker(rng, len(rows))
    out = rows
    bad = set()
    inj = {}
    idx = pick.take(4); bad.update(idx); inj["missing_key"] = 4
    for i in idx:
        out[i]["order_id"] = None
    idx = pick.take(5); bad.update(idx); inj["orphan_fk"] = 5
    for k, i in enumerate(idx):
        out[i]["order_id"] = f"SO-2025-{900001 + k:06d}"
    dup_idx = pick.take(6); inj["duplicates"] = 6
    dups = [dict(out[i]) for i in dup_idx]
    good = [clean[i] for i in range(len(clean)) if i not in bad]
    return shuffle_tail(rng, out + dups), good, inj


# ------------------------------------------------------------------------ main

def generate(out_dir: Path = DEFAULT_OUT, seed: int = DEFAULT_SEED) -> dict:
    rng = random.Random(seed)
    out_dir = Path(out_dir)

    suppliers_rows, suppliers, sup_inj = gen_suppliers(rng)
    materials_rows, materials, mat_inj = gen_materials(rng, suppliers)
    fg = [m for m in materials if m["material_type"] == "FG"]
    orders_rows, orders_good, ord_inj = gen_orders(rng, fg)
    machines = gen_machines(rng)
    runs_rows, runs_good, run_inj = gen_runs(rng, machines, fg)
    weights = {m["material_id"]: m["net_weight_kg"] for m in fg}
    scrap_rows, scrap_good, scr_inj = gen_scrap(rng, runs_good, weights)
    insp_rows, insp_good, ins_inj = gen_inspections(rng, runs_good)
    compl_rows, compl_good, cmp_inj = gen_complaints(rng, orders_good)

    write_csv(out_dir / "erp" / "suppliers.csv", suppliers_rows,
              ["supplier_id", "supplier_name", "country", "rating", "lead_time_days", "extracted_at"])
    write_csv(out_dir / "erp" / "materials.csv", materials_rows,
              ["material_id", "material_name", "material_type", "material_group", "base_unit",
               "net_weight_kg", "standard_cost_eur", "list_price_eur", "supplier_id"])
    write_csv(out_dir / "erp" / "orders.csv", orders_rows,
              ["order_id", "order_line", "customer_id", "material_id", "plant", "order_date",
               "requested_date", "delivered_date", "quantity", "unit", "unit_price_eur",
               "net_value_eur", "currency", "extracted_at"])
    write_csv(out_dir / "production" / "machines.csv", machines,
              ["machine_id", "machine_name", "machine_type", "line", "plant",
               "ideal_cycle_time_s", "commissioned_year"])
    write_csv(out_dir / "production" / "machine_runs.csv", runs_rows,
              ["run_id", "machine_id", "material_id", "shift", "start_ts", "end_ts", "planned_runtime",
               "downtime", "time_unit", "units_produced", "recorded_at"])
    write_jsonl(out_dir / "production" / "scrap_events.jsonl", scrap_rows)
    write_jsonl(out_dir / "quality" / "inspections.jsonl", insp_rows)
    write_json_array(out_dir / "quality" / "complaints.json", compl_rows)

    tables = {
        "suppliers": ("erp/suppliers.csv", suppliers_rows, suppliers, sup_inj),
        "materials": ("erp/materials.csv", materials_rows, materials, mat_inj),
        "orders": ("erp/orders.csv", orders_rows, orders_good, ord_inj),
        "machines": ("production/machines.csv", machines, machines, {}),
        "machine_runs": ("production/machine_runs.csv", runs_rows, runs_good, run_inj),
        "scrap_events": ("production/scrap_events.jsonl", scrap_rows, scrap_good, scr_inj),
        "inspections": ("quality/inspections.jsonl", insp_rows, insp_good, ins_inj),
        "complaints": ("quality/complaints.json", compl_rows, compl_good, cmp_inj),
    }
    manifest = {
        "seed": seed,
        "period_start": START.isoformat(),
        "period_end": END.isoformat(),
        "as_of": iso(AS_OF),
        "tables": {},
    }
    for name, (rel, written, good, inj) in tables.items():
        data = (out_dir / rel).read_bytes()
        manifest["tables"][name] = {
            "file": rel,
            "rows_written": len(written),
            "expected_silver_rows": len(good),
            "injected": inj,
            "sha256": hashlib.sha256(data).hexdigest(),
        }
    (out_dir / "_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return manifest


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--seed", type=int, default=DEFAULT_SEED)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args(argv)
    manifest = generate(args.out, args.seed)
    for name, t in manifest["tables"].items():
        print(f"{name:<14} rows={t['rows_written']:>6}  expected_silver={t['expected_silver_rows']:>6}  injected={t['injected']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
