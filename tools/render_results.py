"""Render the README results section from the built lakehouse and the quality report.

Usage:
    python tools/render_results.py            # print the markdown block
    python tools/render_results.py --write    # replace the block between the markers in README.md

Everything comes from deterministic outputs (Delta tables, quality_report.json),
so the block is identical on every run with the same seed, and a test checks
that README.md matches it.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fabric_local import paths  # noqa: E402
from quality.run_quality import read_delta  # noqa: E402

START = "<!-- results:start -->"
END = "<!-- results:end -->"
README = paths.REPO_ROOT / "README.md"

ENTITY_ORDER = ["suppliers", "materials", "orders", "machines", "machine_runs", "scrap_events",
                "inspections", "complaints"]
SOURCE_OF = {"suppliers": "erp", "materials": "erp", "orders": "erp", "machines": "production",
             "machine_runs": "production", "scrap_events": "production", "inspections": "quality",
             "complaints": "quality"}
ISSUES = [
    ("missing_key", "quarantined (`missing_key`)"),
    ("duplicate", "removed, newest extract wins"),
    ("orphan_fk", "quarantined (`orphan_fk`)"),
    ("out_of_range", "quarantined (`out_of_range`)"),
    ("future_date", "quarantined (`future_date`)"),
    ("unit_mismatch", "converted (pieces, minutes)"),
    ("late_arrival", "kept, flagged `is_late_arrival`"),
]
GOLD = ["dim_date", "dim_material", "dim_supplier", "dim_machine", "fact_orders", "fact_production",
        "fact_inspections", "fact_complaints", "gold_kpi_summary", "gold_kpi_monthly"]


def _fmt_kpi(value: float, fmt: str) -> str:
    if value is None:
        return "n/a"
    if fmt.endswith("%"):
        decimals = len(fmt.split(".")[1]) - 1 if "." in fmt else 0
        return f"{value * 100:.{decimals}f} %"
    if "." in fmt:
        return f"{value:,.2f}"
    return f"{value:,.0f}"


def render(tables_dir: Path | None = None, quality_json: Path | None = None) -> str:
    stats = {r["entity"]: r for r in read_delta("silver_load_stats", tables_dir).to_dict("records")}
    q = json.loads(Path(quality_json or paths.REPORTS_DIR / "quality_report.json").read_text(encoding="utf-8"))
    kpis = read_delta("gold_kpi_summary", tables_dir)
    lines = ["### Rows per layer", "",
             "| Source | Entity | Bronze | Quarantined | Duplicates removed | Silver |",
             "|---|---|---:|---:|---:|---:|"]
    tot = [0, 0, 0, 0]
    for e in ENTITY_ORDER:
        s = stats[e]
        vals = [s["bronze_rows"], s["quarantined"], s["duplicates_removed"], s["silver_rows"]]
        tot = [a + b for a, b in zip(tot, vals)]
        lines.append(f"| {SOURCE_OF[e]} | {e} | " + " | ".join(f"{v:,}" for v in vals) + " |")
    lines.append("| **all** | | " + " | ".join(f"**{v:,}**" for v in tot) + " |")
    lines.append("")

    by = {}
    for r in q["results"]:
        if r["layer"] == "bronze":
            by[(r["source"], r["issue"])] = by.get((r["source"], r["issue"]), 0) + r["failed_rows"]
    left = {}
    for r in q["results"]:
        if r["layer"] == "silver" and r["severity"] == "error":
            left[r["issue"]] = left.get(r["issue"], 0) + r["failed_rows"]
    lines += ["### Issues found in bronze and fixed in silver", "",
              "| Issue | ERP | Production | Quality | Total | Handling | Left in silver |",
              "|---|---:|---:|---:|---:|---|---:|"]
    for issue, handling in ISSUES:
        vals = [by.get((s, issue), 0) for s in ("erp", "production", "quality")]
        remaining = left.get(issue, 0) if issue != "late_arrival" else "n/a (flagged)"
        lines.append(f"| {issue} | " + " | ".join(str(v) for v in vals) + f" | **{sum(vals)}** | {handling} | {remaining} |")
    stale = [r for r in q["results"] if r["check"] == "freshness" and r["layer"] == "bronze"]
    vals = [sum(r["failed_rows"] for r in stale if r["source"] == s) for s in ("erp", "production", "quality")]
    feeds = "; ".join(f"{r['table']}: {r['detail']}" for r in stale if r["failed_rows"]) or "none"
    lines.append(f"| stale_source (tables) | " + " | ".join(str(v) for v in vals)
                 + f" | **{sum(vals)}** | reported as warning ({feeds}) | not fixable in the lakehouse |")
    g = q["gate"]
    rates = ", ".join(f"{s} {v['rate']:.2%}" for s, v in g["bronze_rates"].items())
    lines += ["", f"Quality gate: **{'PASS' if g['passed'] else 'FAIL'}**. Bronze issue rate per source: {rates} "
              f"(limit {g['max_bronze_issue_rate']:.0%}); error findings left in silver: {g['silver_errors']}.", ""]

    lines += ["### Gold tables", "", "| Table | Rows |", "|---|---:|"]
    from deltalake import DeltaTable
    tdir = Path(tables_dir or paths.tables_dir())
    for t in GOLD:
        n = DeltaTable(str(tdir / t)).to_pyarrow_table().num_rows
        lines.append(f"| `{t}` | {n:,} |")
    lines.append("")

    lines += ["### KPIs (grand total, Apr to Sep 2026)", "",
              "Computed by Spark SQL in `gold_kpi_summary` and recomputed from the DAX measures in the TMDL files by the tests.", "",
              "| Area | KPI (DAX measure) | Value |", "|---|---|---:|"]
    order = ["Order Lines", "Net Sales EUR", "Delivered Lines", "On-Time Lines", "On-Time Delivery %",
             "Units Produced", "Scrap Units", "Scrap Rate %", "Availability %", "Performance %",
             "Quality Rate %", "OEE %", "Complaints", "Complaint Rate %", "Inspection Pass Rate %"]
    for r in sorted(kpis.to_dict("records"), key=lambda r: order.index(r["kpi_name"])):
        lines.append(f"| {r['kpi_area']} | {r['kpi_name']} | {_fmt_kpi(r['kpi_value'], r['value_format'])} |")
    return "\n".join(lines) + "\n"


def replace_block(readme: str, block: str) -> str:
    if START not in readme or END not in readme:
        raise ValueError("README.md has no results markers")
    head, rest = readme.split(START, 1)
    _, tail = rest.split(END, 1)
    return f"{head}{START}\n{block}{END}{tail}"


def current_block(readme: str) -> str:
    return readme.split(START, 1)[1].split(END, 1)[0].lstrip("\n")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true", help="update README.md in place")
    args = ap.parse_args(argv)
    block = render()
    if args.write:
        README.write_text(replace_block(README.read_text(encoding="utf-8"), block), encoding="utf-8")
        print("README.md results updated")
    else:
        print(block)
    return 0


if __name__ == "__main__":
    sys.exit(main())
