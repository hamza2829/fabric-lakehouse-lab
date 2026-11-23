"""Run the data quality rules against bronze and silver and write a markdown report.

Usage:
    python -m quality.run_quality [--rules quality/rules.toml] [--report reports/quality_report.md]
                                  [--max-bronze-issue-rate 0.10]

Exit code 1 when the gate fails:
* a source system's bronze issue rate is above ``max_bronze_issue_rate``, or
* silver still has more error findings than ``max_silver_errors``, or
* a source is stale and ``fail_on_freshness`` is set.
"""
from __future__ import annotations

import argparse
import json
import sys
import tomllib
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from fabric_local import paths
from quality import checks as C

DEFAULT_RULES = Path(__file__).with_name("rules.toml")
ISSUE_ORDER = ["missing_key", "duplicate", "orphan_fk", "out_of_range", "future_date",
               "unit_mismatch", "late_arrival", "stale_source"]
SOURCE_ORDER = ["erp", "production", "quality"]


def load_rules(path: Path = DEFAULT_RULES) -> dict:
    with open(path, "rb") as fh:
        return tomllib.load(fh)


def read_delta(name: str, tables_dir: Path | None = None) -> pd.DataFrame:
    from deltalake import DeltaTable

    path = Path(tables_dir or paths.tables_dir()) / name
    if not paths.is_delta_table(path):
        raise FileNotFoundError(f"Delta table not found: {path}")
    return DeltaTable(str(path)).to_pandas()


def run_check(spec: dict, df: pd.DataFrame, ref_frames: dict[str, pd.DataFrame], as_of: datetime):
    t = spec["type"]
    if t == "not_null":
        return C.not_null(df, spec["columns"])
    if t == "unique":
        return C.unique(df, spec["columns"])
    if t == "range":
        return C.value_range(df, spec["column"], spec.get("min"), spec.get("max"), spec.get("exclusive_min", False))
    if t == "compare":
        return C.compare(df, spec["left"], spec["op"], spec["right"])
    if t == "allowed_values":
        return C.allowed_values(df, spec["column"], spec["values"])
    if t == "referential":
        return C.referential(df, spec["columns"], ref_frames[spec["ref"]], spec["ref_columns"])
    if t == "not_in_future":
        return C.not_in_future(df, spec["column"], as_of)
    if t == "late_arrival":
        return C.late_arrival(df, spec["recorded"], spec["event"], spec["max_hours"])
    if t == "freshness":
        return C.freshness(df, spec["column"], as_of, spec["max_age_hours"])
    raise ValueError(f"unknown check type {t!r}")


@dataclass
class QualityRun:
    as_of: str
    generated: str
    results: list[C.CheckResult] = field(default_factory=list)
    row_counts: dict = field(default_factory=dict)          # (layer, entity) -> rows
    load_stats: list[dict] = field(default_factory=list)
    quarantine: list[dict] = field(default_factory=list)
    gate: dict = field(default_factory=dict)

    def to_json(self) -> dict:
        return {
            "as_of": self.as_of, "generated": self.generated, "gate": self.gate,
            "row_counts": {f"{k[0]}.{k[1]}": v for k, v in self.row_counts.items()},
            "results": [asdict(r) for r in self.results],
            "load_stats": self.load_stats, "quarantine": self.quarantine,
        }


def run(rules: dict, tables_dir: Path | None = None) -> QualityRun:
    as_of = datetime.fromisoformat(rules["as_of"])
    qr = QualityRun(rules["as_of"], datetime.now(timezone.utc).isoformat(timespec="seconds"))
    for layer in ("bronze", "silver"):
        frames = {e["name"]: read_delta(e[layer], tables_dir) for e in rules["entity"]}
        for e in rules["entity"]:
            df = frames[e["name"]]
            qr.row_counts[(layer, e["name"])] = len(df)
            for spec in e["checks"]:
                if layer not in spec.get("layers", ["bronze", "silver"]):
                    continue
                failed, detail, examples = run_check(spec, df, frames, as_of)
                total = 1 if spec["type"] == "freshness" else len(df)
                qr.results.append(C.CheckResult(
                    check=spec["type"], table=e["name"], source=e["source"], layer=layer,
                    issue=spec["issue"], severity=rules["severity"][layer][spec["issue"]],
                    failed_rows=failed, total_rows=total, detail=detail, examples=examples))
    stats = read_delta("silver_load_stats", tables_dir)
    qr.load_stats = stats.drop(columns=[c for c in ("batch_id",) if c in stats.columns]).to_dict("records")
    q = read_delta("silver_quarantine", tables_dir)
    qr.quarantine = (q.groupby(["entity", "dq_reason"]).size().reset_index(name="rows")
                     .sort_values(["entity", "dq_reason"]).to_dict("records")) if len(q) else []
    qr.gate = evaluate_gate(qr, rules["gate"])
    return qr


def bronze_issue_rates(qr: QualityRun) -> dict[str, dict]:
    rows = defaultdict(int)
    issues = defaultdict(int)
    for (layer, entity), n in qr.row_counts.items():
        if layer == "bronze":
            src = next(r.source for r in qr.results if r.table == entity)
            rows[src] += n
    for r in qr.results:
        if r.layer == "bronze" and r.check != "freshness" and r.severity in ("error", "warn"):
            issues[r.source] += r.failed_rows
    return {s: {"rows": rows[s], "issues": issues[s], "rate": issues[s] / rows[s] if rows[s] else 0.0}
            for s in rows}


def evaluate_gate(qr: QualityRun, gate: dict) -> dict:
    rates = bronze_issue_rates(qr)
    max_rate = float(gate["max_bronze_issue_rate"])
    silver_errors = sum(r.failed_rows for r in qr.results if r.layer == "silver" and r.severity == "error")
    stale = [f"{r.layer}.{r.table}" for r in qr.results if r.check == "freshness" and r.failed_rows]
    reasons = [f"bronze issue rate of {s} is {v['rate']:.2%} (limit {max_rate:.2%})"
               for s, v in rates.items() if v["rate"] > max_rate]
    if silver_errors > int(gate["max_silver_errors"]):
        reasons.append(f"{silver_errors} error finding(s) left in silver (limit {gate['max_silver_errors']})")
    if gate.get("fail_on_freshness") and stale:
        reasons.append(f"stale sources: {', '.join(stale)}")
    return {"passed": not reasons, "reasons": reasons, "bronze_rates": rates,
            "max_bronze_issue_rate": max_rate, "silver_errors": silver_errors, "stale": stale}


# ---------------------------------------------------------------------- report

def _pct(x: float) -> str:
    return f"{x:.2%}"


def render_markdown(qr: QualityRun) -> str:
    g = qr.gate
    out = [
        "# Data quality report",
        "",
        f"* as-of: `{qr.as_of}`  ",
        f"* generated: `{qr.generated}`  ",
        f"* gate: **{'PASS' if g['passed'] else 'FAIL'}**"
        f" (bronze issue rate limit {_pct(g['max_bronze_issue_rate'])} per source, silver errors must be 0)",
        "",
    ]
    for r in g["reasons"]:
        out.append(f"> FAIL: {r}")
    if g["reasons"]:
        out.append("")

    silver_err = defaultdict(int)
    for r in qr.results:
        if r.layer == "silver" and r.severity == "error":
            silver_err[r.source] += r.failed_rows
    out += ["## Summary per source", "",
            "| Source | Bronze rows | Bronze findings | Issue rate | Silver errors | Stale tables | Status |",
            "|---|---:|---:|---:|---:|---|---|"]
    for s in SOURCE_ORDER:
        v = g["bronze_rates"].get(s)
        if not v:
            continue
        stale = ", ".join(t for t in g["stale"] if any(t.endswith("." + r.table) and r.source == s for r in qr.results)) or "none"
        ok = v["rate"] <= g["max_bronze_issue_rate"] and silver_err[s] == 0
        status = "FAIL" if not ok else ("ok, stale feed warning" if stale != "none" else "ok")
        out.append(f"| {s} | {v['rows']:,} | {v['issues']:,} | {_pct(v['rate'])} | {silver_err[s]} | {stale} | {status} |")
    out.append("")

    out += ["## Inconsistencies found in bronze, per source", "",
            "Rows failing each issue category, as delivered by the source system.", "",
            "| Source | Entity | " + " | ".join(ISSUE_ORDER[:-1]) + " |",
            "|---|---|" + "---:|" * (len(ISSUE_ORDER) - 1)]
    by = defaultdict(int)
    for r in qr.results:
        if r.layer == "bronze":
            by[(r.source, r.table, r.issue)] += r.failed_rows
    entities = []
    for r in qr.results:
        if (r.source, r.table) not in entities:
            entities.append((r.source, r.table))
    for s, t in sorted(entities, key=lambda x: SOURCE_ORDER.index(x[0])):
        out.append(f"| {s} | {t} | " + " | ".join(str(by.get((s, t, i), 0)) for i in ISSUE_ORDER[:-1]) + " |")
    out.append("")

    if qr.load_stats:
        out += ["## What silver did with them", "",
                "| Entity | Bronze rows | Quarantined | Duplicates removed | Units converted | Late flagged | Silver rows |",
                "|---|---:|---:|---:|---:|---:|---:|"]
        for s in qr.load_stats:
            out.append(f"| {s['entity']} | {s['bronze_rows']:,} | {s['quarantined']:,} | {s['duplicates_removed']:,} | "
                       f"{s['units_converted']:,} | {s['late_flagged']:,} | {s['silver_rows']:,} |")
        out.append("")
    if qr.quarantine:
        out += ["### Quarantine by reason (`silver_quarantine`)", "",
                "| Entity | Reason | Rows |", "|---|---|---:|"]
        for q in qr.quarantine:
            out.append(f"| {q['entity']} | {q['dq_reason']} | {q['rows']} |")
        out.append("")

    out += ["## Freshness", "", "| Layer | Entity | Result | Detail |", "|---|---|---|---|"]
    for r in qr.results:
        if r.check == "freshness":
            out.append(f"| {r.layer} | {r.table} | {'stale' if r.failed_rows else 'fresh'} | {r.detail} |")
    out.append("")

    out += ["## All checks", "", "| Layer | Entity | Check | Issue | Severity | Failed | Rows | Detail |",
            "|---|---|---|---|---|---:|---:|---|"]
    for r in qr.results:
        out.append(f"| {r.layer} | {r.table} | {r.check} | {r.issue} | {r.severity} | {r.failed_rows} | "
                   f"{r.total_rows} | {r.detail.replace('|', '/')} |")
    out.append("")
    examples = [r for r in qr.results if r.layer == "bronze" and r.examples]
    if examples:
        out += ["<details><summary>Example rows per bronze finding</summary>", ""]
        for r in examples:
            out.append(f"* **{r.table} / {r.check}** ({r.detail}): `{json.dumps(r.examples[:2], ensure_ascii=False)}`")
        out += ["", "</details>", ""]
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Run lakehouse data quality checks")
    ap.add_argument("--rules", type=Path, default=DEFAULT_RULES)
    ap.add_argument("--tables-dir", type=Path, default=None)
    ap.add_argument("--report", type=Path, default=paths.REPORTS_DIR / "quality_report.md")
    ap.add_argument("--json", type=Path, default=paths.REPORTS_DIR / "quality_report.json")
    ap.add_argument("--max-bronze-issue-rate", type=float, default=None,
                    help="override the gate threshold from the rules file")
    args = ap.parse_args(argv)
    rules = load_rules(args.rules)
    if args.max_bronze_issue_rate is not None:
        rules["gate"]["max_bronze_issue_rate"] = args.max_bronze_issue_rate
    qr = run(rules, args.tables_dir)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(render_markdown(qr), encoding="utf-8")
    args.json.write_text(json.dumps(qr.to_json(), indent=2, default=str), encoding="utf-8")
    for s, v in qr.gate["bronze_rates"].items():
        print(f"{s:<11} bronze findings {v['issues']:>5} / {v['rows']:>6} rows = {v['rate']:.2%}")
    print(f"silver errors: {qr.gate['silver_errors']}   stale: {qr.gate['stale'] or 'none'}")
    print(f"gate: {'PASS' if qr.gate['passed'] else 'FAIL'}  report: {args.report}")
    for r in qr.gate["reasons"]:
        print(f"  - {r}")
    return 0 if qr.gate["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
