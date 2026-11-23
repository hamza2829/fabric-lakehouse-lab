"""Command line entry point: ``python -m fabric_local {validate,run,items}``."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

from . import paths
from .pipeline import discover_items, run_pipeline, validate_all


def cmd_validate(_args) -> int:
    results = validate_all()
    bad = 0
    for name, problems in sorted(results.items()):
        print(f"{'OK  ' if not problems else 'FAIL'} {name}")
        for p in problems:
            print(f"     - {p}")
        bad += len(problems)
    print(f"{len(results)} items checked, {bad} problem(s)")
    return 1 if bad else 0


def _parse_params(pairs: list[str]) -> dict:
    out = {}
    for p in pairs:
        if "=" not in p:
            raise SystemExit(f"--param expects name=value, got {p!r}")
        k, v = p.split("=", 1)
        out[k] = v
    return out


def cmd_run(args) -> int:
    run = run_pipeline(args.pipeline, _parse_params(args.param))
    paths.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    summary = {
        "pipeline": run.pipeline,
        "status": run.status,
        "parameters": run.parameters,
        "finished": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "seconds": round(run.seconds, 1),
        "notebooks": run.notebook_exits(),
        "run": run.to_dict(),
    }
    out = paths.REPORTS_DIR / "run_summary.json"
    out.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print(f"run summary written to {out.relative_to(paths.REPO_ROOT).as_posix()}")
    return 0 if run.status == "Succeeded" else 1


def cmd_items(_args) -> int:
    for it in sorted(discover_items().values(), key=lambda i: (i.type, i.display_name)):
        print(f"{it.type:<14} {it.display_name:<32} {it.logical_id}  {it.path.relative_to(paths.REPO_ROOT).as_posix()}")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="fabric_local", description="Local runner for Fabric format items")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("validate", help="validate every pipeline, schedule and notebook").set_defaults(func=cmd_validate)
    r = sub.add_parser("run", help="run a pipeline and its notebooks locally")
    r.add_argument("pipeline", help="display name or folder of the pipeline")
    r.add_argument("--param", action="append", default=[], metavar="NAME=VALUE")
    r.set_defaults(func=cmd_run)
    sub.add_parser("items", help="list the Fabric items in the repo").set_defaults(func=cmd_items)
    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
