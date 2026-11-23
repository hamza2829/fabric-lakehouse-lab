"""Validate and run Fabric Data Pipeline definitions locally.

A pipeline lives in ``pipelines/<name>.DataPipeline/`` as it does in Fabric git
integration: ``.platform`` (item type, display name, logical id) and
``pipeline-content.json`` (activities, parameters). ``schedule.json`` holds the
body of a Fabric job scheduler call and is validated, not executed.

The runner executes activities in dependency order, one at a time:

* ``Copy``            binary copy from a local "file server" connection into ``Files/``
* ``TridentNotebook`` the notebook referenced by ``notebookId`` with its parameters
* ``InvokePipeline``  another pipeline of this repo, waiting for completion
"""
from __future__ import annotations

import fnmatch
import json
import re
import shutil
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from . import paths
from .expressions import EvalContext, ExpressionError, resolve
from .notebook import load_notebook, run_notebook

GUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
TIMEOUT_RE = re.compile(r"^\d+\.\d{2}:\d{2}:\d{2}$")
HHMM_RE = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")
SUPPORTED_ACTIVITIES = {"Copy", "TridentNotebook", "InvokePipeline"}
DEPENDENCY_CONDITIONS = {"Succeeded", "Failed", "Skipped", "Completed"}
PARAM_TYPES = {"string", "int", "float", "bool", "Expression"}
PIPELINE_PARAM_TYPES = {"string", "int", "float", "bool", "array", "object", "securestring"}
WEEKDAYS = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"}
CONNECTIONS_FILE = paths.PIPELINES_DIR / "connections.local.json"


# ----------------------------------------------------------------------- items

@dataclass
class Item:
    type: str
    display_name: str
    logical_id: str
    path: Path


def discover_items(root: Path = paths.REPO_ROOT) -> dict[str, Item]:
    """Every Fabric item in the repo, keyed by logical id."""
    items: dict[str, Item] = {}
    for plat in sorted(root.glob("*/*/.platform")):
        if ".venv" in plat.parts or "onelake" in plat.parts:
            continue
        meta = json.loads(plat.read_text(encoding="utf-8"))
        item = Item(meta["metadata"]["type"], meta["metadata"]["displayName"],
                    meta["config"]["logicalId"], plat.parent)
        if item.logical_id in items:
            raise ValueError(f"duplicate logicalId {item.logical_id} in {plat} and {items[item.logical_id].path}")
        items[item.logical_id] = item
    return items


def find_pipeline(name_or_path: str | Path, items: dict[str, Item] | None = None) -> Item:
    items = items if items is not None else discover_items()
    p = Path(name_or_path)
    for it in items.values():
        if it.type != "DataPipeline":
            continue
        if it.display_name == str(name_or_path) or it.path.resolve() == (paths.REPO_ROOT / p).resolve() \
                or it.path.resolve() == p.resolve():
            return it
    raise KeyError(f"no DataPipeline item named {name_or_path!r}")


def load_pipeline_content(item: Item) -> dict:
    return json.loads((item.path / "pipeline-content.json").read_text(encoding="utf-8"))


def load_connections(path: Path = CONNECTIONS_FILE) -> dict[str, dict]:
    return json.loads(path.read_text(encoding="utf-8"))["connections"] if path.exists() else {}


# ------------------------------------------------------------------ validation

def topological_order(activities: list[dict]) -> list[str]:
    """Kahn's algorithm, stable in definition order. Raises on cycles."""
    names = [a["name"] for a in activities]
    deps = {a["name"]: [d["activity"] for d in a.get("dependsOn", [])] for a in activities}
    done: list[str] = []
    remaining = list(names)
    while remaining:
        ready = [n for n in remaining if all(d in done for d in deps[n])]
        if not ready:
            raise ValueError(f"dependency cycle among: {', '.join(remaining)}")
        for n in ready:
            done.append(n)
            remaining.remove(n)
    return done


def _notebook_parameters(item: Item) -> set[str]:
    nb = load_notebook(item.path)
    cell = nb.parameters_cell
    if not cell:
        return set()
    return set(re.findall(r"^([A-Za-z_][A-Za-z0-9_]*)\s*=", cell.source, flags=re.M))


def _check_expressions(obj: Any, ctx: EvalContext, where: str, errors: list[str], upstream: set[str]):
    if isinstance(obj, dict):
        if obj.get("type") == "Expression" and "value" in obj:
            try:
                resolve(obj, ctx)
            except ExpressionError as ex:
                m = re.search(r"activity '([^']+)' has no output", str(ex))
                if m and m.group(1) in upstream:
                    return
                errors.append(f"{where}: bad expression {obj['value']!r}: {ex}")
            return
        for k, v in obj.items():
            _check_expressions(v, ctx, f"{where}.{k}", errors, upstream)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            _check_expressions(v, ctx, f"{where}[{i}]", errors, upstream)


def _upstream(activities: list[dict], name: str) -> set[str]:
    deps = {a["name"]: [d["activity"] for d in a.get("dependsOn", [])] for a in activities}
    seen, stack = set(), list(deps.get(name, []))
    while stack:
        n = stack.pop()
        if n not in seen:
            seen.add(n)
            stack.extend(deps.get(n, []))
    return seen


def validate_schedule(schedule: dict, where: str = "schedule") -> list[str]:
    errors: list[str] = []
    if not isinstance(schedule.get("enabled"), bool):
        errors.append(f"{where}: 'enabled' must be true or false")
    cfg = schedule.get("configuration")
    if not isinstance(cfg, dict):
        return errors + [f"{where}: missing 'configuration'"]
    kind = cfg.get("type")
    if kind not in {"Daily", "Weekly", "Cron"}:
        errors.append(f"{where}: type must be Daily, Weekly or Cron, got {kind!r}")
    if kind in {"Daily", "Weekly"}:
        times = cfg.get("times")
        if not times or not all(isinstance(t, str) and HHMM_RE.match(t) for t in times):
            errors.append(f"{where}: 'times' must be a non empty list of HH:MM strings")
    if kind == "Weekly" and (not cfg.get("weekdays") or not set(cfg["weekdays"]) <= WEEKDAYS):
        errors.append(f"{where}: 'weekdays' must name days Monday..Sunday")
    if kind == "Cron" and not (isinstance(cfg.get("interval"), int) and 1 <= cfg["interval"] <= 5270400):
        errors.append(f"{where}: Cron 'interval' must be minutes between 1 and 5270400")
    try:
        start = datetime.fromisoformat(cfg["startDateTime"])
        end = datetime.fromisoformat(cfg["endDateTime"])
        if end <= start:
            errors.append(f"{where}: endDateTime must be after startDateTime")
    except (KeyError, TypeError, ValueError):
        errors.append(f"{where}: startDateTime and endDateTime must be ISO date times")
    if not cfg.get("localTimeZoneId"):
        errors.append(f"{where}: localTimeZoneId is required")
    return errors


def validate_pipeline(item: Item, items: dict[str, Item], connections: dict | None = None,
                      content: dict | None = None) -> list[str]:
    """Return a list of human readable problems; empty means valid."""
    connections = load_connections() if connections is None else connections
    where = item.display_name
    errors: list[str] = []
    if item.type != "DataPipeline":
        errors.append(f"{where}: .platform type is {item.type!r}, expected 'DataPipeline'")
    if not GUID_RE.match(item.logical_id):
        errors.append(f"{where}: logicalId {item.logical_id!r} is not a GUID")
    if item.path.name != f"{item.display_name}.DataPipeline":
        errors.append(f"{where}: folder {item.path.name!r} does not match display name")
    try:
        content = content if content is not None else load_pipeline_content(item)
    except (OSError, json.JSONDecodeError) as ex:
        return errors + [f"{where}: cannot read pipeline-content.json: {ex}"]
    props = content.get("properties", {})
    activities = props.get("activities")
    if not isinstance(activities, list) or not activities:
        return errors + [f"{where}: properties.activities must be a non empty list"]

    params = props.get("parameters", {})
    defaults = {}
    for pname, p in params.items():
        if p.get("type") not in PIPELINE_PARAM_TYPES:
            errors.append(f"{where}: parameter {pname!r} has invalid type {p.get('type')!r}")
        if "defaultValue" not in p:
            errors.append(f"{where}: parameter {pname!r} needs a defaultValue")
        defaults[pname] = p.get("defaultValue")

    names = [a.get("name") for a in activities]
    for n in names:
        if not n or not isinstance(n, str):
            errors.append(f"{where}: activity without a name")
        elif len(n) > 55:
            errors.append(f"{where}: activity name {n!r} is longer than 55 characters")
    dupes = sorted({n for n in names if names.count(n) > 1})
    if dupes:
        errors.append(f"{where}: duplicate activity names {dupes}")

    for a in activities:
        an = f"{where}/{a.get('name')}"
        if a.get("type") not in SUPPORTED_ACTIVITIES:
            errors.append(f"{an}: unsupported activity type {a.get('type')!r}")
        for d in a.get("dependsOn", []):
            if d.get("activity") not in names:
                errors.append(f"{an}: depends on unknown activity {d.get('activity')!r}")
            if d.get("activity") == a.get("name"):
                errors.append(f"{an}: depends on itself")
            bad = set(d.get("dependencyConditions", [])) - DEPENDENCY_CONDITIONS
            if bad or not d.get("dependencyConditions"):
                errors.append(f"{an}: invalid dependencyConditions {d.get('dependencyConditions')}")
        pol = a.get("policy", {})
        if "retry" in pol and not (isinstance(pol["retry"], int) and 0 <= pol["retry"] <= 10):
            errors.append(f"{an}: policy.retry must be an integer 0..10")
        if "timeout" in pol and not TIMEOUT_RE.match(str(pol["timeout"])):
            errors.append(f"{an}: policy.timeout must look like d.hh:mm:ss")
        tp = a.get("typeProperties", {})
        if a.get("type") == "TridentNotebook":
            nb = items.get(tp.get("notebookId", ""))
            if not nb or nb.type != "Notebook":
                errors.append(f"{an}: notebookId {tp.get('notebookId')!r} is not a Notebook item in this repo")
            else:
                declared = _notebook_parameters(nb)
                for pname, pv in tp.get("parameters", {}).items():
                    if pname not in declared:
                        errors.append(f"{an}: parameter {pname!r} is not declared in the parameters cell of {nb.display_name}")
                    if not isinstance(pv, dict) or pv.get("type") not in PARAM_TYPES or "value" not in pv:
                        errors.append(f"{an}: parameter {pname!r} needs {{'value': ..., 'type': string|int|float|bool}}")
        elif a.get("type") == "InvokePipeline":
            target = items.get(tp.get("pipelineId", ""))
            if not target or target.type != "DataPipeline":
                errors.append(f"{an}: pipelineId {tp.get('pipelineId')!r} is not a DataPipeline item in this repo")
            elif target.logical_id == item.logical_id:
                errors.append(f"{an}: a pipeline cannot invoke itself")
            if tp.get("waitOnCompletion") is not True:
                errors.append(f"{an}: waitOnCompletion must be true for the local runner")
        elif a.get("type") == "Copy":
            src = tp.get("source", {})
            snk = tp.get("sink", {})
            if src.get("type") != "BinarySource" or snk.get("type") != "BinarySink":
                errors.append(f"{an}: only BinarySource -> BinarySink copies are supported")
            sds = src.get("datasetSettings", {})
            conn = sds.get("externalReferences", {}).get("connection")
            if conn not in connections:
                errors.append(f"{an}: source connection {conn!r} is not defined in connections.local.json")
            if sds.get("typeProperties", {}).get("location", {}).get("type") != "FileServerLocation":
                errors.append(f"{an}: source location must be a FileServerLocation")
            loc = snk.get("datasetSettings", {}).get("typeProperties", {}).get("location", {})
            if loc.get("type") != "LakehouseLocation":
                errors.append(f"{an}: sink location must be a LakehouseLocation")
            root = snk.get("datasetSettings", {}).get("linkedService", {}).get("properties", {}) \
                .get("typeProperties", {}).get("rootFolder")
            if root != "Files":
                errors.append(f"{an}: sink rootFolder must be 'Files' (raw files land in the Files area)")
        ctx = EvalContext(parameters=defaults, pipeline_name=item.display_name)
        _check_expressions(tp, ctx, an, errors, _upstream(activities, a.get("name")))

    try:
        topological_order(activities)
    except (ValueError, KeyError) as ex:
        errors.append(f"{where}: {ex}")

    sched = item.path / "schedule.json"
    if sched.exists():
        errors += validate_schedule(json.loads(sched.read_text(encoding="utf-8")), f"{where}/schedule.json")
    return errors


def validate_all(items: dict[str, Item] | None = None) -> dict[str, list[str]]:
    items = items if items is not None else discover_items()
    connections = load_connections()
    out = {}
    for it in items.values():
        if it.type == "DataPipeline":
            out[it.display_name] = validate_pipeline(it, items, connections)
        elif it.type == "Notebook":
            try:
                nb = load_notebook(it.path)
                problems = [] if nb.code_cells() else [f"{it.display_name}: no code cells"]
            except Exception as ex:  # noqa: BLE001 - report every parse failure
                problems = [f"{it.display_name}: {ex}"]
            out[it.display_name] = problems
    return out


# --------------------------------------------------------------------- running

@dataclass
class ActivityRun:
    name: str
    type: str
    status: str
    attempts: int = 0
    seconds: float = 0.0
    output: Any = None
    error: str | None = None


@dataclass
class PipelineRun:
    pipeline: str
    run_id: str
    parameters: dict
    status: str = "InProgress"
    activities: list[ActivityRun] = field(default_factory=list)
    started: str = ""
    seconds: float = 0.0
    children: list["PipelineRun"] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline, "run_id": self.run_id, "status": self.status,
            "parameters": self.parameters, "started": self.started, "seconds": round(self.seconds, 2),
            "activities": [a.__dict__ for a in self.activities],
            "children": [c.to_dict() for c in self.children],
        }

    def notebook_exits(self) -> dict[str, Any]:
        out = {}
        for c in self.children:
            out.update(c.notebook_exits())
        for a in self.activities:
            if a.type == "TridentNotebook" and a.output:
                ev = a.output.get("result", {}).get("exitValue")
                out[a.output.get("notebook", a.name)] = json.loads(ev) if ev else None
        return out


def _deps_satisfied(activity: dict, results: dict[str, ActivityRun]) -> bool:
    for d in activity.get("dependsOn", []):
        st = results[d["activity"]].status
        conds = set(d["dependencyConditions"])
        if "Completed" in conds and st in {"Succeeded", "Failed"}:
            continue
        if st not in conds:
            return False
    return True


def _copy(tp: dict, ctx: EvalContext, connections: dict) -> dict:
    src, snk = tp["source"], tp["sink"]
    sds = resolve(src["datasetSettings"], ctx)
    conn = connections[sds["externalReferences"]["connection"]]
    base = (paths.REPO_ROOT / conn["localPath"]).resolve()
    folder = base / sds["typeProperties"]["location"].get("folderPath", "")
    store = resolve(src.get("storeSettings", {}), ctx)
    pattern = store.get("wildcardFileName", "*")
    files = folder.rglob("*") if store.get("recursive") else folder.glob("*")
    files = sorted(f for f in files if f.is_file() and fnmatch.fnmatch(f.name, pattern))
    if not files:
        raise FileNotFoundError(f"copy source {folder} has no files matching {pattern!r}")
    sink_loc = resolve(snk["datasetSettings"]["typeProperties"]["location"], ctx)
    target = paths.files_dir() / sink_loc["folderPath"]
    target.mkdir(parents=True, exist_ok=True)
    preserve = resolve(snk.get("storeSettings", {}), ctx).get("copyBehavior", "PreserveHierarchy") == "PreserveHierarchy"
    written = 0
    for f in files:
        dest = target / (f.relative_to(folder) if preserve else f.name)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(f, dest)
        written += f.stat().st_size
    return {"filesRead": len(files), "filesWritten": len(files), "dataRead": written,
            "dataWritten": written, "sink": str(target.relative_to(paths.lakehouse_root()).as_posix())}


_CASTS: dict[str, Callable[[Any], Any]] = {
    "string": str, "int": int, "float": float,
    "bool": lambda v: v if isinstance(v, bool) else str(v).lower() == "true",
}


def run_pipeline(name_or_path: str | Path, parameters: dict | None = None, *,
                 items: dict[str, Item] | None = None, echo: Callable[[str], None] = print,
                 spark=None, _depth: int = 0) -> PipelineRun:
    items = items if items is not None else discover_items()
    item = find_pipeline(name_or_path, items)
    problems = validate_pipeline(item, items)
    if problems:
        raise ValueError("pipeline is invalid:\n  " + "\n  ".join(problems))
    content = load_pipeline_content(item)
    props = content["properties"]
    params = {k: v.get("defaultValue") for k, v in props.get("parameters", {}).items()}
    for k, v in (parameters or {}).items():
        if k not in params:
            raise KeyError(f"{item.display_name} has no parameter {k!r}")
        params[k] = v
    run = PipelineRun(item.display_name, str(uuid.uuid4()), params,
                      started=datetime.now(timezone.utc).isoformat(timespec="seconds"))
    ctx = EvalContext(parameters=params, run_id=run.run_id, pipeline_name=item.display_name,
                      trigger_time=run.started)
    connections = load_connections()
    by_name = {a["name"]: a for a in props["activities"]}
    results: dict[str, ActivityRun] = {}
    pad = "  " * _depth
    echo(f"{pad}pipeline {item.display_name} run {run.run_id} parameters={params}")
    t0 = time.perf_counter()

    for name in topological_order(props["activities"]):
        a = by_name[name]
        ar = ActivityRun(name, a["type"], "Skipped")
        results[name] = ar
        run.activities.append(ar)
        if not _deps_satisfied(a, results):
            echo(f"{pad}  - {name}: Skipped (upstream condition not met)")
            continue
        retries = int(a.get("policy", {}).get("retry", 0))
        t_a = time.perf_counter()
        while True:
            ar.attempts += 1
            try:
                tp = a.get("typeProperties", {})
                if a["type"] == "Copy":
                    ar.output = _copy(tp, ctx, connections)
                elif a["type"] == "TridentNotebook":
                    from .runtime import NotebookUtils, get_spark, register_lakehouse_tables
                    spark = spark or get_spark()
                    nb_item = items[tp["notebookId"]]
                    nb_params = {}
                    for pname, pv in tp.get("parameters", {}).items():
                        val = resolve(pv["value"], ctx)
                        nb_params[pname] = _CASTS.get(pv["type"], lambda v: v)(val)
                    register_lakehouse_tables(spark)
                    res = run_notebook(load_notebook(nb_item.path), spark, NotebookUtils(), nb_params,
                                       after_cell=lambda: register_lakehouse_tables(spark),
                                       echo=lambda m: echo(pad + m))
                    ar.output = {"notebook": nb_item.display_name, "status": "Succeeded",
                                 "result": {"exitValue": res.exit_value},
                                 "cells": len(res.cells), "seconds": round(res.seconds, 2)}
                elif a["type"] == "InvokePipeline":
                    child_params = {k: resolve(v, ctx) for k, v in tp.get("parameters", {}).items()}
                    child = run_pipeline(items[tp["pipelineId"]].path, child_params, items=items,
                                         echo=echo, spark=spark, _depth=_depth + 1)
                    run.children.append(child)
                    if child.status != "Succeeded":
                        raise RuntimeError(f"invoked pipeline {child.pipeline} ended {child.status}")
                    ar.output = {"pipelineRunId": child.run_id, "status": child.status}
                ar.status = "Succeeded"
                ar.error = None
                break
            except Exception as ex:  # noqa: BLE001 - activity failures are data, not crashes
                ar.status = "Failed"
                ar.error = f"{type(ex).__name__}: {ex}"
                if ar.attempts > retries:
                    break
                echo(f"{pad}  - {name}: attempt {ar.attempts} failed ({ar.error}); retrying")
                time.sleep(min(1.0, float(a.get("policy", {}).get("retryIntervalInSeconds", 0))))
        ar.seconds = round(time.perf_counter() - t_a, 2)
        ctx.activity_outputs[name] = ar.output or {}
        echo(f"{pad}  - {name} [{a['type']}]: {ar.status} in {ar.seconds}s"
             + (f" -> {ar.error}" if ar.error else ""))

    run.seconds = time.perf_counter() - t0
    run.status = "Failed" if any(r.status == "Failed" for r in run.activities) else "Succeeded"
    echo(f"{pad}pipeline {item.display_name}: {run.status} in {run.seconds:.1f}s")
    log_dir = paths.files_dir() / "_pipeline_runs"
    log_dir.mkdir(parents=True, exist_ok=True)
    (log_dir / f"{item.display_name}_{run.run_id[:8]}.json").write_text(
        json.dumps(run.to_dict(), indent=2, default=str), encoding="utf-8")
    return run
