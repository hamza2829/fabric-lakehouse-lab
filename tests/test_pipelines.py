"""Data Pipeline definitions: validator, expressions, schedule and the local runner."""
from __future__ import annotations

import copy
import json

import pytest

from fabric_local import pipeline as P
from fabric_local.expressions import EvalContext, ExpressionError, evaluate, resolve

BRONZE_NB = "241a8f5d-fca5-5a3c-b1ad-0ffa764c97da"


@pytest.fixture(scope="module")
def items():
    return P.discover_items()


@pytest.fixture()
def daily(items):
    item = P.find_pipeline("pl_lakehouse_daily", items)
    return item, P.load_pipeline_content(item)


def test_all_repo_items_validate_clean(items):
    problems = P.validate_all(items)
    assert set(problems) >= {"pl_ingest_sources", "pl_lakehouse_daily", "nb_01_bronze_ingest"}
    assert {k: v for k, v in problems.items() if v} == {}


def test_daily_pipeline_runs_medallion_in_order(daily):
    _, content = daily
    order = P.topological_order(content["properties"]["activities"])
    assert order == ["Ingest source extracts", "Bronze ingest", "Silver clean and conform", "Gold KPIs"]


def test_pipelines_use_copy_notebook_and_invoke_activities(items):
    types = set()
    for it in items.values():
        if it.type == "DataPipeline":
            types |= {a["type"] for a in P.load_pipeline_content(it)["properties"]["activities"]}
    assert types == {"Copy", "TridentNotebook", "InvokePipeline"}


def test_cycle_is_reported(items, daily):
    item, content = daily
    bad = copy.deepcopy(content)
    acts = bad["properties"]["activities"]
    acts[0]["dependsOn"] = [{"activity": "Gold KPIs", "dependencyConditions": ["Succeeded"]}]
    problems = P.validate_pipeline(item, items, content=bad)
    assert any("cycle" in p for p in problems)


def test_unknown_dependency_and_duplicate_names_are_reported(items, daily):
    item, content = daily
    bad = copy.deepcopy(content)
    acts = bad["properties"]["activities"]
    acts[1]["dependsOn"] = [{"activity": "Does not exist", "dependencyConditions": ["Succeeded"]}]
    acts[3]["name"] = acts[2]["name"]
    problems = "\n".join(P.validate_pipeline(item, items, content=bad))
    assert "unknown activity 'Does not exist'" in problems
    assert "duplicate activity names" in problems


def test_notebook_reference_and_parameters_are_checked(items, daily):
    item, content = daily
    bad = copy.deepcopy(content)
    acts = bad["properties"]["activities"]
    acts[1]["typeProperties"]["parameters"]["not_a_parameter"] = {"value": "x", "type": "string"}
    acts[3]["typeProperties"]["notebookId"] = "00000000-0000-0000-0000-000000000000"
    problems = "\n".join(P.validate_pipeline(item, items, content=bad))
    assert "'not_a_parameter' is not declared" in problems
    assert "is not a Notebook item" in problems


def test_bad_expression_is_reported(items, daily):
    item, content = daily
    bad = copy.deepcopy(content)
    bad["properties"]["activities"][1]["typeProperties"]["parameters"]["load_date"]["value"]["value"] = \
        "@pipeline().parameters.missing"
    assert any("bad expression" in p for p in P.validate_pipeline(item, items, content=bad))


def test_schedule_validation():
    good = {"enabled": True, "configuration": {"type": "Daily", "times": ["05:30"],
            "startDateTime": "2026-10-01T00:00:00", "endDateTime": "2027-09-30T23:59:00",
            "localTimeZoneId": "W. Europe Standard Time"}}
    assert P.validate_schedule(good) == []
    bad = json.loads(json.dumps(good))
    bad["configuration"].update(times=["25:00"], endDateTime="2026-01-01T00:00:00")
    errors = "\n".join(P.validate_schedule(bad))
    assert "HH:MM" in errors and "endDateTime" in errors
    weekly = json.loads(json.dumps(good))
    weekly["configuration"].update(type="Weekly", weekdays=["Funday"])
    assert any("weekdays" in e for e in P.validate_schedule(weekly))


def test_expressions():
    ctx = EvalContext(parameters={"load_date": "2026-09-30"}, run_id="r1", pipeline_name="pl")
    assert evaluate("@concat('landing/erp/', pipeline().parameters.load_date)", ctx) == "landing/erp/2026-09-30"
    assert evaluate("@pipeline().RunId", ctx) == "r1"
    assert evaluate("Files/@{pipeline().parameters.load_date}/x", ctx) == "Files/2026-09-30/x"
    assert evaluate("@toUpper('it''s')", ctx) == "IT'S"
    assert resolve({"value": "@pipeline().Pipeline", "type": "Expression"}, ctx) == "pl"
    with pytest.raises(ExpressionError):
        evaluate("@pipeline().parameters.nope", ctx)
    with pytest.raises(ExpressionError):
        evaluate("@utcnow()", ctx)


def test_ingest_pipeline_copies_all_sources(generated, tmp_path, monkeypatch, items):
    raw, manifest = generated
    monkeypatch.setenv("ONELAKE_ROOT", str(tmp_path / "onelake"))
    conns = {"528844c0-a54b-5557-af21-ab2320e35390": {"displayName": "test", "type": "FileServer", "localPath": str(raw)}}
    monkeypatch.setattr(P, "load_connections", lambda path=None: conns)
    run = P.run_pipeline("pl_ingest_sources", {"load_date": "2026-09-30"}, items=items, echo=lambda m: None)
    assert run.status == "Succeeded"
    landed = sorted(p.relative_to(tmp_path / "onelake").as_posix()
                    for p in (tmp_path / "onelake").rglob("landing/**/*") if p.is_file())
    expected = sorted(f"ws_manufacturing/lh_manufacturing.Lakehouse/Files/landing/{t['file'].split('/')[0]}/2026-09-30/"
                      f"{t['file'].split('/')[1]}" for t in manifest["tables"].values())
    assert landed == expected
    assert sum(a.output["filesWritten"] for a in run.activities) == len(manifest["tables"])
    assert list((tmp_path / "onelake").rglob("_pipeline_runs/*.json")), "run log written to Files/_pipeline_runs"


def test_failed_activity_retries_then_skips_downstream(tmp_path, monkeypatch, items):
    """A copy from an empty folder fails, is retried per policy, and its dependant is skipped."""
    monkeypatch.setenv("ONELAKE_ROOT", str(tmp_path / "onelake"))
    (tmp_path / "empty").mkdir()
    conns = {"528844c0-a54b-5557-af21-ab2320e35390": {"displayName": "t", "type": "FileServer", "localPath": str(tmp_path / "empty")}}
    monkeypatch.setattr(P, "load_connections", lambda path=None: conns)
    monkeypatch.setattr(P.time, "sleep", lambda s: None)
    src = P.find_pipeline("pl_ingest_sources", items)
    content = P.load_pipeline_content(src)
    acts = content["properties"]["activities"][:2]
    acts[1]["dependsOn"] = [{"activity": acts[0]["name"], "dependencyConditions": ["Succeeded"]}]
    content["properties"]["activities"] = acts
    folder = tmp_path / "pl_test.DataPipeline"
    folder.mkdir()
    (folder / "pipeline-content.json").write_text(json.dumps(content))
    test_item = P.Item("DataPipeline", "pl_test", "11111111-2222-3333-4444-555555555555", folder)
    all_items = {**items, test_item.logical_id: test_item}
    run = P.run_pipeline(folder, items=all_items, echo=lambda m: None)
    first, second = run.activities
    assert run.status == "Failed"
    assert first.status == "Failed" and first.attempts == acts[0]["policy"]["retry"] + 1
    assert second.status == "Skipped"
