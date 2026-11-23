from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from fabric_local import paths  # noqa: E402


def lakehouse_built() -> bool:
    return paths.is_delta_table(paths.table_path("gold_kpi_summary"))


@pytest.fixture(autouse=True)
def _needs_lakehouse(request):
    """Tests marked ``lakehouse`` need the pipeline output. Locally they skip when it is
    missing; in CI (REQUIRE_LAKEHOUSE=1) a missing lakehouse is a failure."""
    if request.node.get_closest_marker("lakehouse") and not lakehouse_built():
        if os.environ.get("REQUIRE_LAKEHOUSE") == "1":
            pytest.fail("lakehouse not built but REQUIRE_LAKEHOUSE=1")
        pytest.skip("run the pipeline first: python pipelines/run.py pl_lakehouse_daily")


@pytest.fixture(scope="session")
def generated(tmp_path_factory):
    """Generate the synthetic sources once into a temp folder."""
    from data.generate_data import generate

    out = tmp_path_factory.mktemp("raw")
    manifest = generate(out, 42)
    return out, manifest


@pytest.fixture(scope="session")
def repo_manifest():
    p = paths.DATA_RAW / "_manifest.json"
    if not p.exists():
        pytest.skip("data/raw not generated")
    return json.loads(p.read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def delta():
    from quality.run_quality import read_delta

    cache: dict = {}

    def load(name: str):
        if name not in cache:
            cache[name] = read_delta(name)
        return cache[name]

    return load


@pytest.fixture(scope="session")
def model():
    from fabric_local.tmdl import load_model

    return load_model(paths.POWERBI_DIR / "ManufacturingKPIs.SemanticModel")
