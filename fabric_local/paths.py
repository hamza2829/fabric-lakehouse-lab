"""Where things live, locally and in the OneLake-like folder layout."""
from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = "ws_manufacturing"
LAKEHOUSE = "lh_manufacturing"

DATA_RAW = REPO_ROOT / "data" / "raw"
NOTEBOOKS_DIR = REPO_ROOT / "notebooks"
PIPELINES_DIR = REPO_ROOT / "pipelines"
POWERBI_DIR = REPO_ROOT / "powerbi"
REPORTS_DIR = REPO_ROOT / "reports"


def onelake_root() -> Path:
    """Root of the local stand-in for OneLake (``onelake/`` in the repo unless overridden)."""
    return Path(os.environ.get("ONELAKE_ROOT", REPO_ROOT / "onelake")).resolve()


def lakehouse_root() -> Path:
    """``onelake/<workspace>/<lakehouse>.Lakehouse`` mirrors the OneLake URL structure."""
    return onelake_root() / WORKSPACE / f"{LAKEHOUSE}.Lakehouse"


def files_dir() -> Path:
    return lakehouse_root() / "Files"


def tables_dir() -> Path:
    return lakehouse_root() / "Tables"


def table_path(name: str) -> Path:
    return tables_dir() / name


def is_delta_table(path: Path) -> bool:
    return (path / "_delta_log").is_dir()
