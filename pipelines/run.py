"""Run a Data Pipeline locally, activity by activity, in dependency order.

Usage: python pipelines/run.py pl_lakehouse_daily [--param load_date=2026-09-30]
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fabric_local.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main(["run", *sys.argv[1:]]))
