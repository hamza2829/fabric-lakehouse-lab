"""Validate every Data Pipeline definition (and its schedule) plus every notebook.

Usage: python pipelines/validate.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fabric_local.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main(["validate"]))
