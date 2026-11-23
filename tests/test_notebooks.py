"""Fabric notebook files: format, parameters, and the local executor."""
from __future__ import annotations

import json
import re

import pytest

from fabric_local import paths
from fabric_local.notebook import (NotebookExit, NotebookFormatError, load_notebook, parse_notebook_text,
                                   run_notebook, split_sql)
from fabric_local.pipeline import GUID_RE, discover_items

NOTEBOOKS = sorted(paths.NOTEBOOKS_DIR.glob("*.Notebook"))


def test_three_medallion_notebooks_exist():
    assert [p.name for p in NOTEBOOKS] == [
        "nb_01_bronze_ingest.Notebook", "nb_02_silver_clean_conform.Notebook", "nb_03_gold_kpis.Notebook"]


@pytest.mark.parametrize("nb_dir", NOTEBOOKS, ids=lambda p: p.name)
def test_notebook_parses_with_one_parameters_cell_first(nb_dir):
    nb = load_notebook(nb_dir)
    code = nb.code_cells()
    assert code and code[0].kind == "parameters"
    assert sum(c.kind == "parameters" for c in nb.cells) == 1
    assert nb.cells[0].kind == "markdown"


@pytest.mark.parametrize("nb_dir", NOTEBOOKS, ids=lambda p: p.name)
def test_notebook_uses_pyspark_and_spark_sql(nb_dir):
    nb = load_notebook(nb_dir)
    langs = {c.language for c in nb.code_cells()}
    assert {"python", "sparksql"} <= langs
    assert all(c.metadata.get("language_group") == "synapse_pyspark" for c in nb.code_cells())


@pytest.mark.parametrize("nb_dir", NOTEBOOKS, ids=lambda p: p.name)
def test_notebook_targets_the_repo_lakehouse_with_relative_paths(nb_dir):
    nb = load_notebook(nb_dir)
    lh = nb.metadata["dependencies"]["lakehouse"]
    items = discover_items()
    assert items[lh["default_lakehouse"]].type == "Lakehouse"
    assert lh["default_lakehouse_name"] == paths.LAKEHOUSE
    text = (nb_dir / "notebook-content.py").read_text(encoding="utf-8")
    assert not re.search(r"abfss://|[A-Z]:\\\\|/home/|/Users/", text), "notebooks must use Files/ and Tables/ only"
    assert "notebookutils.notebook.exit(" in text


def test_items_have_unique_guid_logical_ids():
    items = discover_items()
    assert all(GUID_RE.match(k) for k in items)
    for it in items.values():
        suffix = {"Notebook": ".Notebook", "DataPipeline": ".DataPipeline", "Lakehouse": ".Lakehouse",
                  "SemanticModel": ".SemanticModel", "Report": ".Report"}[it.type]
        assert it.path.name.endswith(suffix)


SAMPLE = """# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {"name": "synapse_pyspark"}
# META }

# MARKDOWN ********************

# # Title
# some text

# PARAMETERS CELL ********************

x = 1
label = "default"

# METADATA ********************

# META {"language": "python", "language_group": "synapse_pyspark"}

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE VIEW a AS SELECT ';' AS s;
# MAGIC SELECT * FROM a

# METADATA ********************

# META {"language": "sparksql", "language_group": "synapse_pyspark"}

# CELL ********************

result = x * 10
notebookutils.notebook.exit(json.dumps({"result": result, "label": label}))
never_reached = True

# METADATA ********************

# META {"language": "python", "language_group": "synapse_pyspark"}
"""


def test_parser_reads_cells_markdown_magic_and_metadata():
    nb = parse_notebook_text(SAMPLE)
    assert nb.metadata["kernel_info"]["name"] == "synapse_pyspark"
    assert [c.kind for c in nb.cells] == ["markdown", "parameters", "code", "code"]
    assert nb.cells[0].source.startswith("# Title")
    assert nb.cells[2].language == "sparksql"
    assert nb.cells[2].source.startswith("CREATE VIEW")


def test_parser_rejects_files_without_header():
    with pytest.raises(NotebookFormatError):
        parse_notebook_text("print('hello')\n")


def test_split_sql_ignores_semicolons_in_strings_and_comments():
    assert split_sql("SELECT ';' AS a; -- c;\nSELECT 2;") == ["SELECT ';' AS a", "SELECT 2"]


class FakeDF:
    columns = ["s"]

    def show(self, *a, **k):
        pass


class FakeSpark:
    def __init__(self):
        self.statements = []

    def sql(self, stmt):
        self.statements.append(stmt)
        return FakeDF()


class FakeUtils:
    class notebook:
        @staticmethod
        def exit(value):
            raise NotebookExit(value)


def test_executor_injects_parameters_runs_sql_and_returns_exit_value():
    nb = parse_notebook_text(SAMPLE)
    nb.cells[-1].source = "import json\n" + nb.cells[-1].source
    spark = FakeSpark()
    res = run_notebook(nb, spark, FakeUtils(), {"x": 4, "label": "from pipeline"}, echo=lambda m: None)
    assert spark.statements == ["CREATE VIEW a AS SELECT ';' AS s", "SELECT * FROM a"]
    assert res.exit_json() == {"result": 40, "label": "from pipeline"}
    assert len(res.cells) == 2  # the exit stops the notebook before its cell completes
