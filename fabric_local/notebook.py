"""Parse and execute Fabric notebooks stored in the Fabric git format.

Fabric serialises a notebook as ``<name>.Notebook/notebook-content.py``: a plain
Python file whose cells are separated by marker comments::

    # Fabric notebook source
    # METADATA ********************     notebook level metadata (kernel, lakehouse)
    # MARKDOWN ********************     markdown cell, every line prefixed with "# "
    # PARAMETERS CELL ********************
    # CELL ********************          code cell
    # METADATA ********************     per cell metadata: language, language_group

Spark SQL cells are code cells whose lines carry the ``# MAGIC`` prefix and start
with ``%%sql``. This module turns that file into cells and runs them the way a
Fabric Spark session would: ``spark`` and ``notebookutils`` are predefined,
pipeline parameters are injected right after the parameters cell, and
``notebookutils.notebook.exit(value)`` stops the run and returns ``value``.
"""
from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

HEADER = "# Fabric notebook source"
MARKER_RE = re.compile(r"^# (CELL|MARKDOWN|PARAMETERS CELL|METADATA) \*{20}\s*$")


@dataclass
class Cell:
    kind: str                      # "code" | "markdown" | "parameters"
    source: str
    language: str = "python"       # "python" | "sparksql" | "markdown"
    metadata: dict = field(default_factory=dict)
    index: int = 0


@dataclass
class Notebook:
    path: Path
    metadata: dict
    cells: list[Cell]

    @property
    def name(self) -> str:
        return self.path.parent.name.removesuffix(".Notebook")

    @property
    def parameters_cell(self) -> Cell | None:
        return next((c for c in self.cells if c.kind == "parameters"), None)

    def code_cells(self) -> list[Cell]:
        return [c for c in self.cells if c.kind in ("code", "parameters")]


class NotebookFormatError(ValueError):
    pass


class NotebookExit(Exception):
    """Raised by ``notebookutils.notebook.exit``; carries the exit value."""

    def __init__(self, value: str):
        super().__init__(value)
        self.value = value


def _meta_json(lines: list[str]) -> dict:
    body = []
    for ln in lines:
        if not ln.strip():
            continue
        if not ln.startswith("# META"):
            raise NotebookFormatError(f"metadata line without '# META' prefix: {ln!r}")
        body.append(ln[len("# META"):].removeprefix(" "))
    return json.loads("\n".join(body)) if body else {}


def _strip_blank(lines: list[str]) -> list[str]:
    while lines and not lines[0].strip():
        lines = lines[1:]
    while lines and not lines[-1].strip():
        lines = lines[:-1]
    return lines


def parse_notebook_text(text: str, path: Path | str = "<memory>") -> Notebook:
    lines = text.splitlines()
    if not lines or lines[0].strip() != HEADER:
        raise NotebookFormatError(f"{path}: first line must be '{HEADER}'")
    blocks: list[tuple[str, list[str]]] = []
    current: tuple[str, list[str]] | None = None
    for ln in lines[1:]:
        m = MARKER_RE.match(ln)
        if m:
            current = (m.group(1), [])
            blocks.append(current)
        elif current is None:
            if ln.strip():
                raise NotebookFormatError(f"{path}: content before the first cell marker: {ln!r}")
        else:
            current[1].append(ln)

    nb_meta: dict = {}
    cells: list[Cell] = []
    for i, (kind, body) in enumerate(blocks):
        if kind == "METADATA":
            meta = _meta_json(body)
            if i == 0:
                nb_meta = meta
            elif cells and blocks[i - 1][0] != "METADATA":
                cells[-1].metadata = meta
                lang = meta.get("language")
                if lang and cells[-1].kind != "markdown":
                    cells[-1].language = lang
            else:
                raise NotebookFormatError(f"{path}: METADATA block #{i} does not follow a cell")
            continue
        body = _strip_blank(body)
        if kind == "MARKDOWN":
            src = "\n".join(ln[2:] if ln.startswith("# ") else ln.lstrip("#") for ln in body)
            cells.append(Cell("markdown", src, "markdown", index=len(cells)))
            continue
        cell_kind = "parameters" if kind == "PARAMETERS CELL" else "code"
        if body and all(ln.startswith("# MAGIC") or not ln.strip() for ln in body):
            body = [ln[len("# MAGIC"):].removeprefix(" ") for ln in body]
            if body[0].strip().lower() == "%%sql":
                cells.append(Cell(cell_kind, "\n".join(body[1:]).strip(), "sparksql", index=len(cells)))
                continue
            raise NotebookFormatError(f"{path}: unsupported magic {body[0]!r}")
        cells.append(Cell(cell_kind, "\n".join(body), "python", index=len(cells)))
    if sum(c.kind == "parameters" for c in cells) > 1:
        raise NotebookFormatError(f"{path}: more than one parameters cell")
    return Notebook(Path(path), nb_meta, cells)


def load_notebook(path: Path) -> Notebook:
    path = Path(path)
    if path.is_dir():
        path = path / "notebook-content.py"
    return parse_notebook_text(path.read_text(encoding="utf-8"), path)


def split_sql(sql: str) -> list[str]:
    """Split a SQL cell into statements on semicolons outside quotes and comments."""
    stmts, buf, quote, i = [], [], None, 0
    while i < len(sql):
        ch = sql[i]
        if quote:
            buf.append(ch)
            if ch == quote:
                quote = None
        elif ch in ("'", '"', "`"):
            quote = ch
            buf.append(ch)
        elif sql.startswith("--", i):
            j = sql.find("\n", i)
            j = len(sql) if j < 0 else j
            i = j
            continue
        elif ch == ";":
            stmts.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
        i += 1
    stmts.append("".join(buf))
    return [s.strip() for s in stmts if s.strip()]


def format_parameter_cell(params: dict[str, Any]) -> str:
    """What Fabric injects after the parameters cell when a pipeline passes values."""
    return "\n".join(f"{k} = {v!r}" for k, v in params.items())


@dataclass
class CellRun:
    index: int
    kind: str
    language: str
    seconds: float


@dataclass
class NotebookResult:
    notebook: str
    exit_value: str | None
    cells: list[CellRun]
    seconds: float

    def exit_json(self) -> Any:
        return json.loads(self.exit_value) if self.exit_value else None


def run_notebook(nb: Notebook, spark, notebookutils, parameters: dict[str, Any] | None = None,
                 after_cell: Callable[[], None] | None = None, display: Callable | None = None,
                 echo: Callable[[str], None] = print) -> NotebookResult:
    """Execute every code cell in order inside one namespace, like a Spark session."""
    ns: dict[str, Any] = {
        "__name__": "__fabric_notebook__",
        "spark": spark,
        "notebookutils": notebookutils,
        "mssparkutils": notebookutils,
        "display": display or (lambda df, *a, **k: df.show(20, truncate=False)),
    }
    runs: list[CellRun] = []
    t_nb = time.perf_counter()
    exit_value = None
    try:
        for cell in nb.code_cells():
            t0 = time.perf_counter()
            if cell.language == "sparksql":
                last = None
                for stmt in split_sql(cell.source):
                    last = spark.sql(stmt)
                if last is not None and last.columns:
                    ns["display"](last)
            else:
                code = compile(cell.source, f"{nb.name}[cell {cell.index}]", "exec")
                exec(code, ns)
            if cell.kind == "parameters" and parameters:
                echo(f"  [{nb.name}] injected parameters: {parameters}")
                exec(compile(format_parameter_cell(parameters), f"{nb.name}[injected]", "exec"), ns)
            runs.append(CellRun(cell.index, cell.kind, cell.language, time.perf_counter() - t0))
            if after_cell:
                after_cell()
    except NotebookExit as ex:
        exit_value = ex.value
        if after_cell:
            after_cell()
    except Exception as ex:
        failed = len(runs)
        cell = nb.code_cells()[failed] if failed < len(nb.code_cells()) else None
        where = f"cell {cell.index} ({cell.language})" if cell else "after the last cell"
        raise RuntimeError(f"{nb.name} failed in {where}: {type(ex).__name__}: {ex}") from ex
    return NotebookResult(nb.name, exit_value, runs, time.perf_counter() - t_nb)
