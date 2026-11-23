"""A small reader for the TMDL files of a Power BI project (PBIP).

It understands what this repo's semantic model uses: tables with columns,
measures (single and multi line), Direct Lake entity partitions, and
relationships. Indentation is by tabs, as Power BI Desktop writes it.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

NAME = r"('(?:[^']|'')*'|[^\s=:']+)"
OBJ_RE = re.compile(rf"^(table|column|measure|partition|relationship|expression)\s+{NAME}\s*(=\s*(.*))?$")
PROP_RE = re.compile(r"^([A-Za-z][A-Za-z0-9]*)\s*:\s*(.*)$")
FLAG_RE = re.compile(r"^([A-Za-z][A-Za-z0-9]*)$")
COLREF_RE = re.compile(r"('(?:[^']|'')+'|[A-Za-z_][A-Za-z0-9_]*)\[([^\]]+)\]")
MEASREF_RE = re.compile(r"\[([^\]]+)\]")


def unquote(name: str) -> str:
    name = name.strip()
    if name.startswith("'") and name.endswith("'"):
        return name[1:-1].replace("''", "'")
    return name


@dataclass
class Column:
    name: str
    props: dict = field(default_factory=dict)

    @property
    def data_type(self) -> str:
        return self.props.get("dataType", "")

    @property
    def source_column(self) -> str:
        return self.props.get("sourceColumn", self.name)


@dataclass
class Measure:
    name: str
    expression: str
    table: str
    props: dict = field(default_factory=dict)
    description: str = ""

    def column_refs(self) -> list[tuple[str, str]]:
        return [(unquote(t), c) for t, c in COLREF_RE.findall(self.expression)]

    def measure_refs(self) -> list[str]:
        stripped = COLREF_RE.sub("", self.expression)
        return MEASREF_RE.findall(stripped)


@dataclass
class Partition:
    name: str
    kind: str
    props: dict = field(default_factory=dict)


@dataclass
class Table:
    name: str
    columns: dict[str, Column] = field(default_factory=dict)
    measures: dict[str, Measure] = field(default_factory=dict)
    partitions: list[Partition] = field(default_factory=list)
    props: dict = field(default_factory=dict)


@dataclass
class Relationship:
    name: str
    props: dict = field(default_factory=dict)

    def _split(self, key: str) -> tuple[str, str]:
        t, c = self.props[key].rsplit(".", 1)
        return unquote(t), unquote(c)

    @property
    def from_table(self) -> str:
        return self._split("fromColumn")[0]

    @property
    def from_column(self) -> str:
        return self._split("fromColumn")[1]

    @property
    def to_table(self) -> str:
        return self._split("toColumn")[0]

    @property
    def to_column(self) -> str:
        return self._split("toColumn")[1]

    @property
    def is_active(self) -> bool:
        return str(self.props.get("isActive", "true")).lower() != "false"


@dataclass
class SemanticModel:
    tables: dict[str, Table]
    relationships: list[Relationship]
    model_props: dict = field(default_factory=dict)

    @property
    def measures(self) -> dict[str, Measure]:
        return {m.name: m for t in self.tables.values() for m in t.measures.values()}


def _indent(line: str) -> int:
    return len(line) - len(line.lstrip("\t"))


def parse_tmdl(text: str, path: str = "<memory>") -> tuple[list[Table], list[Relationship], dict]:
    lines = text.replace("\r\n", "\n").split("\n")
    tables: list[Table] = []
    rels: list[Relationship] = []
    model_props: dict = {}
    table: Table | None = None
    obj = None                      # current second level object (column/measure/partition) or relationship
    pending_doc: list[str] = []
    i = 0
    while i < len(lines):
        raw = lines[i]
        i += 1
        if not raw.strip():
            continue
        ind = _indent(raw)
        s = raw.strip()
        if s.startswith("///"):
            pending_doc.append(s[3:].strip())
            continue
        if ind == 0:
            obj = None
            m = OBJ_RE.match(s)
            if m and m.group(1) == "table":
                table = Table(unquote(m.group(2)))
                tables.append(table)
            elif m and m.group(1) == "relationship":
                table = None
                obj = Relationship(unquote(m.group(2)))
                rels.append(obj)
            else:
                table = None
                if s.startswith("model "):
                    model_props["name"] = s.split(" ", 1)[1]
            pending_doc = []
            continue
        if ind == 1 and table is not None:
            m = OBJ_RE.match(s)
            if m and m.group(1) == "column":
                obj = Column(unquote(m.group(2)))
                table.columns[obj.name] = obj
            elif m and m.group(1) == "measure":
                expr = (m.group(4) or "").strip()
                if not expr:  # multi line expression, indented deeper than the properties
                    body = []
                    while i < len(lines) and (not lines[i].strip() or _indent(lines[i]) >= 3):
                        body.append(lines[i].strip("\n")[3:] if _indent(lines[i]) >= 3 else "")
                        i += 1
                    expr = "\n".join(body).strip()
                if expr.startswith("```"):
                    expr = expr.strip("`").strip()
                obj = Measure(unquote(m.group(2)), expr, table.name, description=" ".join(pending_doc))
                table.measures[obj.name] = obj
            elif m and m.group(1) == "partition":
                obj = Partition(unquote(m.group(2)), (m.group(4) or "").strip())
                table.partitions.append(obj)
            else:
                obj = None
                pm = PROP_RE.match(s)
                if pm:
                    table.props[pm.group(1)] = pm.group(2).strip()
            pending_doc = []
            continue
        if ind == 1 and isinstance(obj, Relationship):
            pm = PROP_RE.match(s)
            if pm:
                obj.props[pm.group(1)] = pm.group(2).strip()
            continue
        if ind == 1 and model_props.get("name"):
            pm = PROP_RE.match(s)
            if pm:
                model_props[pm.group(1)] = pm.group(2).strip()
            continue
        if ind >= 2 and obj is not None and not isinstance(obj, Relationship):
            pm = PROP_RE.match(s)
            fm = FLAG_RE.match(s)
            if pm:
                obj.props[pm.group(1)] = pm.group(2).strip()
            elif fm:
                obj.props[fm.group(1)] = True
            continue
    return tables, rels, model_props


def load_model(semantic_model_dir: Path) -> SemanticModel:
    d = Path(semantic_model_dir) / "definition"
    tables: dict[str, Table] = {}
    rels: list[Relationship] = []
    props: dict = {}
    for f in [d / "model.tmdl", d / "relationships.tmdl", *sorted((d / "tables").glob("*.tmdl"))]:
        if not f.exists():
            continue
        ts, rs, mp = parse_tmdl(f.read_text(encoding="utf-8"), str(f))
        for t in ts:
            if t.name in tables:
                raise ValueError(f"table {t.name} defined twice")
            tables[t.name] = t
        rels += rs
        props.update(mp)
    return SemanticModel(tables, rels, props)
