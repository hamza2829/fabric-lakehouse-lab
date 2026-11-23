"""Evaluate a small, explicit subset of DAX against pandas DataFrames.

Purpose: prove that the measures in the TMDL files compute the same numbers as
the gold KPI table built by Spark SQL, without Power BI. Only the grand total
(no slicers) is evaluated. Supported:

* ``COUNTROWS(table)``, ``SUM(table[col])``, ``DIVIDE(a, b[, alt])``
* ``CALCULATE(expr, table[col] = <scalar>, ...)`` with simple column filters
* ``TRUE()``, ``FALSE()``, ``BLANK()``, numbers, strings
* ``[Measure]`` references, ``VAR name = expr ... RETURN expr``
* ``+ - * /`` and comparisons ``= <> < <= > >=``

Anything else raises ``DaxError`` so an unsupported measure fails loudly.
"""
from __future__ import annotations

import re
from typing import Any

import pandas as pd

from .tmdl import SemanticModel, unquote


class DaxError(ValueError):
    pass


_TOKEN = re.compile(r"""
    (?P<ws>\s+|//[^\n]*|--[^\n]*)
  | (?P<colref>(?:'(?:[^']|'')+'|[A-Za-z_][A-Za-z0-9_]*)\[[^\]]+\])
  | (?P<measref>\[[^\]]+\])
  | (?P<num>\d+(?:\.\d+)?)
  | (?P<str>"(?:[^"]|"")*")
  | (?P<op><=|>=|<>|&&|\|\||[-+*/=<>(),])
  | (?P<name>[A-Za-z_][A-Za-z0-9_.]*)
""", re.VERBOSE)


def tokenize(text: str) -> list[tuple[str, str]]:
    out, pos = [], 0
    while pos < len(text):
        m = _TOKEN.match(text, pos)
        if not m:
            raise DaxError(f"cannot tokenize DAX near {text[pos:pos + 20]!r}")
        pos = m.end()
        if m.lastgroup != "ws":
            out.append((m.lastgroup, m.group(m.lastgroup)))
    return out


class Evaluator:
    def __init__(self, model: SemanticModel, frames: dict[str, pd.DataFrame]):
        self.model = model
        self.frames = frames

    # ------------------------------------------------------------- public
    def measure(self, name: str, filters: dict | None = None) -> Any:
        m = self.model.measures.get(name)
        if m is None:
            raise DaxError(f"unknown measure [{name}]")
        return self.evaluate(m.expression, filters or {})

    def evaluate(self, expr: str, filters: dict | None = None, variables: dict | None = None) -> Any:
        p = _Parser(tokenize(expr), self, filters or {}, dict(variables or {}))
        v = p.expression()
        if p.i != len(p.toks):
            raise DaxError(f"unexpected token {p.toks[p.i][1]!r} in {expr!r}")
        return v

    # ------------------------------------------------------------ helpers
    def rows(self, table: str, filters: dict) -> pd.DataFrame:
        if table not in self.frames:
            raise DaxError(f"no data for table {table}")
        df = self.frames[table]
        mask = filters.get(table)
        return df if mask is None else df[mask.reindex(df.index, fill_value=False)]

    def column(self, ref: str) -> tuple[str, str]:
        t, c = ref[:-1].split("[", 1)
        t = unquote(t)
        if t not in self.model.tables or c not in self.model.tables[t].columns:
            raise DaxError(f"unknown column {ref}")
        return t, self.model.tables[t].columns[c].source_column


class _Parser:
    def __init__(self, toks, ev: Evaluator, filters: dict, variables: dict):
        self.toks, self.i, self.ev, self.filters, self.vars = toks, 0, ev, filters, variables

    def peek(self, k: int = 0):
        j = self.i + k
        return self.toks[j] if j < len(self.toks) else (None, None)

    def take(self, value: str | None = None):
        tok = self.peek()
        if tok[0] is None or (value is not None and tok[1].upper() != value.upper()):
            raise DaxError(f"expected {value!r}, got {tok[1]!r}")
        self.i += 1
        return tok

    # expression := VAR ... RETURN expr | comparison
    def expression(self):
        if self.peek()[1] and self.peek()[1].upper() == "VAR":
            while self.peek()[1] and self.peek()[1].upper() == "VAR":
                self.take("VAR")
                _, name = self.take()
                self.take("=")
                self.vars[name] = self.expression()
            self.take("RETURN")
        return self.comparison()

    def comparison(self):
        left = self.additive()
        op = self.peek()[1]
        if op in ("=", "<>", "<", "<=", ">", ">="):
            self.take()
            right = self.additive()
            return _compare(left, op, right)
        return left

    def additive(self):
        v = self.term()
        while self.peek()[1] in ("+", "-"):
            op = self.take()[1]
            r = self.term()
            v = None if v is None or r is None else (v + r if op == "+" else v - r)
        return v

    def term(self):
        v = self.factor()
        while self.peek()[1] in ("*", "/"):
            op = self.take()[1]
            r = self.factor()
            if v is None or r is None:
                v = None
            elif op == "*":
                v = v * r
            else:
                if r == 0:
                    raise DaxError("division by zero (use DIVIDE)")
                v = v / r
        return v

    def factor(self):
        kind, val = self.take()
        if kind == "num":
            return float(val)
        if kind == "str":
            return val[1:-1].replace('""', '"')
        if kind == "op" and val == "(":
            v = self.expression()
            self.take(")")
            return v
        if kind == "op" and val == "-":
            v = self.factor()
            return None if v is None else -v
        if kind == "measref":
            return self.ev.measure(val[1:-1], self.filters)
        if kind == "colref":
            raise DaxError(f"bare column reference {val} needs an aggregation")
        if kind == "name":
            up = val.upper()
            if self.peek()[1] == "(":
                return self.call(up)
            if val in self.vars:
                return self.vars[val]
            raise DaxError(f"unknown name {val!r}")
        raise DaxError(f"unexpected {val!r}")

    def call(self, fn: str):
        self.take("(")
        if fn in ("TRUE", "FALSE", "BLANK"):
            self.take(")")
            return {"TRUE": True, "FALSE": False, "BLANK": None}[fn]
        if fn == "COUNTROWS":
            _, t = self.take()
            self.take(")")
            t = unquote(t)
            if t not in self.ev.model.tables:
                raise DaxError(f"unknown table {t}")
            return float(len(self.ev.rows(t, self.filters)))
        if fn == "SUM":
            kind, ref = self.take()
            if kind != "colref":
                raise DaxError("SUM expects a column reference")
            self.take(")")
            t, c = self.ev.column(ref)
            s = pd.to_numeric(self.ev.rows(t, self.filters)[c], errors="coerce")
            return float(s.sum()) if s.notna().any() else None
        if fn == "DIVIDE":
            a = self.expression()
            self.take(",")
            b = self.expression()
            alt = None
            if self.peek()[1] == ",":
                self.take(",")
                alt = self.expression()
            self.take(")")
            if a is None or b in (None, 0):
                return alt
            return a / b
        if fn == "CALCULATE":
            start = self.i
            self._skip_expression_arg()
            filters = {k: v for k, v in self.filters.items()}
            while self.peek()[1] == ",":
                self.take(",")
                kind, ref = self.take()
                if kind != "colref":
                    raise DaxError("CALCULATE filters must be table[column] <op> value")
                op = self.take()[1]
                value = self.additive()
                t, c = self.ev.column(ref)
                col = self.ev.frames[t][c]
                mask = _compare_series(col, op, value)
                filters[t] = mask if t not in filters else (filters[t] & mask)
            self.take(")")
            end = self.i
            sub = _Parser(self.toks[start:], self.ev, filters, self.vars)
            v = sub.expression()
            self.i = end
            return v
        raise DaxError(f"unsupported DAX function {fn}")

    def _skip_expression_arg(self):
        depth = 0
        while True:
            _, v = self.peek()
            if v is None:
                raise DaxError("unterminated CALCULATE")
            if depth == 0 and v in (",", ")"):
                return
            if v == "(":
                depth += 1
            elif v == ")":
                depth -= 1
            self.i += 1


def _compare(a, op, b):
    if a is None or b is None:
        return False
    return {"=": a == b, "<>": a != b, "<": a < b, "<=": a <= b, ">": a > b, ">=": a >= b}[op]


def _compare_series(s: pd.Series, op: str, v) -> pd.Series:
    if isinstance(v, bool):
        s = s.astype("boolean")
    res = {"=": s == v, "<>": s != v, "<": s < v, "<=": s <= v, ">": s > v, ">=": s >= v}[op]
    return res.fillna(False).astype(bool)
