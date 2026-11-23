"""A deliberately small evaluator for Data Pipeline expressions.

Supported, which is everything the pipelines in this repo use:

* ``@pipeline().parameters.<name>``, ``@pipeline().RunId``, ``@pipeline().Pipeline``,
  ``@pipeline().TriggerTime``
* ``@concat(a, b, ...)``, ``@toUpper(x)``, ``@toLower(x)``, ``@string(x)``
* ``@activity('<name>').output.<path>``
* single quoted string literals (``''`` escapes a quote) and integers
* string interpolation: ``"landing/@{pipeline().parameters.load_date}"``

Anything else raises ``ExpressionError``, which the validator reports.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any


class ExpressionError(ValueError):
    pass


@dataclass
class EvalContext:
    parameters: dict[str, Any] = field(default_factory=dict)
    run_id: str = "local-run"
    pipeline_name: str = ""
    trigger_time: str = ""
    activity_outputs: dict[str, Any] = field(default_factory=dict)


FUNCS = {
    "concat": lambda *a: "".join("" if x is None else str(x) for x in a),
    "toUpper": lambda x: str(x).upper(),
    "toLower": lambda x: str(x).lower(),
    "string": lambda x: "" if x is None else str(x),
}

_TOKEN = re.compile(r"\s*(?:(?P<str>'(?:[^']|'')*')|(?P<num>-?\d+)|(?P<name>[A-Za-z_][A-Za-z0-9_]*)|(?P<op>[().,\[\]]))")


class _Parser:
    def __init__(self, text: str, ctx: EvalContext):
        self.toks = []
        pos = 0
        text = text.strip()
        while pos < len(text):
            m = _TOKEN.match(text, pos)
            if not m or m.end() == pos:
                raise ExpressionError(f"cannot parse expression near {text[pos:]!r}")
            kind = m.lastgroup
            self.toks.append((kind, m.group(kind)))
            pos = m.end()
        self.i = 0
        self.ctx = ctx

    def peek(self):
        return self.toks[self.i] if self.i < len(self.toks) else (None, None)

    def take(self, value=None):
        tok = self.peek()
        if tok[0] is None or (value is not None and tok[1] != value):
            raise ExpressionError(f"expected {value!r}, got {tok[1]!r}")
        self.i += 1
        return tok

    def parse(self):
        v = self.expr()
        if self.i != len(self.toks):
            raise ExpressionError(f"unexpected token {self.peek()[1]!r}")
        return v

    def args(self):
        self.take("(")
        out = []
        if self.peek()[1] != ")":
            out.append(self.expr())
            while self.peek()[1] == ",":
                self.take(",")
                out.append(self.expr())
        self.take(")")
        return out

    def expr(self):
        kind, val = self.take()
        if kind == "str":
            return val[1:-1].replace("''", "'")
        if kind == "num":
            return int(val)
        if kind != "name":
            raise ExpressionError(f"unexpected {val!r}")
        if val == "pipeline":
            self.take("(")
            self.take(")")
            self.take(".")
            _, attr = self.take()
            if attr == "parameters":
                self.take(".")
                _, pname = self.take()
                if pname not in self.ctx.parameters:
                    raise ExpressionError(f"unknown pipeline parameter {pname!r}")
                return self.ctx.parameters[pname]
            simple = {"RunId": self.ctx.run_id, "Pipeline": self.ctx.pipeline_name,
                      "TriggerTime": self.ctx.trigger_time}
            if attr not in simple:
                raise ExpressionError(f"unsupported pipeline() property {attr!r}")
            return simple[attr]
        if val == "activity":
            (name,) = self.args()
            if name not in self.ctx.activity_outputs:
                raise ExpressionError(f"activity {name!r} has no output yet")
            cur: Any = self.ctx.activity_outputs[name]
            while self.peek()[1] == ".":
                self.take(".")
                _, key = self.take()
                if not isinstance(cur, dict) or key not in cur:
                    raise ExpressionError(f"activity output has no field {key!r}")
                cur = cur[key]
            return cur
        if val in FUNCS:
            return FUNCS[val](*self.args())
        raise ExpressionError(f"unsupported function {val!r}")


def evaluate(text: str, ctx: EvalContext) -> Any:
    """Evaluate one expression string (with or without the leading ``@``)."""
    if text.startswith("@{") or "@{" in text:
        return re.sub(r"@\{([^}]*)\}", lambda m: str(_Parser(m.group(1), ctx).parse()), text)
    if text.startswith("@@"):
        return text[1:]
    if text.startswith("@"):
        return _Parser(text[1:], ctx).parse()
    return text


def resolve(value: Any, ctx: EvalContext) -> Any:
    """Resolve the ``{"value": ..., "type": "Expression"}`` wrappers pipelines use."""
    if isinstance(value, dict):
        if value.get("type") == "Expression" and "value" in value:
            return evaluate(str(value["value"]), ctx)
        return {k: resolve(v, ctx) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve(v, ctx) for v in value]
    if isinstance(value, str) and value.startswith("@"):
        return evaluate(value, ctx)
    return value
