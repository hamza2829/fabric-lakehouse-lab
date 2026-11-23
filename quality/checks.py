"""Data quality check primitives on pandas DataFrames.

Each check returns a ``CheckResult`` with the number of failing rows, so a check
can run on bronze (strings exactly as delivered) and on silver (typed and
cleaned) alike. Values are coerced inside the check; anything that does not
parse counts as a failure of that check, never as a crash.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

import pandas as pd

OPS = {
    "<": lambda a, b: a < b,
    "<=": lambda a, b: a <= b,
    ">": lambda a, b: a > b,
    ">=": lambda a, b: a >= b,
}


@dataclass
class CheckResult:
    check: str                 # check type, e.g. "unique"
    table: str                 # logical entity, e.g. "orders"
    source: str                # source system: erp | production | quality
    layer: str                 # bronze | silver
    issue: str                 # issue category reported per source
    severity: str              # error | warn | info
    failed_rows: int
    total_rows: int
    detail: str = ""
    examples: list = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return self.failed_rows == 0

    @property
    def rate(self) -> float:
        return self.failed_rows / self.total_rows if self.total_rows else 0.0


def _blank(s: pd.Series) -> pd.Series:
    return s.isna() | (s.astype("string").str.strip() == "")


def to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def to_ts(s: pd.Series) -> pd.Series:
    out = pd.to_datetime(s.astype("string"), errors="coerce", utc=True, format="mixed")
    return out.dt.tz_localize(None)


def _examples(df: pd.DataFrame, mask: pd.Series, cols: list[str], n: int = 3) -> list:
    if not mask.any():
        return []
    cols = [c for c in cols if c in df.columns]
    return df.loc[mask, cols].head(n).astype("string").fillna("<null>").to_dict("records")


def not_null(df: pd.DataFrame, columns: list[str]) -> tuple[int, str, list]:
    mask = pd.Series(False, index=df.index)
    for c in columns:
        mask |= _blank(df[c])
    return int(mask.sum()), f"NULL or empty in {', '.join(columns)}", _examples(df, mask, columns)


def unique(df: pd.DataFrame, columns: list[str]) -> tuple[int, str, list]:
    """Rows beyond the first per business key (rows with a NULL key part are ignored)."""
    keyed = df.loc[~pd.concat([_blank(df[c]) for c in columns], axis=1).any(axis=1)]
    dup = keyed.duplicated(subset=columns, keep="first")
    return int(dup.sum()), f"duplicate key ({', '.join(columns)})", _examples(keyed, dup, columns)


def value_range(df: pd.DataFrame, column: str, min_value=None, max_value=None,
                exclusive_min: bool = False) -> tuple[int, str, list]:
    raw = df[column]
    v = to_num(raw)
    mask = v.isna() & ~_blank(raw)            # present but not numeric
    if min_value is not None:
        mask |= (v <= min_value) if exclusive_min else (v < min_value)
    if max_value is not None:
        mask |= v > max_value
    lo = "(" if exclusive_min else "["
    return int(mask.fillna(False).sum()), f"{column} outside {lo}{min_value}, {max_value}]", \
        _examples(df, mask.fillna(False), [column])


def compare(df: pd.DataFrame, left: str, op: str, right: str) -> tuple[int, str, list]:
    a, b = to_num(df[left]), to_num(df[right])
    ok = OPS[op](a, b)
    mask = ~ok & a.notna() & b.notna()
    return int(mask.sum()), f"violates {left} {op} {right}", _examples(df, mask, [left, right])


def allowed_values(df: pd.DataFrame, column: str, values: list[str]) -> tuple[int, str, list]:
    s = df[column]
    mask = ~_blank(s) & ~s.astype("string").isin(values)
    return int(mask.sum()), f"{column} not in {values}", _examples(df, mask, [column])


def referential(df: pd.DataFrame, columns: list[str], ref: pd.DataFrame, ref_columns: list[str]) -> tuple[int, str, list]:
    """Rows whose (non NULL) key has no match in the referenced table."""
    present = ~pd.concat([_blank(df[c]) for c in columns], axis=1).any(axis=1)
    left = df.loc[present, columns].astype("string")
    right = ref[ref_columns].astype("string").drop_duplicates()
    right.columns = columns
    merged = left.merge(right.assign(__hit=True), on=columns, how="left")
    miss = merged["__hit"].isna().to_numpy()
    mask = pd.Series(False, index=df.index)
    mask.loc[left.index[miss]] = True
    return int(mask.sum()), f"({', '.join(columns)}) not found in {', '.join(ref_columns)}", \
        _examples(df, mask, columns)


def not_in_future(df: pd.DataFrame, column: str, as_of: datetime) -> tuple[int, str, list]:
    t = to_ts(df[column])
    mask = (t > pd.Timestamp(as_of)).fillna(False)
    return int(mask.sum()), f"{column} after as-of {as_of:%Y-%m-%d %H:%M}", _examples(df, mask, [column])


def late_arrival(df: pd.DataFrame, recorded: str, event: str, max_hours: float) -> tuple[int, str, list]:
    lag = (to_ts(df[recorded]) - to_ts(df[event])).dt.total_seconds() / 3600
    mask = (lag > max_hours).fillna(False)
    return int(mask.sum()), f"{recorded} more than {max_hours:g} h after {event}", \
        _examples(df, mask, [event, recorded])


def freshness(df: pd.DataFrame, column: str, as_of: datetime, max_age_hours: float) -> tuple[int, str, list]:
    """Fails (one 'row', the table) if the newest record is older than the SLA."""
    t = to_ts(df[column])
    t = t[t <= pd.Timestamp(as_of)]
    if t.empty:
        return 1, f"no {column} values at or before as-of", []
    age = (pd.Timestamp(as_of) - t.max()).total_seconds() / 3600
    detail = f"newest {column} {t.max():%Y-%m-%d %H:%M}, age {age:.1f} h (SLA {max_age_hours:g} h)"
    return int(age > max_age_hours), detail, []
