"""Data contracts: coerce extracted records to declared types and decide what lands.

For every record:
  * values are cast to the contract type (strings like "720" or "2026-03-01T10:00:00Z" are fine)
  * a value that can't be cast, a null in a non-nullable column, or a value outside min/max/allowed
    sends the record to quarantine with the reason, instead of failing the whole run
  * duplicate primary keys within one extract keep the newest version

For the batch as a whole:
  * a required column missing from the source fails the run (an upstream breaking change)
  * a column not in the contract is kept as text and reported (`evolve`) or fails the run (`fail`)
  * if more than `max_reject_fraction` of records are rejected, the run fails: something upstream
    is badly wrong and landing the remainder would hide it
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import pyarrow as pa

from .config import Column, SourceConfig

ARROW = {
    "string": pa.string(),
    "int": pa.int64(),
    "decimal": pa.decimal128(18, 4),
    "bool": pa.bool_(),
    "timestamp": pa.timestamp("us", tz="UTC"),
    "date": pa.date32(),
}

_TRUE = {"true", "t", "yes", "y", "1"}
_FALSE = {"false", "f", "no", "n", "0"}


class ContractViolation(Exception):
    """The batch as a whole breaks the contract; nothing from it is landed."""


def _cast(value: Any, col: Column) -> Any:
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return None
    t = col.type
    if t == "string":
        return str(value).strip()
    if t == "int":
        if isinstance(value, bool):
            raise ValueError("bool is not an int")
        d = Decimal(str(value).strip())
        if d != d.to_integral_value():
            raise ValueError("not a whole number")
        return int(d)
    if t == "decimal":
        try:
            return Decimal(str(value).strip()).quantize(Decimal("0.0001"))
        except InvalidOperation as e:
            raise ValueError("not a number") from e
    if t == "bool":
        if isinstance(value, bool):
            return value
        s = str(value).strip().lower()
        if s in _TRUE:
            return True
        if s in _FALSE:
            return False
        raise ValueError("not a boolean")
    if t == "timestamp":
        if isinstance(value, datetime):
            ts = value
        else:
            ts = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        return ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts.astimezone(UTC)
    if t == "date":
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        return date.fromisoformat(str(value).strip()[:10])
    raise ValueError(f"unknown type {t}")


def _check(value: Any, col: Column) -> str | None:
    if value is None:
        return None if col.nullable else f"{col.name} is required"
    if col.min is not None and value < col.min:
        return f"{col.name}={value} is below {col.min}"
    if col.max is not None and value > col.max:
        return f"{col.name}={value} is above {col.max}"
    if col.allowed is not None and value not in col.allowed:
        return f"{col.name}={value!r} is not one of {list(col.allowed)}"
    return None


@dataclass
class ValidationResult:
    table: pa.Table
    rejected: list[dict] = field(default_factory=list)
    new_columns: list[str] = field(default_factory=list)
    duplicates_dropped: int = 0

    @property
    def accepted(self) -> int:
        return self.table.num_rows


def validate(records: list[dict], source: SourceConfig, max_reject_fraction: float = 0.05) -> ValidationResult:
    contract = source.contract
    declared = [c.name for c in contract.columns]
    seen: set[str] = set()
    for r in records:
        seen.update(r)
    if records:
        missing = [c.name for c in contract.columns if c.name not in seen and not c.nullable]
        if missing:
            raise ContractViolation(f"{source.name}: required columns missing from source: {', '.join(missing)}")
    new_columns = sorted(seen - set(declared))
    if new_columns and contract.on_new_columns == "fail":
        raise ContractViolation(
            f"{source.name}: unexpected columns {', '.join(new_columns)} (contract says on_new_columns: fail)"
        )

    good: list[dict] = []
    rejected: list[dict] = []
    for r in records:
        row, reason = {}, None
        for col in contract.columns:
            try:
                v = _cast(r.get(col.name), col)
            except (ValueError, TypeError) as e:
                reason = f"{col.name}={r.get(col.name)!r}: {e}"
                break
            reason = _check(v, col)
            if reason:
                break
            row[col.name] = v
        if reason:
            rejected.append({"record": {k: (None if v is None else str(v)) for k, v in r.items()}, "reason": reason})
            continue
        for extra in new_columns:
            v = r.get(extra)
            row[extra] = None if v is None else str(v)
        good.append(row)

    if records and len(rejected) / len(records) > max_reject_fraction:
        raise ContractViolation(
            f"{source.name}: {len(rejected)} of {len(records)} records rejected "
            f"(limit {max_reject_fraction:.0%}); first reason: {rejected[0]['reason']}"
        )

    # Same key twice in one extract (an API page re-read, an overlap window): keep the newest.
    order_col = source.incremental.column if source.incremental else None
    latest: dict[tuple, dict] = {}
    for row in good:
        key = tuple(row[k] for k in source.primary_key)
        cur = latest.get(key)
        if cur is None or (order_col and row[order_col] >= cur[order_col]) or not order_col:
            latest[key] = row
    deduped = list(latest.values())

    schema = pa.schema(
        [pa.field(c.name, ARROW[c.type], nullable=c.nullable) for c in contract.columns]
        + [pa.field(n, pa.string()) for n in new_columns]
    )
    table = pa.Table.from_pylist(deduped, schema=schema)
    return ValidationResult(table, rejected, new_columns, len(good) - len(deduped))
