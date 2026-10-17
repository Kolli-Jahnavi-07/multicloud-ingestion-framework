"""Load and validate config/sources.yml."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

TYPES = {"string", "int", "decimal", "bool", "timestamp", "date"}
SOURCE_TYPES = {"rest_api", "sql", "files"}
_ENV = re.compile(r"\$\{([A-Z0-9_]+)(?::-([^}]*))?\}")


def expand_env(value: Any) -> Any:
    """Replace ${NAME} / ${NAME:-default} in every string of a parsed YAML document."""
    if isinstance(value, str):

        def sub(m: re.Match) -> str:
            name, default = m.group(1), m.group(2)
            if name in os.environ:
                return os.environ[name]
            if default is None:
                raise KeyError(f"environment variable {name} is not set and has no default")
            return default

        return _ENV.sub(sub, value)
    if isinstance(value, dict):
        return {k: expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [expand_env(v) for v in value]
    return value


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool = True
    min: float | None = None
    max: float | None = None
    allowed: tuple[str, ...] | None = None


@dataclass(frozen=True)
class Contract:
    columns: tuple[Column, ...]
    on_new_columns: str = "evolve"  # evolve | fail

    def column(self, name: str) -> Column | None:
        return next((c for c in self.columns if c.name == name), None)


@dataclass(frozen=True)
class Incremental:
    column: str
    lookback_minutes: int = 0


@dataclass(frozen=True)
class SourceConfig:
    name: str
    type: str
    landing: str
    primary_key: tuple[str, ...]
    contract: Contract
    options: dict[str, Any] = field(default_factory=dict)
    incremental: Incremental | None = None
    description: str = ""


@dataclass(frozen=True)
class Config:
    landings: dict[str, str]
    state_uri: str
    database: str
    schema: str
    stages: dict[str, str]
    sources: tuple[SourceConfig, ...]

    def source(self, name: str) -> SourceConfig:
        for s in self.sources:
            if s.name == name:
                return s
        raise KeyError(f"unknown source {name!r}; known: {', '.join(s.name for s in self.sources)}")


def load(path: Path | str = "config/sources.yml") -> Config:
    raw = expand_env(yaml.safe_load(Path(path).read_text()))
    errors: list[str] = []
    sources = []
    for s in raw.get("sources", []):
        name = s.get("name", "?")
        if s.get("type") not in SOURCE_TYPES:
            errors.append(f"{name}: type must be one of {sorted(SOURCE_TYPES)}")
        if s.get("landing") not in raw.get("landings", {}):
            errors.append(f"{name}: landing {s.get('landing')!r} is not defined under landings")
        cols = []
        for col_name, spec in (s.get("contract", {}).get("columns") or {}).items():
            spec = spec or {}
            if spec.get("type") not in TYPES:
                errors.append(f"{name}.{col_name}: type must be one of {sorted(TYPES)}")
            cols.append(
                Column(
                    name=col_name,
                    type=spec.get("type", "string"),
                    nullable=spec.get("nullable", True),
                    min=spec.get("min"),
                    max=spec.get("max"),
                    allowed=tuple(spec["allowed"]) if spec.get("allowed") else None,
                )
            )
        contract = Contract(tuple(cols), s.get("contract", {}).get("on_new_columns", "evolve"))
        if contract.on_new_columns not in {"evolve", "fail"}:
            errors.append(f"{name}: on_new_columns must be evolve or fail")
        pk = tuple(s.get("primary_key", []))
        if not pk:
            errors.append(f"{name}: primary_key is required (loads are MERGEs)")
        for k in pk:
            col = contract.column(k)
            if col is None:
                errors.append(f"{name}: primary key column {k} is not in the contract")
            elif col.nullable:
                errors.append(f"{name}: primary key column {k} must be nullable: false")
        inc = s.get("incremental")
        incremental = Incremental(inc["column"], int(inc.get("lookback_minutes", 0))) if inc else None
        if incremental and not contract.column(incremental.column):
            errors.append(f"{name}: incremental column {incremental.column} is not in the contract")
        sources.append(
            SourceConfig(
                name=name,
                type=s.get("type", ""),
                landing=s.get("landing", ""),
                primary_key=pk,
                contract=contract,
                options=s.get("options", {}),
                incremental=incremental,
                description=s.get("description", ""),
            )
        )
    names = [s.name for s in sources]
    if len(names) != len(set(names)):
        errors.append("source names must be unique")
    if errors:
        raise ValueError("invalid config:\n  " + "\n  ".join(errors))
    wh = raw.get("warehouse", {})
    return Config(
        landings=raw["landings"],
        state_uri=raw.get("state", "data/lake/_state"),
        database=wh.get("database", "RAW"),
        schema=wh.get("schema", "RAW"),
        stages=wh.get("stages", {}),
        sources=tuple(sources),
    )
