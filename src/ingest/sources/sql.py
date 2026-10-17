"""Incremental table extract from any SQLAlchemy database (Postgres in production, SQLite in the demo)."""

from __future__ import annotations

import re
from datetime import datetime

import sqlalchemy as sa

from .base import Extract, Source

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")


class SqlSource(Source):
    def __init__(self, cfg, engine: sa.Engine | None = None):
        super().__init__(cfg)
        self.engine = engine or sa.create_engine(cfg.options["url"])
        table = cfg.options["table"]
        if not _IDENT.match(table):
            raise ValueError(f"unsafe table name {table!r}")
        self.table = table

    def extract(self, watermark: datetime | None, state: dict) -> Extract:
        inc = self.cfg.incremental
        query = f"SELECT * FROM {self.table}"
        params = {}
        since = self.lower_bound(watermark)
        if inc and since is not None:
            query += f" WHERE {inc.column} >= :since"
            naive = since.replace(tzinfo=None)  # source columns are UTC without time zone
            # SQLite stores timestamps as ISO text; real databases get a proper timestamp parameter.
            params["since"] = naive.isoformat(sep=" ") if self.engine.dialect.name == "sqlite" else naive
        if inc:
            query += f" ORDER BY {inc.column}"
        records = []
        with self.engine.connect() as con:
            result = con.execution_options(stream_results=True, yield_per=5000).execute(sa.text(query), params)
            for chunk in result.mappings().partitions():
                records.extend(dict(r) for r in chunk)
        return Extract(records, stats={"since": str(params["since"]) if params else None})
