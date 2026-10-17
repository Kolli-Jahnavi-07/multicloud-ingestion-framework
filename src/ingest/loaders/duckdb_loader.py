"""Local warehouse for the demo and tests: the same COPY + MERGE semantics on DuckDB."""

from __future__ import annotations

import io
from dataclasses import dataclass

import duckdb
import pyarrow.parquet as pq

from ..config import SourceConfig
from ..landing import Manifest
from ..storage import Storage

DUCK_TYPES = {
    "string": "VARCHAR",
    "int": "BIGINT",
    "decimal": "DECIMAL(18,4)",
    "bool": "BOOLEAN",
    "timestamp": "TIMESTAMPTZ",
    "date": "DATE",
}


def _q(name: str) -> str:
    return f'"{name}"'


@dataclass
class MergeStats:
    inserted: int = 0
    updated: int = 0


class DuckDBLoader:
    def __init__(self, path: str = ":memory:", schema: str = "raw"):
        self.con = duckdb.connect(path)
        self.schema = schema
        self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        self.con.execute("SET TimeZone = 'UTC'")

    def _ensure_table(self, src: SourceConfig, new_columns: list[str]) -> str:
        t = f"{self.schema}.{src.name}"
        cols = [f'"{c.name}" {DUCK_TYPES[c.type]}' for c in src.contract.columns]
        cols += ['"_run_id" VARCHAR', '"_source_file" VARCHAR', '"_ingested_at" TIMESTAMPTZ']
        self.con.execute(f"CREATE TABLE IF NOT EXISTS {t} ({', '.join(cols)})")
        existing = {r[0] for r in self.con.execute(f"DESCRIBE {t}").fetchall()}
        for c in new_columns:
            if c not in existing:
                self.con.execute(f'ALTER TABLE {t} ADD COLUMN "{c}" VARCHAR')
        return t

    def load(self, src: SourceConfig, m: Manifest, storage: Storage) -> MergeStats:
        t = self._ensure_table(src, m.new_columns)
        cols = [r[0] for r in self.con.execute(f"DESCRIBE {t}").fetchall()]
        data_cols = [c for c in cols if not c.startswith("_")]
        pk = ", ".join(_q(k) for k in src.primary_key)
        inc = src.incremental.column if src.incremental else None
        order = f"{_q(inc)} DESC" if inc else "_source_file DESC"
        latest = f"SELECT * FROM stg QUALIFY ROW_NUMBER() OVER (PARTITION BY {pk} ORDER BY {order}) = 1"
        on = " AND ".join(f"t.{_q(k)} = s.{_q(k)}" for k in src.primary_key)
        newer = f" AND s.{_q(inc)} > t.{_q(inc)}" if inc else ""
        sets = ", ".join(f"{_q(c)} = s.{_q(c)}" for c in cols if c not in src.primary_key)

        self.con.execute("BEGIN")
        try:
            self.con.execute(f"CREATE OR REPLACE TEMP TABLE stg AS SELECT * FROM {t} LIMIT 0")
            for f in m.files:
                landed = pq.read_table(io.BytesIO(storage.get(f["key"])))
                self.con.register("landed_file", landed)
                present = [c for c in data_cols if c in landed.column_names]
                sel = ", ".join(_q(c) for c in present)
                self.con.execute(
                    f"INSERT INTO stg ({sel}, _run_id, _source_file, _ingested_at) SELECT {sel}, ?, ?, now() FROM landed_file",
                    [m.run_id, f["key"]],
                )
                self.con.unregister("landed_file")
            before = self.con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            updated = self.con.execute(f"SELECT COUNT(*) FROM ({latest}) s JOIN {t} t ON {on} WHERE TRUE{newer}").fetchone()[0]
            self.con.execute(
                f"MERGE INTO {t} t USING ({latest}) s ON {on} "
                f"WHEN MATCHED{newer} THEN UPDATE SET {sets} "
                f"WHEN NOT MATCHED THEN INSERT ({', '.join(map(_q, cols))}) VALUES ({', '.join('s.' + _q(c) for c in cols)})"
            )
            after = self.con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            self.con.execute("COMMIT")
        except Exception:
            self.con.execute("ROLLBACK")
            raise
        return MergeStats(inserted=after - before, updated=updated)

    def sql(self, q: str) -> list[tuple]:
        return self.con.execute(q).fetchall()
