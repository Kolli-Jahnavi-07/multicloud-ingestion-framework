"""Load landed Parquet into Snowflake: COPY INTO a temporary table, then MERGE on the primary key.

The SQL is generated from the source's contract and the run's manifest, so a new source needs no
hand-written load script. `SnowflakeLoader` runs it through snowflake-connector-python;
`render_load_sql` is used on its own for reviews and tests.
"""

from __future__ import annotations

import os

from ..config import Config, SourceConfig
from ..landing import Manifest
from .duckdb_loader import MergeStats

SF_TYPES = {
    "string": "VARCHAR",
    "int": "NUMBER(38,0)",
    "decimal": "NUMBER(18,4)",
    "bool": "BOOLEAN",
    "timestamp": "TIMESTAMP_TZ",
    "date": "DATE",
}
META = [("_run_id", "VARCHAR"), ("_source_file", "VARCHAR"), ("_ingested_at", "TIMESTAMP_TZ")]


def _ident(name: str) -> str:
    return name.upper()


def table_name(cfg: Config, src: SourceConfig) -> str:
    return f"{cfg.database}.{cfg.schema}.{_ident(src.name)}"


def render_table_ddl(cfg: Config, src: SourceConfig) -> str:
    cols = [f"  {_ident(c.name)} {SF_TYPES[c.type]}{'' if c.nullable else ' NOT NULL'}" for c in src.contract.columns]
    cols += [f"  {n.upper()} {t}" for n, t in META]
    pk = ", ".join(_ident(k) for k in src.primary_key)
    return (
        f"CREATE TABLE IF NOT EXISTS {table_name(cfg, src)} (\n"
        + ",\n".join(cols)
        + f",\n  CONSTRAINT PK_{_ident(src.name)} PRIMARY KEY ({pk})\n);"
    )


def render_load_sql(cfg: Config, src: SourceConfig, m: Manifest) -> list[str]:
    target = table_name(cfg, src)
    stg = f"{cfg.database}.{cfg.schema}.{_ident(src.name)}__STG"
    stage = cfg.stages[src.landing]
    declared = {c.name for c in src.contract.columns}
    new_cols = [c for c in m.new_columns if c not in declared]
    stmts = [render_table_ddl(cfg, src)]
    # Additive schema drift: new source columns become nullable VARCHAR columns.
    stmts += [f"ALTER TABLE {target} ADD COLUMN IF NOT EXISTS {_ident(c)} VARCHAR;" for c in new_cols]
    stmts.append(f"CREATE OR REPLACE TEMPORARY TABLE {stg} LIKE {target};")

    typed = [(c.name, SF_TYPES[c.type]) for c in src.contract.columns] + [(c, "VARCHAR") for c in new_cols]
    select = ",\n    ".join(f'$1:"{n}"::{t}' for n, t in typed)
    target_cols = ", ".join(_ident(n) for n, _ in typed)
    files = ", ".join(f"'{f['key'].rsplit('/', 1)[-1]}'" for f in m.files)
    stmts.append(
        f"COPY INTO {stg} ({target_cols}, _RUN_ID, _SOURCE_FILE, _INGESTED_AT)\n"
        f"FROM (\n  SELECT\n    {select},\n    '{m.run_id}', METADATA$FILENAME, CURRENT_TIMESTAMP()\n"
        f"  FROM @{stage}/{m.folder}/\n)\n"
        f"FILES = ({files})\nFILE_FORMAT = (TYPE = PARQUET USE_LOGICAL_TYPE = TRUE)\nON_ERROR = ABORT_STATEMENT;"
    )

    pk = [_ident(k) for k in src.primary_key]
    on = " AND ".join(f"t.{k} = s.{k}" for k in pk)
    inc = _ident(src.incremental.column) if src.incremental else None
    order = f"{inc} DESC" if inc else "_SOURCE_FILE DESC"
    all_cols = [n for n, _ in typed] + [n for n, _ in META]
    sets = ",\n  ".join(f"{_ident(c)} = s.{_ident(c)}" for c in all_cols if _ident(c) not in pk)
    newer = f" AND s.{inc} > t.{inc}" if inc else ""
    stmts.append(
        f"MERGE INTO {target} t\nUSING (\n  SELECT * FROM {stg}\n"
        f"  QUALIFY ROW_NUMBER() OVER (PARTITION BY {', '.join(pk)} ORDER BY {order}) = 1\n) s\n"
        f"ON {on}\nWHEN MATCHED{newer} THEN UPDATE SET\n  {sets}\n"
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(_ident(c) for c in all_cols)})\n"
        f"VALUES ({', '.join('s.' + _ident(c) for c in all_cols)});"
    )
    return stmts


class SnowflakeLoader:
    def __init__(self, cfg: Config, connection=None):
        self.cfg = cfg
        if connection is None:
            import snowflake.connector

            connection = snowflake.connector.connect(
                account=os.environ["SNOWFLAKE_ACCOUNT"],
                user=os.environ["SNOWFLAKE_USER"],
                private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_FILE"],
                role=os.environ.get("SNOWFLAKE_ROLE", "LOANS_LOADER"),
                warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "LOAD_WH"),
            )
        self.con = connection

    def load(self, src: SourceConfig, m: Manifest, storage=None) -> MergeStats:
        # No explicit transaction: the DDL statements would commit it implicitly anyway. The only write
        # to the target is the final MERGE, which is atomic on its own; everything before it touches
        # a session-scoped temporary table, so a failure part-way leaves the target unchanged.
        cur = self.con.cursor()
        result = None
        try:
            for stmt in render_load_sql(self.cfg, src, m):
                cur.execute(stmt)
                if stmt.startswith("MERGE"):
                    result = cur.fetchone()
        finally:
            cur.close()
        inserted, updated = (result or (0, 0))[:2]
        return MergeStats(inserted=int(inserted), updated=int(updated))
