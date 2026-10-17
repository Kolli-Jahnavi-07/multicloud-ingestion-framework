import sqlglot

from ingest.config import load
from ingest.landing import Manifest
from ingest.loaders import render_load_sql, render_table_ddl


def manifest(new_columns=()):
    return Manifest(
        source="credit_applications",
        run_id="R1",
        folder="credit_applications/ingest_date=2026-10-17/run_id=R1",
        files=[{"key": "credit_applications/ingest_date=2026-10-17/run_id=R1/part-0000.parquet"}],
        rows=10,
        new_columns=list(new_columns),
    )


def test_load_sql_is_valid_snowflake_and_does_the_right_things(env):
    cfg = load()
    src = cfg.source("credit_applications")
    stmts = render_load_sql(cfg, src, manifest(["channel"]))
    for s in stmts:
        sqlglot.parse_one(s, read="snowflake")  # raises on a syntax error
    sql = "\n".join(stmts)
    assert "ALTER TABLE LOANS.RAW.CREDIT_APPLICATIONS ADD COLUMN IF NOT EXISTS CHANNEL VARCHAR;" in sql
    assert "FROM @RAW.AWS_LANDING/credit_applications/ingest_date=2026-10-17/run_id=R1/" in sql
    assert "FILES = ('part-0000.parquet')" in sql
    assert "USE_LOGICAL_TYPE = TRUE" in sql
    assert "QUALIFY ROW_NUMBER() OVER (PARTITION BY APPLICATION_ID ORDER BY UPDATED_AT DESC) = 1" in sql
    assert "WHEN MATCHED AND s.UPDATED_AT > t.UPDATED_AT THEN UPDATE" in sql


def test_ddl_uses_contract_types_and_composite_keys(env):
    cfg = load()
    ddl = render_table_ddl(cfg, cfg.source("vehicle_valuations"))
    sqlglot.parse_one(ddl, read="snowflake")
    assert "RETAIL_VALUE NUMBER(18,4)" in ddl and "VALUATION_DATE DATE NOT NULL" in ddl
    assert "PRIMARY KEY (VIN, VALUATION_DATE)" in ddl


def test_azure_sources_load_from_the_azure_stage(env):
    cfg = load()
    m = manifest()
    m.folder = "dealers/ingest_date=2026-10-17/run_id=R1"
    assert any("@RAW.AZURE_LANDING/dealers/" in s for s in render_load_sql(cfg, cfg.source("dealers"), m))
