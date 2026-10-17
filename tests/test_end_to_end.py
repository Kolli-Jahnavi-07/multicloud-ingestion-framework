"""Run every source end to end against a real local HTTP API, SQLite database and file drop."""

import pytest
from demo import seed
from demo.fake_bureau_api import BureauData, serve

from ingest.config import load
from ingest.contracts import _cast
from ingest.landing import manifests
from ingest.loaders import DuckDBLoader
from ingest.runner import Runner, SimulatedCrash
from ingest.storage import storage_for


@pytest.fixture()
def world(env):
    api = BureauData(seed.round1(env / "src"))
    server = serve(api, port=8799)
    yield env, api
    server.shutdown()
    server.server_close()


def expected_applications(api):
    """Source of truth: the API's current records that pass the contract."""
    cfg = load().source("credit_applications")
    out = {}
    for r in api.records:
        if r["credit_score"] > 850 or r["decision"] not in {"APPROVED", "DECLINED", "PENDING", "COUNTER_OFFER"}:
            continue
        out[r["application_id"]] = (r["decision"], _cast(r["updated_at"], cfg.contract.column("updated_at")))
    return out


def warehouse_applications(loader):
    return {a: (d, u) for a, d, u in loader.sql("SELECT application_id, decision, updated_at FROM raw.credit_applications")}


def test_two_rounds_match_the_sources(world):
    env, api = world
    cfg, loader = load(), DuckDBLoader()
    runner = Runner(cfg, loader)

    r1 = {r.source: r for r in runner.run_all()}
    assert all(r.status == "succeeded" for r in r1.values()), r1
    assert r1["credit_applications"].stats["retries"] >= 1  # the fake API throws 429/503 on purpose
    assert warehouse_applications(loader) == expected_applications(api)

    api.upsert(seed.round2(env / "src", api.records))
    r2 = {r.source: r for r in runner.run_all()}
    assert r2["credit_applications"].extracted < 800, "second run must be incremental"
    assert r2["credit_applications"].new_columns == ["channel"]
    assert r2["vehicle_valuations"].stats["files_skipped"] == 1
    assert warehouse_applications(loader) == expected_applications(api)
    assert loader.sql("SELECT COUNT(*) FROM raw.credit_applications WHERE channel IS NOT NULL")[0][0] > 600

    dealers = dict(loader.sql("SELECT dealer_id, is_active FROM raw.dealers"))
    import sqlite3

    src = dict(sqlite3.connect(env / "src/dealers.db").execute("SELECT dealer_id, is_active FROM dealers").fetchall())
    assert dealers == {k: bool(v) for k, v in src.items()}
    assert loader.sql("SELECT COUNT(*), COUNT(DISTINCT (vin, valuation_date)) FROM raw.vehicle_valuations")[0] == (4000, 4000)

    # Nothing changed upstream: the overlap window is re-read but changes nothing.
    r3 = runner.run_all()
    assert sum(r.inserted + r.updated for r in r3) == 0


def test_rejected_records_are_kept_for_review(world):
    env, _ = world
    cfg = load()
    report = Runner(cfg, DuckDBLoader()).run("credit_applications")
    assert report.rejected > 0
    aws = storage_for(cfg.landings["aws"])
    rejected = [k for k in aws.list("_rejected") if k.endswith("rejected.jsonl")]
    lines = aws.get(rejected[0]).decode().splitlines()
    assert len(lines) == report.rejected and '"reason"' in lines[0]


def test_crash_after_landing_is_recovered_without_duplicates(world):
    env, api = world
    cfg, loader = load(), DuckDBLoader()
    runner = Runner(cfg, loader)
    runner.crash_after_landing = True
    with pytest.raises(SimulatedCrash):
        runner.run("credit_applications")
    assert runner.state.watermark("credit_applications") is None  # state was never committed

    runner.crash_after_landing = False
    report = runner.run("credit_applications")
    assert report.status == "succeeded"
    aws = storage_for(cfg.landings["aws"])
    assert len(manifests(aws, "credit_applications")) == 2  # both runs landed; the second re-read the window
    assert warehouse_applications(loader) == expected_applications(api)
    assert loader.sql("SELECT COUNT(*) - COUNT(DISTINCT application_id) FROM raw.credit_applications")[0][0] == 0


def test_vendor_schema_change_stops_the_source_and_nothing_lands(world):
    env, _ = world
    cfg, loader = load(), DuckDBLoader()
    drop = env / "src/vendor_drop"
    for f in drop.glob("*.csv"):
        f.unlink()
    (drop / "valuations_20261002.csv").write_text(
        "vin,make,model,model_year,mileage,retail_value,valuation_date,trim\n1G1,GMC,Sierra,2024,100,50000,2026-10-02,SLT\n"
    )
    report = Runner(cfg, loader).run("vehicle_valuations")
    assert report.status == "failed" and "unexpected columns trim" in report.error
    assert storage_for(cfg.landings["azure"]).list("vehicle_valuations") == []
    assert Runner(cfg, loader).state.get("vehicle_valuations") == {}  # will be picked up again once fixed


def test_late_older_record_does_not_overwrite_newer(world):
    env, api = world
    cfg, loader = load(), DuckDBLoader()
    runner = Runner(cfg, loader)
    runner.run("credit_applications")
    target = next(iter(expected_applications(api)))
    stale = next(dict(r) for r in api.records if r["application_id"] == target)
    stale["decision"] = "DECLINED" if stale["decision"] != "DECLINED" else "APPROVED"
    stale["updated_at"] = "2020-01-01T00:00:00Z"
    m_cfg = cfg.source("credit_applications")
    from ingest.contracts import validate
    from ingest.landing import land

    res = validate([stale], m_cfg)
    m = land(storage_for(cfg.landings["aws"]), "credit_applications", "replay", res, None)
    stats = loader.load(m_cfg, m, storage_for(cfg.landings["aws"]))
    assert (stats.inserted, stats.updated) == (0, 0)
    assert warehouse_applications(loader)[target][0] != stale["decision"]
