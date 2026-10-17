import sqlite3
from datetime import UTC, datetime

import pytest
import requests

from ingest.config import load
from ingest.sources import build
from ingest.sources.files import FileDropSource


class FakeResponse:
    def __init__(self, status, body=None, headers=None):
        self.status_code, self._body, self.headers = status, body or {}, headers or {}

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}")


class FakeSession:
    def __init__(self, script):
        self.script, self.calls, self.headers = list(script), [], {}

    def get(self, url, params, timeout):
        self.calls.append(dict(params))
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def page(ids, cursor=None):
    return FakeResponse(200, {"data": [{"application_id": i} for i in ids], "next_cursor": cursor})


def test_rest_paginates_retries_and_applies_lookback(env, monkeypatch):
    monkeypatch.setenv("BUREAU_API_TOKEN", "secret")
    cfg = load().source("credit_applications")
    session = FakeSession(
        [
            FakeResponse(429, headers={"Retry-After": "2"}),
            page(["A1", "A2"], cursor="2"),
            FakeResponse(503),
            requests.ConnectionError("reset"),
            page(["A3"]),
        ]
    )
    sleeps = []
    src = build(cfg, session=session, sleep=sleeps.append)
    out = src.extract(datetime(2026, 10, 1, 12, 0, tzinfo=UTC), {})
    assert [r["application_id"] for r in out.records] == ["A1", "A2", "A3"]
    assert out.stats == {"pages": 2, "retries": 3, "since": "2026-10-01T11:45:00+00:00"}
    assert sleeps[0] == 2.0  # honoured Retry-After
    assert session.calls[-1]["cursor"] == "2"
    assert session.headers["Authorization"] == "Bearer secret"


def test_rest_gives_up_after_max_retries(env):
    cfg = load().source("credit_applications")
    session = FakeSession([FakeResponse(503)] * 10)
    with pytest.raises(requests.HTTPError):
        build(cfg, session=session, sleep=lambda s: None).extract(None, {})
    assert len(session.calls) == 6  # first try + max_retries (5)


def test_sql_source_reads_only_changed_rows(env):
    db = env / "src/dealers.db"
    db.parent.mkdir(parents=True)
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE dealers (dealer_id TEXT, dealer_name TEXT, modified_at TEXT)")
    con.executemany(
        "INSERT INTO dealers VALUES (?, ?, ?)",
        [
            ("D1", "Old", "2026-09-01 10:00:00"),
            ("D2", "Edge", "2026-10-01 12:00:00"),
            ("D3", "New", "2026-10-02 08:00:00"),
        ],
    )
    con.commit()
    src = build(load().source("dealers"))
    assert len(src.extract(None, {}).records) == 3
    out = src.extract(datetime(2026, 10, 1, 12, 0, tzinfo=UTC), {})
    assert [r["dealer_id"] for r in out.records] == ["D2", "D3"]  # >= watermark: boundary row re-read, harmless


def test_file_drop_skips_seen_files_but_reprocesses_corrections(env):
    drop = env / "src/vendor_drop"
    drop.mkdir(parents=True)
    (drop / "valuations_20261001.csv").write_text("vin,retail_value\nV1,100\n")
    (drop / "notes.txt").write_text("ignore me")
    src = FileDropSource(load().source("vehicle_valuations"))
    first = src.extract(None, {})
    assert len(first.records) == 1 and first.stats["files"] == ["valuations_20261001.csv"]
    again = src.extract(None, first.state_updates)
    assert again.records == [] and again.stats["files_skipped"] == 1
    (drop / "valuations_20261001.csv").write_text("vin,retail_value\nV1,120\n")  # vendor correction
    fixed = src.extract(None, first.state_updates)
    assert fixed.records == [{"vin": "V1", "retail_value": "120"}]
