from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from ingest.config import Column, Contract, Incremental, SourceConfig
from ingest.contracts import ContractViolation, validate


def source(on_new="evolve", incremental=True):
    return SourceConfig(
        name="apps",
        type="rest_api",
        landing="aws",
        primary_key=("id",),
        incremental=Incremental("updated_at") if incremental else None,
        contract=Contract(
            (
                Column("id", "string", nullable=False),
                Column("score", "int", min=300, max=850),
                Column("amount", "decimal", min=0),
                Column("active", "bool"),
                Column("decision", "string", allowed=("APPROVED", "DECLINED")),
                Column("opened", "date"),
                Column("updated_at", "timestamp", nullable=False),
            ),
            on_new,
        ),
    )


def rec(i="A1", **kw):
    base = {
        "id": i,
        "score": "720",
        "amount": "1500.5",
        "active": "yes",
        "decision": "APPROVED",
        "opened": "2026-01-02",
        "updated_at": "2026-03-01T10:00:00Z",
    }
    return {**base, **kw}


def test_values_are_cast_to_contract_types():
    t = validate([rec()], source()).table.to_pylist()[0]
    assert t == {
        "id": "A1",
        "score": 720,
        "amount": Decimal("1500.5000"),
        "active": True,
        "decision": "APPROVED",
        "opened": date(2026, 1, 2),
        "updated_at": datetime(2026, 3, 1, 10, tzinfo=UTC),
    }


@pytest.mark.parametrize(
    "bad, reason",
    [
        ({"score": "912"}, "above 850"),
        ({"score": "7.5"}, "whole number"),
        ({"amount": "-1"}, "below 0"),
        ({"decision": "MAYBE"}, "not one of"),
        ({"updated_at": None}, "updated_at is required"),
        ({"opened": "02/30/2026"}, "opened="),
        ({"active": "sometimes"}, "not a boolean"),
    ],
)
def test_bad_records_are_quarantined_with_a_reason(bad, reason):
    records = [rec(f"A{i}") for i in range(50)] + [rec("BAD", **bad)]
    res = validate(records, source())
    assert res.accepted == 50
    assert len(res.rejected) == 1 and reason in res.rejected[0]["reason"]
    assert res.rejected[0]["record"]["id"] == "BAD"


def test_too_many_rejects_fail_the_batch():
    with pytest.raises(ContractViolation, match="rejected"):
        validate([rec("A", score="999"), rec("B")], source(), max_reject_fraction=0.05)


def test_missing_required_column_fails_the_batch():
    r = rec()
    del r["updated_at"]
    with pytest.raises(ContractViolation, match="required columns missing"):
        validate([r], source())


def test_new_columns_evolve_or_fail_by_contract():
    res = validate([rec(channel="online")], source("evolve"))
    assert res.new_columns == ["channel"] and res.table.column("channel").to_pylist() == ["online"]
    with pytest.raises(ContractViolation, match="unexpected columns channel"):
        validate([rec(channel="online")], source("fail"))


def test_duplicate_keys_keep_newest():
    res = validate([rec(score="700", updated_at="2026-03-02T00:00:00Z"), rec(score="650")], source())
    assert res.duplicates_dropped == 1
    assert res.table.column("score").to_pylist() == [700]
