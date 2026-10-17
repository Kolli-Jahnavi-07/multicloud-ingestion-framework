import textwrap

import pytest

from ingest.config import expand_env, load


def test_repository_config_is_valid(env):
    cfg = load()
    assert [s.name for s in cfg.sources] == ["credit_applications", "dealers", "vehicle_valuations"]
    assert cfg.source("credit_applications").incremental.lookback_minutes == 15
    assert cfg.landings["aws"].endswith("lake/aws")


def test_env_expansion(monkeypatch):
    monkeypatch.setenv("X_URI", "s3://bucket/p")
    assert expand_env({"a": ["${X_URI}", "${MISSING_VAR:-fallback}"]}) == {"a": ["s3://bucket/p", "fallback"]}
    with pytest.raises(KeyError, match="NOPE"):
        expand_env("${NOPE}")


def test_invalid_config_lists_every_problem(tmp_path):
    p = tmp_path / "bad.yml"
    p.write_text(
        textwrap.dedent("""
        landings: {aws: data/x}
        sources:
          - name: a
            type: ftp
            landing: gcp
            primary_key: [id]
            incremental: {column: changed_at}
            contract:
              on_new_columns: ignore
              columns:
                id: {type: string}
                amount: {type: money}
    """)
    )
    with pytest.raises(ValueError) as e:
        load(p)
    msg = str(e.value)
    for expected in [
        "type must be one of",
        "landing 'gcp'",
        "amount: type",
        "on_new_columns",
        "primary key column id must be nullable: false",
        "incremental column changed_at",
    ]:
        assert expected in msg
