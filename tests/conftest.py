import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT)]


@pytest.fixture()
def env(monkeypatch, tmp_path):
    """Point every URI in config/sources.yml at a temporary directory."""
    values = {
        "LANDING_AWS_URI": str(tmp_path / "lake/aws"),
        "LANDING_AZURE_URI": str(tmp_path / "lake/azure"),
        "STATE_URI": str(tmp_path / "lake/_state"),
        "DEALERS_DB_URL": f"sqlite:///{tmp_path / 'src/dealers.db'}",
        "VALUATIONS_DROP": str(tmp_path / "src/vendor_drop"),
        "BUREAU_API_URL": "http://127.0.0.1:8799",
    }
    for k, v in values.items():
        monkeypatch.setenv(k, v)
    monkeypatch.chdir(ROOT)
    return tmp_path
