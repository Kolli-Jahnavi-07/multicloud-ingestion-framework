"""Per-source state (watermark, processed files), stored next to the data in object storage.

The state is saved only after a run's data has landed *and* loaded, so a crash anywhere before
that point makes the next run read the same window again; the loads are MERGEs on the primary
key, so reading twice can't create duplicates.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from .storage import Storage


class StateStore:
    def __init__(self, storage: Storage):
        self.storage = storage

    def get(self, source: str) -> dict[str, Any]:
        key = f"{source}.json"
        return json.loads(self.storage.get(key)) if self.storage.exists(key) else {}

    def watermark(self, source: str) -> datetime | None:
        wm = self.get(source).get("watermark")
        return datetime.fromisoformat(wm) if wm else None

    def save(self, source: str, state: dict[str, Any]) -> None:
        self.storage.put_atomic(f"{source}.json", json.dumps(state, indent=2, default=str).encode())
