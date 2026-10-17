from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from ..config import SourceConfig


@dataclass
class Extract:
    records: list[dict[str, Any]]
    # Source-specific bookkeeping to persist only after the data has landed (e.g. files processed).
    state_updates: dict[str, Any] = field(default_factory=dict)
    # Free-form details for the run report (pages read, retries, files).
    stats: dict[str, Any] = field(default_factory=dict)


class Source(ABC):
    def __init__(self, cfg: SourceConfig):
        self.cfg = cfg

    @abstractmethod
    def extract(self, watermark: datetime | None, state: dict[str, Any]) -> Extract: ...

    def lower_bound(self, watermark: datetime | None) -> datetime | None:
        """Where to start reading: the last watermark minus the configured overlap."""
        if watermark is None or self.cfg.incremental is None:
            return None
        return watermark - timedelta(minutes=self.cfg.incremental.lookback_minutes)
