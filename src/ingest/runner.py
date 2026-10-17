"""Run one source end to end: extract -> validate -> land -> load -> commit state."""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime

from . import sources
from .config import Config, SourceConfig
from .contracts import ContractViolation, validate
from .landing import Manifest, land
from .state import StateStore
from .storage import Storage, storage_for

log = logging.getLogger("ingest")


@dataclass
class RunReport:
    source: str
    run_id: str
    status: str  # succeeded | no_new_data | failed
    extracted: int = 0
    landed: int = 0
    rejected: int = 0
    duplicates_dropped: int = 0
    new_columns: list[str] = field(default_factory=list)
    inserted: int = 0
    updated: int = 0
    watermark: str | None = None
    landing_folder: str | None = None
    seconds: float = 0.0
    error: str | None = None
    stats: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return asdict(self)


class Runner:
    def __init__(self, cfg: Config, loader, source_kwargs: dict[str, dict] | None = None, max_reject_fraction: float = 0.05):
        self.cfg = cfg
        self.loader = loader
        self.state = StateStore(storage_for(cfg.state_uri))
        self.source_kwargs = source_kwargs or {}
        self.max_reject_fraction = max_reject_fraction
        self._storages: dict[str, Storage] = {}
        self.crash_after_landing = False  # test hook: simulate the process dying before state is saved

    def storage(self, landing: str) -> Storage:
        if landing not in self._storages:
            self._storages[landing] = storage_for(self.cfg.landings[landing])
        return self._storages[landing]

    def run(self, name: str) -> RunReport:
        src = self.cfg.source(name)
        run_id = f"{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
        report = RunReport(source=name, run_id=run_id, status="failed")
        t0 = time.perf_counter()
        try:
            self._run(src, report)
        except ContractViolation as e:
            report.error = str(e)
            log.error("%s: contract violation, nothing landed: %s", name, e)
        except Exception as e:  # noqa: BLE001 - a failed source must not stop the others
            report.error = f"{type(e).__name__}: {e}"
            log.exception("%s failed", name)
            if isinstance(e, SimulatedCrash):
                raise
        report.seconds = round(time.perf_counter() - t0, 2)
        return report

    def _run(self, src: SourceConfig, report: RunReport) -> None:
        state = self.state.get(src.name)
        watermark = self.state.watermark(src.name)
        extract = sources.build(src, **self.source_kwargs.get(src.name, {})).extract(watermark, state)
        report.extracted = len(extract.records)
        report.stats = extract.stats
        report.watermark = watermark.isoformat() if watermark else None
        if not extract.records:
            report.status = "no_new_data"
            if extract.state_updates:
                self.state.save(src.name, {**state, **extract.state_updates})
            return

        result = validate(extract.records, src, self.max_reject_fraction)
        report.rejected = len(result.rejected)
        report.duplicates_dropped = result.duplicates_dropped
        # Report only columns seen for the first time; known evolved columns are business as usual.
        report.new_columns = [c for c in result.new_columns if c not in state.get("evolved_columns", [])]

        new_wm = watermark
        if src.incremental and result.table.num_rows:
            batch_max = max(result.table.column(src.incremental.column).to_pylist())
            new_wm = max(batch_max, watermark) if watermark else batch_max

        storage = self.storage(src.landing)
        manifest: Manifest = land(storage, src.name, report.run_id, result, new_wm.isoformat() if new_wm else None)
        report.landed = manifest.rows
        report.landing_folder = storage.url(manifest.folder)

        if self.crash_after_landing:
            raise SimulatedCrash("process killed after landing, before load and state commit")

        if manifest.rows:
            stats = self.loader.load(src, manifest, storage)
            report.inserted, report.updated = stats.inserted, stats.updated

        self.state.save(
            src.name,
            {
                **state,
                **extract.state_updates,
                "watermark": new_wm.isoformat() if new_wm else None,
                "last_run_id": report.run_id,
                "last_success_at": datetime.now(UTC).isoformat(),
                "evolved_columns": sorted(set(state.get("evolved_columns", [])) | set(result.new_columns)),
            },
        )
        report.watermark = new_wm.isoformat() if new_wm else None
        report.status = "succeeded"

    def run_all(self, names: list[str] | None = None) -> list[RunReport]:
        return [self.run(n) for n in (names or [s.name for s in self.cfg.sources])]


class SimulatedCrash(RuntimeError):
    pass
