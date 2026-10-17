"""Write a validated extract to the landing zone as Parquet plus a manifest.

    <source>/ingest_date=2026-10-17/run_id=<id>/part-0000.parquet
    <source>/ingest_date=2026-10-17/run_id=<id>/_manifest.json      <- written last: the commit marker
    _rejected/<source>/ingest_date=2026-10-17/run_id=<id>/rejected.jsonl

Downstream loaders only ever read folders that have a manifest, so a crash mid-write leaves
nothing half-visible.
"""

from __future__ import annotations

import hashlib
import io
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq

from .contracts import ValidationResult
from .storage import Storage

ROWS_PER_FILE = 250_000


@dataclass
class Manifest:
    source: str
    run_id: str
    folder: str
    files: list[dict] = field(default_factory=list)
    rows: int = 0
    rejected: int = 0
    columns: list[dict] = field(default_factory=list)
    new_columns: list[str] = field(default_factory=list)
    watermark: str | None = None
    created_at: str = ""

    def to_json(self) -> bytes:
        return json.dumps(self.__dict__, indent=2).encode()

    @classmethod
    def from_json(cls, data: bytes) -> Manifest:
        return cls(**json.loads(data))


def land(storage: Storage, source: str, run_id: str, result: ValidationResult, watermark: str | None) -> Manifest:
    now = datetime.now(UTC)
    folder = f"{source}/ingest_date={now:%Y-%m-%d}/run_id={run_id}"
    m = Manifest(
        source=source,
        run_id=run_id,
        folder=folder,
        rows=result.table.num_rows,
        rejected=len(result.rejected),
        columns=[{"name": f.name, "type": str(f.type)} for f in result.table.schema],
        new_columns=result.new_columns,
        watermark=watermark,
        created_at=now.isoformat(),
    )
    for i, offset in enumerate(range(0, max(result.table.num_rows, 1), ROWS_PER_FILE)):
        chunk = result.table.slice(offset, ROWS_PER_FILE)
        if chunk.num_rows == 0 and i > 0:
            break
        buf = io.BytesIO()
        pq.write_table(chunk, buf, compression="zstd")
        data = buf.getvalue()
        key = f"{folder}/part-{i:04d}.parquet"
        storage.put(key, data)
        m.files.append({"key": key, "rows": chunk.num_rows, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})
    if result.rejected:
        body = "".join(json.dumps(r, default=str) + "\n" for r in result.rejected).encode()
        storage.put(f"_rejected/{folder}/rejected.jsonl", body)
    storage.put_atomic(f"{folder}/_manifest.json", m.to_json())
    return m


def read_table(storage: Storage, manifest: Manifest) -> pa.Table:
    tables = [pq.read_table(io.BytesIO(storage.get(f["key"]))) for f in manifest.files]
    return pa.concat_tables(tables) if tables else pa.table({})


def manifests(storage: Storage, source: str) -> list[Manifest]:
    return [Manifest.from_json(storage.get(k)) for k in storage.list(source) if k.endswith("/_manifest.json")]
