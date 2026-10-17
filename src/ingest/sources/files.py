"""Vendor file drop (SFTP landing folder or a storage prefix): process each file once.

A file is identified by name *and* checksum: the same file re-sent is skipped, a corrected file
re-sent under the same name is processed again (its rows replace the old ones through the MERGE).
"""

from __future__ import annotations

import csv
import fnmatch
import hashlib
import io
from datetime import datetime

from ..storage import storage_for
from .base import Extract, Source


class FileDropSource(Source):
    def __init__(self, cfg):
        super().__init__(cfg)
        self.drop = storage_for(cfg.options["path"])
        self.pattern = cfg.options.get("pattern", "*.csv")

    def extract(self, watermark: datetime | None, state: dict) -> Extract:
        processed: dict[str, str] = dict(state.get("files", {}))
        records, new_files, skipped = [], {}, 0
        for key in self.drop.list():
            name = key.rsplit("/", 1)[-1]
            if not fnmatch.fnmatch(name, self.pattern):
                continue
            data = self.drop.get(key)
            digest = hashlib.sha256(data).hexdigest()
            if processed.get(key) == digest:
                skipped += 1
                continue
            rows = list(csv.DictReader(io.StringIO(data.decode("utf-8-sig"))))
            for r in rows:
                r.pop(None, None)  # ragged lines: csv puts the overflow under key None
            records.extend(rows)
            new_files[key] = digest
        return Extract(
            records,
            state_updates={"files": {**processed, **new_files}},
            stats={"files": sorted(new_files), "files_skipped": skipped},
        )
