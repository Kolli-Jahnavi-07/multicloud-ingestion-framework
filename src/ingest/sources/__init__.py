from __future__ import annotations

from ..config import SourceConfig
from .base import Extract, Source
from .files import FileDropSource
from .rest import RestApiSource
from .sql import SqlSource

_TYPES: dict[str, type[Source]] = {"rest_api": RestApiSource, "sql": SqlSource, "files": FileDropSource}


def build(cfg: SourceConfig, **kwargs) -> Source:
    return _TYPES[cfg.type](cfg, **kwargs)


__all__ = ["Extract", "Source", "build"]
