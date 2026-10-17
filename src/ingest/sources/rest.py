"""Paginated REST API source with retries.

Expects the common cursor style: GET <endpoint>?limit=N&updated_since=...&cursor=...
returning {"data": [...], "next_cursor": "..." | null}.
"""

from __future__ import annotations

import os
import time
from datetime import datetime

import requests

from .base import Extract, Source

RETRYABLE = {429, 500, 502, 503, 504}


class RestApiSource(Source):
    def __init__(self, cfg, session: requests.Session | None = None, sleep=time.sleep):
        super().__init__(cfg)
        self.session = session or requests.Session()
        self.sleep = sleep
        token = os.environ.get(cfg.options.get("auth_token_env", ""), "")
        if token:
            self.session.headers["Authorization"] = f"Bearer {token}"

    def _get(self, url: str, params: dict) -> dict:
        max_retries = int(self.cfg.options.get("max_retries", 5))
        for attempt in range(max_retries + 1):
            try:
                resp = self.session.get(url, params=params, timeout=30)
            except (requests.ConnectionError, requests.Timeout):
                if attempt == max_retries:
                    raise
                self.retries += 1
                self.sleep(min(2**attempt, 30))
                continue
            if resp.status_code in RETRYABLE and attempt < max_retries:
                self.retries += 1
                retry_after = resp.headers.get("Retry-After")
                self.sleep(float(retry_after) if retry_after else min(2**attempt, 30))
                continue
            resp.raise_for_status()
            return resp.json()
        raise RuntimeError("unreachable")

    def extract(self, watermark: datetime | None, state: dict) -> Extract:
        o = self.cfg.options
        url = o["base_url"].rstrip("/") + o["endpoint"]
        params: dict = {"limit": int(o.get("page_size", 100))}
        since = self.lower_bound(watermark)
        if since is not None:
            params[o.get("updated_since_param", "updated_since")] = since.isoformat()
        records, pages, self.retries = [], 0, 0
        while True:
            body = self._get(url, params)
            records.extend(body.get("data", []))
            pages += 1
            cursor = body.get("next_cursor")
            if not cursor:
                break
            params["cursor"] = cursor
        return Extract(records, stats={"pages": pages, "retries": self.retries, "since": params.get("updated_since")})
