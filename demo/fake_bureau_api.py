"""A small stand-in for the credit bureau partner's API, served over real HTTP.

GET /v1/applications?limit=&updated_since=&cursor=
  -> {"data": [...], "next_cursor": "..."}

It misbehaves on purpose: every 7th request returns 503 and the first request of each run
returns 429 with Retry-After, so the client's retry logic is exercised for real.
"""

from __future__ import annotations

import json
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


class BureauData:
    def __init__(self, records: list[dict]):
        self.records = records
        self.requests = 0
        self.lock = threading.Lock()

    def upsert(self, rows: list[dict]) -> None:
        with self.lock:
            by_id = {r["application_id"]: r for r in self.records}
            for r in rows:
                by_id[r["application_id"]] = r
            self.records = list(by_id.values())


def _handler(data: BureauData):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):  # keep demo output readable
            pass

        def _send(self, code: int, body: dict | None = None, headers: dict | None = None) -> None:
            payload = json.dumps(body or {}).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            for k, v in (headers or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self):  # noqa: N802
            url = urlparse(self.path)
            if url.path != "/v1/applications":
                return self._send(404, {"error": "not found"})
            with data.lock:
                data.requests += 1
                n = data.requests
                rows = sorted(data.records, key=lambda r: (r["updated_at"], r["application_id"]))
            if n == 1:
                return self._send(429, {"error": "slow down"}, {"Retry-After": "0"})
            if n % 7 == 0:
                return self._send(503, {"error": "temporarily unavailable"})
            q = {k: v[0] for k, v in parse_qs(url.query).items()}
            if "updated_since" in q:
                since = datetime.fromisoformat(q["updated_since"])
                rows = [r for r in rows if datetime.fromisoformat(r["updated_at"].replace("Z", "+00:00")) >= since]
            limit = min(int(q.get("limit", 100)), 500)
            offset = int(q.get("cursor", 0))
            page = rows[offset : offset + limit]
            nxt = offset + limit if offset + limit < len(rows) else None
            return self._send(200, {"data": page, "next_cursor": str(nxt) if nxt is not None else None})

    return Handler


def serve(data: BureauData, port: int = 8765) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer(("127.0.0.1", port), _handler(data))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server
