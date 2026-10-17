"""Synthetic sources for the demo: an API dataset, a SQLite 'dealer database' and vendor CSV drops.

Round 1 is the initial state. Round 2 is what changes before the next scheduled run:
  * API: 400 applications updated, 300 new ones, and a new field `channel` (schema evolution)
  * dealers: 12 rows modified, 5 new dealers
  * vendor: a new daily file, plus yesterday's file re-sent unchanged (must be skipped)
All values are fictional.
"""

from __future__ import annotations

import csv
import random
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

STATES = ["TX", "CA", "FL", "GA", "OH", "MI", "NC", "AZ", "IL", "PA", "NY", "MA"]
REGIONS = {
    "TX": "South",
    "FL": "South",
    "GA": "South",
    "NC": "South",
    "CA": "West",
    "AZ": "West",
    "OH": "Midwest",
    "MI": "Midwest",
    "IL": "Midwest",
    "PA": "Northeast",
    "NY": "Northeast",
    "MA": "Northeast",
}
MAKES = {
    "Chevrolet": ["Equinox", "Silverado", "Malibu"],
    "GMC": ["Sierra", "Terrain"],
    "Buick": ["Enclave", "Encore"],
    "Toyota": ["Camry", "RAV4"],
    "Ford": ["F-150", "Escape"],
    "Honda": ["Civic", "CR-V"],
}
NOW = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


def _application(rng: random.Random, i: int, ts: datetime, dealers: int) -> dict:
    score = max(500, min(850, int(rng.gauss(690, 70))))
    decision = "APPROVED" if score > 640 else rng.choice(["DECLINED", "COUNTER_OFFER", "PENDING"])
    row = {
        "application_id": f"APP-{200000 + i}",
        "dealer_id": f"D{1000 + rng.randint(1, dealers)}",
        "applicant_state": rng.choice(STATES),
        "credit_score": score,
        "amount_requested": f"{rng.uniform(8000, 65000):.2f}",
        "decision": decision,
        "apr_offered": f"{rng.uniform(0.039, 0.229):.4f}" if decision != "DECLINED" else None,
        "updated_at": ts.isoformat().replace("+00:00", "Z"),
    }
    if rng.random() < 0.01:  # bad records from the partner: out-of-range score or unknown decision
        if rng.random() < 0.5:
            row["credit_score"] = 912
        else:
            row["decision"] = "MAYBE"
    return row


def api_round1(seed: int = 1, n: int = 3000) -> list[dict]:
    rng = random.Random(seed)
    return [_application(rng, i, NOW - timedelta(minutes=rng.randint(60, 30 * 24 * 60)), 120) for i in range(n)]


def api_round2(existing: list[dict], seed: int = 2) -> list[dict]:
    rng = random.Random(seed)
    later = NOW + timedelta(hours=6)
    changed = []
    for r in rng.sample(existing, 400):
        r = dict(r)
        r["decision"] = rng.choice(["APPROVED", "DECLINED"])
        r["apr_offered"] = f"{rng.uniform(0.039, 0.229):.4f}" if r["decision"] == "APPROVED" else None
        r["updated_at"] = (later - timedelta(minutes=rng.randint(0, 300))).isoformat().replace("+00:00", "Z")
        r["channel"] = rng.choice(["dealer_portal", "online", "phone"])
        changed.append(r)
    for i in range(300):
        r = _application(rng, 10_000 + i, later - timedelta(minutes=rng.randint(0, 300)), 125)
        r["channel"] = rng.choice(["dealer_portal", "online", "phone"])
        changed.append(r)
    return changed


def dealers_db(path: Path, seed: int = 3) -> None:
    rng = random.Random(seed)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.unlink(missing_ok=True)
    con = sqlite3.connect(path)
    con.execute(
        "CREATE TABLE dealers (dealer_id TEXT PRIMARY KEY, dealer_name TEXT NOT NULL, state TEXT, "
        "region TEXT, is_active INTEGER NOT NULL, modified_at TEXT NOT NULL)"
    )
    rows = []
    for i in range(1, 121):
        st = rng.choice(STATES)
        ts = (NOW - timedelta(days=rng.randint(1, 400))).replace(tzinfo=None).isoformat(sep=" ")
        rows.append(
            (
                f"D{1000 + i}",
                f"{rng.choice(['Metro', 'Lakeside', 'Summit', 'Valley', 'Capital'])} "
                f"{rng.choice(['Auto', 'Motors', 'Chevrolet', 'GMC'])} {i}",
                st,
                REGIONS[st],
                1,
                ts,
            )
        )
    con.executemany("INSERT INTO dealers VALUES (?, ?, ?, ?, ?, ?)", rows)
    con.commit()
    con.close()


def dealers_round2(path: Path, seed: int = 4) -> None:
    rng = random.Random(seed)
    con = sqlite3.connect(path)
    ts = (NOW + timedelta(hours=5)).replace(tzinfo=None).isoformat(sep=" ")
    for did in rng.sample([f"D{1000 + i}" for i in range(1, 121)], 12):
        con.execute("UPDATE dealers SET is_active = ?, modified_at = ? WHERE dealer_id = ?", (rng.choice([0, 1]), ts, did))
    for i in range(121, 126):
        st = rng.choice(STATES)
        con.execute("INSERT INTO dealers VALUES (?, ?, ?, ?, 1, ?)", (f"D{1000 + i}", f"New Dealer {i}", st, REGIONS[st], ts))
    con.commit()
    con.close()


def valuations_file(drop: Path, day: date, seed: int, n: int = 2000) -> Path:
    rng = random.Random(seed)
    drop.mkdir(parents=True, exist_ok=True)
    path = drop / f"valuations_{day:%Y%m%d}.csv"
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["vin", "make", "model", "model_year", "mileage", "retail_value", "valuation_date"])
        for i in range(n):
            make = rng.choice(list(MAKES))
            year = rng.randint(2016, 2026)
            miles = max(0, int(rng.gauss((2026 - year) * 12000, 6000)))
            value = max(2500, 48000 * (0.86 ** (2026 - year)) - miles * 0.05)
            w.writerow([f"1G{seed:02d}{i:013d}", make, rng.choice(MAKES[make]), year, miles, f"{value:.2f}", day.isoformat()])
    return path


def round1(root: Path) -> list[dict]:
    dealers_db(root / "dealers.db")
    valuations_file(root / "vendor_drop", date(2026, 9, 30), seed=10)
    return api_round1()


def round2(root: Path, api_records: list[dict]) -> list[dict]:
    dealers_round2(root / "dealers.db")
    valuations_file(root / "vendor_drop", date(2026, 10, 1), seed=11)
    return api_round2(api_records)
