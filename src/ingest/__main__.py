"""Command line.

python -m ingest run [SOURCE ...]        # extract -> validate -> land -> load, for some or all sources
python -m ingest plan                    # show configured sources, landings and current watermarks
python -m ingest sql SOURCE MANIFEST     # print the Snowflake COPY + MERGE for a landed run
python -m ingest ddl                     # print Snowflake CREATE TABLE statements from the contracts
python -m ingest demo                    # self-contained demo: fake API + SQLite + CSV drop, two rounds
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
from pathlib import Path

from . import config as config_mod
from .landing import Manifest
from .loaders import DuckDBLoader, render_load_sql, render_table_ddl
from .runner import Runner, RunReport
from .state import StateStore
from .storage import storage_for


def _print_reports(reports: list[RunReport]) -> None:
    head = (
        f"{'source':<20} {'status':<12} {'extracted':>9} {'landed':>7} {'rejected':>8} {'dupes':>6} {'ins':>6} {'upd':>6}  notes"
    )
    print(head)
    print("-" * len(head))
    for r in reports:
        notes = []
        if r.new_columns:
            notes.append(f"new columns: {', '.join(r.new_columns)}")
        if r.stats.get("retries"):
            notes.append(f"{r.stats['retries']} retries over {r.stats['pages']} pages")
        if r.stats.get("files_skipped"):
            notes.append(f"{r.stats['files_skipped']} file(s) already processed")
        if r.error:
            notes.append(r.error)
        print(
            f"{r.source:<20} {r.status:<12} {r.extracted:>9,} {r.landed:>7,} {r.rejected:>8,} "
            f"{r.duplicates_dropped:>6,} {r.inserted:>6,} {r.updated:>6,}  {'; '.join(notes)}"
        )


def demo(workdir: Path) -> int:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from demo import seed
    from demo.fake_bureau_api import BureauData, serve

    if workdir.exists():
        shutil.rmtree(workdir)
    (workdir / "demo").mkdir(parents=True)
    import os

    os.environ.update(
        {
            "LANDING_AWS_URI": str(workdir / "lake/aws"),
            "LANDING_AZURE_URI": str(workdir / "lake/azure"),
            "STATE_URI": str(workdir / "lake/_state"),
            "DEALERS_DB_URL": f"sqlite:///{workdir / 'demo/dealers.db'}",
            "VALUATIONS_DROP": str(workdir / "demo/vendor_drop"),
            "BUREAU_API_URL": "http://127.0.0.1:8765",
        }
    )
    cfg = config_mod.load()
    api = BureauData(seed.round1(workdir / "demo"))
    server = serve(api)
    loader = DuckDBLoader(str(workdir / "warehouse.duckdb"))
    runner = Runner(cfg, loader)
    try:
        print("\nRound 1: first load of every source\n")
        _print_reports(runner.run_all())
        api.upsert(seed.round2(workdir / "demo", api.records))
        print("\nRound 2: sources changed (updates, new rows, a new API field, a new vendor file)\n")
        _print_reports(runner.run_all())
        print("\nRound 3: nothing changed upstream\n")
        _print_reports(runner.run_all())
    finally:
        server.shutdown()
    print("\nWarehouse tables (raw schema):")
    for s in cfg.sources:
        n = loader.sql(f"SELECT COUNT(*) FROM raw.{s.name}")[0][0]
        print(f"  raw.{s.name:<22} {n:>6,} rows")
    print(f"\nLanding zones: {workdir / 'lake'}")
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
    ap = argparse.ArgumentParser(prog="python -m ingest")
    ap.add_argument("--config", default="config/sources.yml")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("sources", nargs="*")
    r.add_argument("--warehouse", default="data/warehouse.duckdb", help="local DuckDB file, or 'snowflake'")
    r.add_argument("--json", action="store_true", help="print run reports as JSON (for orchestrators)")
    sub.add_parser("plan")
    q = sub.add_parser("sql")
    q.add_argument("source")
    q.add_argument("manifest", help="path or key of a _manifest.json in the source's landing")
    sub.add_parser("ddl")
    d = sub.add_parser("demo")
    d.add_argument("--workdir", type=Path, default=Path("data/demo-run"))
    args = ap.parse_args(argv)

    if args.cmd == "demo":
        return demo(args.workdir)
    cfg = config_mod.load(args.config)
    if args.cmd == "plan":
        states = StateStore(storage_for(cfg.state_uri))
        for s in cfg.sources:
            st = states.get(s.name)
            print(
                f"{s.name:<20} {s.type:<9} -> {s.landing:<6} {cfg.landings[s.landing]:<40} watermark={st.get('watermark') or '-'}"
            )
        return 0
    if args.cmd == "ddl":
        print("\n\n".join(render_table_ddl(cfg, s) for s in cfg.sources))
        return 0
    if args.cmd == "sql":
        src = cfg.source(args.source)
        storage = storage_for(cfg.landings[src.landing])
        p = Path(args.manifest)
        m = Manifest.from_json(p.read_bytes() if p.exists() else storage.get(args.manifest))
        print("\n\n".join(render_load_sql(cfg, src, m)))
        return 0
    if args.cmd == "run":
        if args.warehouse == "snowflake":
            from .loaders import SnowflakeLoader

            loader = SnowflakeLoader(cfg)
        else:
            Path(args.warehouse).parent.mkdir(parents=True, exist_ok=True)
            loader = DuckDBLoader(args.warehouse)
        reports = Runner(cfg, loader).run_all(args.sources or None)
        if args.json:
            print(json.dumps([r.as_dict() for r in reports], indent=2, default=str))
        else:
            _print_reports(reports)
        return 0 if all(r.status != "failed" for r in reports) else 1
    return 2


if __name__ == "__main__":
    sys.exit(main())
