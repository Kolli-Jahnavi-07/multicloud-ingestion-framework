"""Airflow alternative to the ADF pipeline: one task per configured source, generated from sources.yml.

Adding a source to the YAML adds a task here on the next DAG parse; no DAG code changes.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import yaml
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

CONFIG = Path(__file__).parent / "sources.yml"  # deployed alongside the DAG

with DAG(
    dag_id="ingest_sources",
    start_date=datetime(2026, 1, 1),
    schedule="5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2), "execution_timeout": timedelta(hours=2)},
    tags=["ingestion", "snowflake"],
) as dag:
    for source in yaml.safe_load(CONFIG.read_text())["sources"]:
        DockerOperator(
            task_id=f"ingest_{source['name']}",
            image="acmeacr.azurecr.io/ingestion-framework:latest",
            command=f"python -m ingest run {source['name']} --warehouse snowflake --json",
            environment={"SNOWFLAKE_ROLE": "LOANS_LOADER"},
            private_environment={"SNOWFLAKE_PRIVATE_KEY_FILE": "/run/secrets/snowflake_key"},
            auto_remove="success",
        )
