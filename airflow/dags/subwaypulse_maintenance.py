"""Hourly Iceberg maintenance.

The streaming job commits every 30 seconds, so each table collects ~120 small files
and 120 snapshots an hour. This compacts recent partitions, expires old snapshots,
removes orphan files, and logs file counts before and after to ops.maintenance_log.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="subwaypulse_maintenance",
    description="Compact small files and expire snapshots on the Iceberg tables",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    schedule="15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        # A compaction commit can conflict with a streaming commit; a retry re-reads
        # the latest snapshot and goes through.
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
    },
    doc_md=__doc__,
    tags=["subwaypulse", "maintenance"],
) as dag:
    BashOperator(
        task_id="iceberg_maintenance",
        bash_command="/opt/airflow/jobs-venv/bin/python -m subwaypulse.maintenance",
    )
