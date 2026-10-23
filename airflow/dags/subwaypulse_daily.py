"""Daily batch side of SubwayPulse.

1. refresh_schedule  download the static schedule and publish scheduled headways for
                     today and tomorrow (the running Flink job picks them up from
                     Kafka without a restart)
2. quality_checks    Great Expectations and custom checks on yesterday's silver data;
                     a failure stops the run before bad data reaches the marts
3. dbt_build         incidents and hourly reliability marts, plus dbt tests

Runs at 04:00 New York time, after the overnight service changes are published and
before the morning rush.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

PY = "/opt/airflow/jobs-venv/bin/python -m"
DBT = (
    "/opt/airflow/jobs-venv/bin/dbt {cmd} --project-dir /opt/dbt --profiles-dir /opt/dbt "
    "--target trino --target-path /tmp/dbt/target --log-path /tmp/dbt/logs"
)

with DAG(
    dag_id="subwaypulse_daily",
    description="Schedule refresh, data quality, and reliability marts",
    start_date=pendulum.datetime(2026, 10, 1, tz="America/New_York"),
    schedule="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    doc_md=__doc__,
    tags=["subwaypulse", "batch"],
) as dag:
    refresh_schedule = BashOperator(
        task_id="refresh_schedule",
        bash_command=f"{PY} subwaypulse.load_schedule",
        env={"SCHEDULE_START_DATE": "today"},
        append_env=True,
    )

    quality_checks = BashOperator(
        task_id="quality_checks",
        # data_interval_start is yesterday 04:00 NYC; check that UTC day
        bash_command=f"{PY} subwaypulse.quality --date {{{{ data_interval_start.in_timezone('UTC') | ds }}}}",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=DBT.format(cmd="build"),
    )

    refresh_schedule >> quality_checks >> dbt_build
