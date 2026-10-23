"""Data quality checks on the silver tables, run by Airflow before dbt builds the marts.

    python -m subwaypulse.quality --date 2026-10-26

Exits non-zero on failure so the DAG stops instead of publishing marts on bad data.
A day of one route group is a few hundred thousand rows at most, so the checks run
on a pandas DataFrame pulled through Trino.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from datetime import date, timedelta

os.environ.setdefault("GX_ANALYTICS_ENABLED", "false")
os.environ.setdefault("TQDM_DISABLE", "1")


def arrival_expectations(gxe) -> list:
    return [
        gxe.ExpectColumnValuesToNotBeNull(column="trip_key"),
        gxe.ExpectColumnValuesToNotBeNull(column="stop_id"),
        gxe.ExpectColumnValuesToMatchRegex(column="stop_id", regex=r"^[A-Z0-9]+[NS]$"),
        gxe.ExpectColumnValuesToBeInSet(column="direction", value_set=["N", "S"]),
        # The update that revealed an arrival can't come before it, and should come
        # within minutes. Both times are feed (event) time, so this holds for live and
        # simulated runs alike.
        gxe.ExpectColumnValuesToBeBetween(column="observe_lag_s", min_value=0, max_value=1800),
    ]


def headway_expectations(gxe) -> list:
    return [
        gxe.ExpectColumnValuesToBeBetween(column="headway_s", min_value=1, max_value=4 * 3600),
        gxe.ExpectColumnValuesToBeInSet(column="status", value_set=["OK", "BUNCHING", "GAP", "UNSCHEDULED"]),
        gxe.ExpectColumnValuesToNotBeNull(column="trip_key"),
    ]


def validate(df, name: str, expectations: list) -> dict:
    import great_expectations as gx

    run = f"{name}_{uuid.uuid4().hex[:8]}"  # GX caches by batch id
    context = gx.get_context(mode="ephemeral")
    asset = context.data_sources.add_pandas(name=f"{run}_src").add_dataframe_asset(name=run)
    batch_def = asset.add_batch_definition_whole_dataframe(f"{run}_batch")
    suite = context.suites.add(gx.ExpectationSuite(name=f"{run}_suite"))
    for exp in expectations:
        suite.add_expectation(exp)
    result = batch_def.get_batch(batch_parameters={"dataframe": df}).validate(suite)
    failures = [
        {"expectation": r.expectation_config.type, "column": r.expectation_config.kwargs.get("column"),
         "unexpected_count": (r.result or {}).get("unexpected_count")}
        for r in result.results if not r.success
    ]
    return {"table": name, "rows": len(df), "checks": len(result.results), "failures": failures}


def custom_checks(arrivals, headways) -> list[dict]:
    """Checks GX doesn't express well."""
    failures = []
    dupes = int(arrivals.duplicated(subset=["trip_key", "stop_id"]).sum())
    if dupes:
        failures.append({"expectation": "unique (trip_key, stop_id)", "table": "silver.arrivals",
                         "unexpected_count": dupes})
    if len(headways):
        # Most headways should have a schedule to compare to. If not, the schedule load
        # is broken or stale and every incident count is meaningless.
        unscheduled = float((headways["status"] == "UNSCHEDULED").mean())
        if unscheduled > 0.10:
            failures.append({"expectation": "at most 10% of headways unscheduled", "table": "silver.headways",
                             "observed": round(unscheduled, 3)})
    return failures


def main() -> None:
    import great_expectations.expectations as gxe
    import pandas as pd
    import trino

    ap = argparse.ArgumentParser()
    ap.add_argument("--date", type=date.fromisoformat, required=True, help="UTC date to check")
    args = ap.parse_args()
    lo, hi = args.date.isoformat(), (args.date + timedelta(days=1)).isoformat()

    conn = trino.dbapi.connect(host=os.environ.get("TRINO_HOST", "localhost"),
                               port=int(os.environ.get("TRINO_PORT", "8080")), user="quality", catalog="lake")
    rng = f"BETWEEN TIMESTAMP '{lo} 00:00:00 UTC' AND TIMESTAMP '{hi} 00:00:00 UTC'"
    def frame(sql: str) -> pd.DataFrame:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=[c[0] for c in cur.description])

    arrivals = frame(
        f"SELECT trip_key, stop_id, direction, date_diff('second', arrival_ts, observed_at) AS observe_lag_s "
        f"FROM silver.arrivals WHERE arrival_ts {rng}")
    headways = frame(f"SELECT trip_key, headway_s, status FROM silver.headways WHERE arrival_ts {rng}")

    results = []
    if len(arrivals):
        results.append(validate(arrivals, "silver_arrivals", arrival_expectations(gxe)))
    if len(headways):
        results.append(validate(headways, "silver_headways", headway_expectations(gxe)))
    extra = custom_checks(arrivals, headways)
    report = {"date": lo, "results": results, "custom_failures": extra}
    print(json.dumps(report, indent=2, default=str))
    if any(r["failures"] for r in results) or extra:
        sys.exit(1)


if __name__ == "__main__":
    main()
