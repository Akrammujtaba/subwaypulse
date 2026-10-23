"""Iceberg table maintenance through Trino.

    python -m subwaypulse.maintenance

The Flink job commits to every table on every checkpoint (every 30 seconds), and
each commit adds small data files and a snapshot. Left alone that is ~2,900 commits
per table per day: reads slow down (more files to open) and storage grows
(snapshots keep deleted files alive). Every run:

1. compacts small files in the current and previous day's partitions
2. expires snapshots older than the retention window
3. removes orphan files (written by a commit that never finished)

and records file counts before and after in `ops.maintenance_log`, so the effect is
measured rather than assumed.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import UTC, datetime, timedelta

log = logging.getLogger("maintenance")

TABLES = {
    # table -> partition timestamp column, for limiting compaction to recent days
    "bronze.trip_events": "feed_ts",
    "silver.arrivals": "arrival_ts",
    "silver.headways": "arrival_ts",
    "gold.incident_events": "event_ts",
}


def file_stats(cur, table: str) -> tuple[int, int]:
    cur.execute(f'SELECT count(*), coalesce(sum(file_size_in_bytes), 0) FROM {table_ref(table, "files")}')
    n, size = cur.fetchone()
    return int(n), int(size)


def table_ref(table: str, meta: str | None = None) -> str:
    schema, name = table.split(".")
    return f'{schema}."{name}${meta}"' if meta else f"{schema}.{name}"


def ensure_log_table(cur) -> None:
    cur.execute("CREATE SCHEMA IF NOT EXISTS ops")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ops.maintenance_log (
            run_at timestamp(6) with time zone,
            table_name varchar,
            files_before bigint,
            bytes_before bigint,
            files_after bigint,
            bytes_after bigint,
            seconds double
        )""")
    cur.fetchall()


def maintain(cur, table: str, ts_col: str, now: datetime, retention: str) -> dict:
    t0 = time.time()
    files_before, bytes_before = file_stats(cur, table)
    since = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    steps = [
        # Rewrite files under the threshold, only in recent partitions
        f"ALTER TABLE {table_ref(table)} EXECUTE optimize(file_size_threshold => '32MB') "
        f"WHERE {ts_col} >= TIMESTAMP '{since} 00:00:00 UTC'",
        f"ALTER TABLE {table_ref(table)} EXECUTE expire_snapshots(retention_threshold => '{retention}')",
        f"ALTER TABLE {table_ref(table)} EXECUTE remove_orphan_files(retention_threshold => '{retention}')",
    ]
    for sql in steps:
        cur.execute(sql)
        cur.fetchall()
    files_after, bytes_after = file_stats(cur, table)
    row = {"table": table, "files_before": files_before, "bytes_before": bytes_before,
           "files_after": files_after, "bytes_after": bytes_after, "seconds": round(time.time() - t0, 1)}
    cur.execute(
        "INSERT INTO ops.maintenance_log VALUES (?, ?, ?, ?, ?, ?, ?)",
        [now, table, files_before, bytes_before, files_after, bytes_after, row["seconds"]],
    )
    cur.fetchall()
    return row


def main() -> None:
    import trino

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    conn = trino.dbapi.connect(host=os.environ.get("TRINO_HOST", "localhost"),
                               port=int(os.environ.get("TRINO_PORT", "8080")),
                               user="maintenance", catalog="lake")
    cur = conn.cursor()
    ensure_log_table(cur)
    retention = os.environ.get("SNAPSHOT_RETENTION", "6h")
    now = datetime.now(tz=UTC)
    for table, ts_col in TABLES.items():
        row = maintain(cur, table, ts_col, now, retention)
        log.info("%s: %d files (%.1f MB) -> %d files (%.1f MB) in %ss", table, row["files_before"],
                 row["bytes_before"] / 1e6, row["files_after"], row["bytes_after"] / 1e6, row["seconds"])


if __name__ == "__main__":
    main()
