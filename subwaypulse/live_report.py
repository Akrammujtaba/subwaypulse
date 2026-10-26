"""Summarize a live run in one markdown file.

    docker compose run --rm live-report

Pulls counters from Prometheus (collector and Flink) and row counts from Trino, so
every number in the README's live section comes from this output rather than from
memory. Run it after the stack has watched the live feeds for a while.
"""

from __future__ import annotations

import argparse
import json
import os
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from pathlib import Path


def prom(url: str, query: str) -> float | None:
    q = urllib.parse.urlencode({"query": query})
    with urllib.request.urlopen(f"{url}/api/v1/query?{q}", timeout=30) as r:
        data = json.load(r)["data"]["result"]
    if not data:
        return None
    return float(data[0]["value"][1])


def main() -> None:
    import trino

    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="evaluation/results-live.md")
    ap.add_argument("--since", help="only count rows from this UTC time on, e.g. 2026-10-26T22:30")
    args = ap.parse_args()
    # Trino timestamp literal; the default reaches back past any real run.
    since = f"TIMESTAMP '{(args.since or '1970-01-01T00:00').replace('T', ' ')} UTC'"
    p = os.environ.get("PROMETHEUS_URL", "http://localhost:9090")

    def total(metric: str, labels: str = "") -> float:
        return prom(p, f"sum({metric}{labels})") or 0.0

    naive = total("sp_naive_event_bytes_total")
    sent = total("sp_sent_event_bytes_total")
    fetches = total("sp_feed_fetches_total")
    unchanged = total("sp_feed_fetches_total", '{outcome="unchanged"}')
    errors = fetches - unchanged - total("sp_feed_fetches_total", '{outcome=~"changed|not_modified"}')
    downloaded = total("sp_feed_bytes_total")
    arch_in, arch_out = total("sp_archive_bytes_in"), total("sp_archive_bytes_out")
    arch_objects = total("sp_archive_objects")
    trains = total("sp_trips_active")
    feed_age_p50 = prom(p, "quantile(0.5, sp_feed_age_seconds)")
    ckpt = prom(p, "max(flink_jobmanager_job_lastCheckpointDuration)")
    outages = total("flink_taskmanager_job_task_operator_feedOutages")
    gaps_skipped = total("flink_taskmanager_job_task_operator_gapsSkippedForOutage")

    conn = trino.dbapi.connect(host=os.environ.get("TRINO_HOST", "localhost"),
                               port=int(os.environ.get("TRINO_PORT", "8080")), user="report", catalog="lake")
    cur = conn.cursor()

    def one(sql: str):
        cur.execute(sql)
        return cur.fetchone()

    first, last, n_arr, n_trips, n_stops = one(
        "SELECT min(arrival_ts), max(arrival_ts), count(*), count(DISTINCT trip_key), count(DISTINCT stop_id) "
        f"FROM silver.arrivals WHERE arrival_ts >= {since}")
    n_hw, n_sched, n_bunch, n_gap = one(
        "SELECT count(*), count_if(status <> 'UNSCHEDULED'), count_if(status = 'BUNCHING'), "
        f"count_if(status = 'GAP') FROM silver.headways WHERE arrival_ts >= {since}")
    gaps_live, = one(f"SELECT count(*) FROM gold.incident_events WHERE event_type = 'GAP_OPENED' AND event_ts >= {since}")
    detect = one(
        "SELECT approx_percentile(date_diff('second', arrival_ts, detected_at), 0.5), "
        f"approx_percentile(date_diff('second', arrival_ts, detected_at), 0.95) FROM silver.arrivals "
        f"WHERE arrival_ts >= {since}")
    bunch_lat = one(
        "SELECT approx_percentile(date_diff('second', event_ts, detected_at), 0.5), "
        "approx_percentile(date_diff('second', event_ts, detected_at), 0.95) "
        f"FROM gold.incident_events WHERE event_type = 'BUNCHING' AND event_ts >= {since}")
    gap_lat = one(
        "SELECT approx_percentile(date_diff('second', event_ts, detected_at), 0.5), "
        "approx_percentile(date_diff('second', event_ts, detected_at), 0.95) "
        f"FROM gold.incident_events WHERE event_type = 'GAP_OPENED' AND event_ts >= {since}")

    hours = (last - first).total_seconds() / 3600 if first and last else 0

    def pct(a: float, b: float) -> str:
        return "n/a" if not b else f"{100 * a / b:.1f}%"

    def s(v) -> str:
        return "n/a" if v is None else f"{v:.0f}s"

    lines = [
        "# Live run",
        "",
        f"Arrivals from {first:%Y-%m-%d %H:%M} to {last:%Y-%m-%d %H:%M} UTC ({hours:.1f} hours)." if first else "No arrivals yet.",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| Trains in the latest fetch | {trains:.0f} |",
        f"| Arrivals inferred | {n_arr:,} ({n_trips:,} trips, {n_stops:,} stops) |",
        f"| Headways measured | {n_hw:,} ({pct(n_sched, n_hw)} with a schedule to compare to) |",
        f"| Bunching / gaps (by headway) | {n_bunch:,} / {n_gap:,} |",
        f"| Gaps flagged while still open | {gaps_live:,} |",
        f"| Arrival available after it happened, p50 / p95 | {s(detect[0])} / {s(detect[1])} |",
        f"| Bunching flagged after the second train arrived, p50 / p95 | {s(bunch_lat[0])} / {s(bunch_lat[1])} |",
        f"| Gap flagged after the next train became overdue, p50 / p95 | {s(gap_lat[0])} / {s(gap_lat[1])} |",
        f"| Feed outages detected / gap alerts held back for them (since Flink started) | {outages:.0f} / {gaps_skipped:.0f} |",
        f"| Feed age, median across feeds | {s(feed_age_p50)} |",
        f"| Last Flink checkpoint | {'n/a' if ckpt is None else f'{ckpt / 1000:.1f}s'} |",
        "",
        "**Cost side (collector counters since its last start)**",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| Feed fetches | {fetches:,.0f} ({pct(unchanged, fetches)} had no new content, {pct(errors, fetches)} failed or stale) |",
        f"| Feed bytes downloaded | {downloaded / 1e6:,.1f} MB |",
        f"| Kafka event bytes sent vs. sending every trip every fetch | {sent / 1e6:,.2f} MB vs. {naive / 1e6:,.2f} MB ({pct(naive - sent, naive)} saved) |",
        f"| Raw archive: distinct snapshots vs. written to S3 | {arch_in / 1e6:,.1f} MB vs. {arch_out / 1e6:,.1f} MB in {arch_objects:.0f} objects |",
        "",
        f"Generated {datetime.now(tz=UTC):%Y-%m-%d %H:%M UTC} by `python -m subwaypulse.live_report`.",
    ]
    report = "\n".join(lines)
    print(report)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(report + "\n")


if __name__ == "__main__":
    main()
