"""Score the pipeline against the simulator's ground truth.

    python -m subwaypulse.evaluate --out evaluation/results.md

Compares what the pipeline wrote to Iceberg with what really happened in the
simulation:

- Arrivals: how many real arrivals the pipeline found, how many it made up, and how
  far its arrival times are from the true ones.
- Incidents: true bunching and gaps are computed from the true arrivals with the same
  rules (subwaypulse/rules.py) and the same schedule. Then recall, precision, and
  detection latency: how long after the problem became knowable the pipeline flagged
  it. For bunching that's the second train's arrival; for a gap, the moment the next
  train became overdue.

The first minutes are skipped (warm-up: the pipeline needs one train per stop before
it can measure a headway) and so are the last few (results still in flight).
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from subwaypulse.rules import bunching_threshold_s, classify, gap_threshold_s
from subwaypulse.schedule import NY


@dataclass
class Window:
    start: float
    end: float


def pct(xs: list[float], p: float) -> float | None:
    if not xs:
        return None
    xs = sorted(xs)
    k = min(len(xs) - 1, max(0, round(p / 100 * (len(xs) - 1))))
    return xs[k]


def sched_key(route: str, stop: str, ts: float) -> tuple[str, str, str, int]:
    t = datetime.fromtimestamp(ts, tz=NY)
    return route, stop, t.date().isoformat(), t.hour


# ---------- truth ----------

def load_truth(truth_dir: Path):
    meta = json.loads((truth_dir / "meta.json").read_text())
    arrivals = []
    with open(truth_dir / "arrivals.csv") as f:
        for r in csv.DictReader(f):
            arrivals.append((r["route_id"], r["direction"], r["stop_id"], r["trip_key"], float(r["arrival_ts"])))
    return meta, arrivals


def true_incidents(arrivals, schedule: dict, window: Window, observed_from: float):
    """Return (bunching, gaps). Bunching keyed by (route, stop, trip_key of the second
    train); gaps keyed by (route, stop, prev_trip_key). Values: knowable-at time.

    Incidents whose first train passed before `observed_from` (when the feed started)
    are left out: no pipeline could have seen that train arrive."""
    by_stop = defaultdict(list)
    for route, _d, stop, trip, ts in arrivals:
        by_stop[(route, stop)].append((ts, trip))
    bunching, gaps = {}, {}
    for (route, stop), rows in by_stop.items():
        rows.sort()
        for i, (t_prev, trip_prev) in enumerate(rows):
            if t_prev < observed_from:
                continue
            t_next, trip_next = rows[i + 1] if i + 1 < len(rows) else (math.inf, None)
            if trip_next is not None:
                sched = schedule.get(sched_key(route, stop, t_next))
                if classify(t_next - t_prev, sched) == "BUNCHING" and window.start <= t_next <= window.end:
                    bunching[(route, stop, trip_next)] = t_next
            # A gap is judged the way the pipeline's timer judges it: on the schedule in
            # force when the previous train arrived, and only if service is still
            # scheduled at the deadline. It counts even if no train ever came after.
            sched_prev = schedule.get(sched_key(route, stop, t_prev))
            if sched_prev is None:
                continue
            deadline = t_prev + gap_threshold_s(sched_prev)
            if schedule.get(sched_key(route, stop, deadline)) is None:
                continue
            if t_next > deadline and window.start <= deadline <= window.end:
                gaps[(route, stop, trip_prev)] = deadline
    return bunching, gaps


# ---------- pipeline output ----------

def query(cur, sql: str) -> list[tuple]:
    cur.execute(sql)
    return cur.fetchall()


def ts(v) -> float:
    return v.timestamp() if isinstance(v, datetime) else float(v)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--truth", default=os.environ.get("TRUTH_DIR", "/truth"))
    ap.add_argument("--out", default="evaluation/results.md")
    ap.add_argument("--warmup-minutes", type=float, default=10)
    ap.add_argument("--settle-minutes", type=float, default=5)
    ap.add_argument("--until-minutes", type=float, default=None,
                    help="end the scored window this many simulated minutes after the start, "
                         "to compare two runs over the same stretch")
    args = ap.parse_args()

    import psycopg
    import trino

    meta, truth_arrivals = load_truth(Path(args.truth))
    speed = meta["speed"]

    def wall_to_sim(wall_s: float) -> float:
        return meta["sim_start"] + (wall_s - meta["wall_start"]) * speed

    sim_now = wall_to_sim(time.time())
    window = Window(meta["sim_start"] + args.warmup_minutes * 60,
                    min(sim_now, meta["sim_end"]) - args.settle_minutes * 60)
    if args.until_minutes is not None:
        window.end = min(window.end, meta["sim_start"] + args.until_minutes * 60)
    if window.end <= window.start:
        raise SystemExit("not enough simulated time has passed yet; let it run longer")

    routes = set(meta["routes"])
    with psycopg.connect(os.environ.get("REF_DSN", "postgresql://subway:subway@localhost:5432/subway")) as pg:
        schedule = {(r, s, d.isoformat(), h): v for r, s, d, h, v in pg.execute(
            "SELECT route_id, stop_id, local_date, hour, scheduled_headway_s FROM ref.scheduled_headways "
            "WHERE route_id = ANY(%s)", (list(routes),)).fetchall()}

    conn = trino.dbapi.connect(host=os.environ.get("TRINO_HOST", "localhost"),
                               port=int(os.environ.get("TRINO_PORT", "8080")), user="evaluator",
                               catalog="lake")
    cur = conn.cursor()
    lo = datetime.fromtimestamp(window.start - 3600, tz=UTC).strftime("%Y-%m-%d %H:%M:%S")
    hi = datetime.fromtimestamp(window.end + 3600, tz=UTC).strftime("%Y-%m-%d %H:%M:%S")
    route_list = ",".join(f"'{r}'" for r in sorted(routes))
    rng = f"TIMESTAMP '{lo} UTC' AND TIMESTAMP '{hi} UTC'"

    p_arrivals = query(cur, f"""
        SELECT trip_key, stop_id, arrival_ts, detected_at FROM silver.arrivals
        WHERE arrival_ts BETWEEN {rng} AND route_id IN ({route_list})""")
    p_incidents = query(cur, f"""
        SELECT event_type, route_id, stop_id, trip_key, prev_trip_key, event_ts, detected_at
        FROM gold.incident_events WHERE event_ts BETWEEN {rng} AND route_id IN ({route_list})""")

    # ----- arrivals -----
    truth_by_key = {(trip, stop): t for _r, _d, stop, trip, t in truth_arrivals}
    in_window = {k: t for k, t in truth_by_key.items() if window.start <= t <= window.end}
    found = {}
    for trip, stop, at, det in p_arrivals:
        found[(trip, stop)] = (ts(at), wall_to_sim(ts(det)))
    matched = [k for k in in_window if k in found]
    errors = [abs(found[k][0] - in_window[k]) for k in matched]
    arrival_lag = [found[k][1] - in_window[k] for k in matched]
    made_up = [k for k, (at, _) in found.items() if window.start <= at <= window.end and k not in truth_by_key]

    # ----- incidents -----
    t_bunch, t_gap = true_incidents(truth_arrivals, schedule, window, observed_from=meta["sim_start"] + 60)
    # key -> (event time as the pipeline saw it, detection time in simulated seconds)
    p_bunch, p_gap = {}, {}
    for etype, route, stop, trip, prev_trip, ev, det in p_incidents:
        target, key = (p_bunch, (route, stop, trip)) if etype == "BUNCHING" else (p_gap, (route, stop, prev_trip))
        if etype not in ("BUNCHING", "GAP_OPENED"):
            continue
        d = wall_to_sim(ts(det))
        if key not in target or d < target[key][1]:
            target[key] = (ts(ev), d)

    def score(truth: dict, pred: dict):
        hit = [k for k in truth if k in pred]
        latency = [pred[k][1] - truth[k] for k in hit]
        # A flagged incident inside the window with no real incident behind it is a false alarm.
        false = [k for k, (ev, _) in pred.items() if window.start <= ev <= window.end and k not in truth]
        return hit, latency, false

    b_hit, b_lat, b_false = score(t_bunch, p_bunch)
    g_hit, g_lat, g_false = score(t_gap, p_gap)

    def fmt(x, unit="s"):
        return "n/a" if x is None else f"{x:.0f}{unit}"

    def ratio(a, b):
        return "n/a" if b == 0 else f"{a} / {b} ({100 * a / b:.1f}%)"

    def precision(hits, false):
        n = len(hits) + len(false)
        return "n/a" if n == 0 else f"{100 * len(hits) / n:.1f}% ({len(false)} false)"

    start_local = datetime.fromtimestamp(window.start, tz=NY).strftime("%Y-%m-%d %H:%M")
    end_local = datetime.fromtimestamp(window.end, tz=NY).strftime("%H:%M")
    lines = [
        "# Evaluation results",
        "",
        f"Simulated routes {', '.join(sorted(routes))}, {meta['trips']} trips, speed {speed}x. "
        f"Scored window {start_local} to {end_local} NYC time ({(window.end - window.start) / 60:.0f} simulated minutes).",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| Real arrivals found | {ratio(len(matched), len(in_window))} |",
        f"| Arrivals with no real counterpart | {len(made_up)} |",
        f"| Arrival time error p50 / p95 | {fmt(pct(errors, 50))} / {fmt(pct(errors, 95))} |",
        f"| Arrival available after it happened p50 / p95 | {fmt(pct(arrival_lag, 50))} / {fmt(pct(arrival_lag, 95))} |",
        f"| Bunching detected (recall) | {ratio(len(b_hit), len(t_bunch))} |",
        f"| Bunching precision | {precision(b_hit, b_false)} |",
        f"| Bunching detection latency p50 / p95 | {fmt(pct(b_lat, 50))} / {fmt(pct(b_lat, 95))} |",
        f"| Gaps detected while open (recall) | {ratio(len(g_hit), len(t_gap))} |",
        f"| Gap precision | {precision(g_hit, g_false)} |",
        f"| Gap detection latency p50 / p95 | {fmt(pct(g_lat, 50))} / {fmt(pct(g_lat, 95))} |",
        "",
        f"Thresholds: bunching at max(90s, 30% of scheduled headway), e.g. {bunching_threshold_s(240)}s on a "
        f"4-minute schedule; gap at scheduled + max(5 min, scheduled), e.g. {gap_threshold_s(240)}s.",
        "Latencies are in simulated seconds from when the problem became knowable.",
        "",
    ]
    lines.append(f"Generated {datetime.now(tz=UTC).strftime('%Y-%m-%d %H:%M UTC')}.")
    report = "\n".join(lines)
    print(report)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(report + "\n")
    # Keep the raw misses for debugging
    misses = {
        "missed_bunching": [list(k) for k in t_bunch if k not in p_bunch][:50],
        "false_bunching": [list(k) for k in b_false][:50],
        "missed_gaps": [list(k) for k in t_gap if k not in p_gap][:50],
        "false_gaps": [list(k) for k in g_false][:50],
    }
    Path(args.out).with_suffix(".misses.json").write_text(json.dumps(misses, indent=1))


if __name__ == "__main__":
    main()
