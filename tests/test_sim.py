from datetime import datetime

from conftest import make_gtfs

from subwaypulse.changes import ChangeTracker
from subwaypulse.gtfsrt import parse_feed
from subwaypulse.rules import classify
from subwaypulse.schedule import NY, StaticGtfs
from subwaypulse.sim.feedgen import build_feeds
from subwaypulse.sim.model import MIN_SEPARATION_S, Simulation

START = datetime(2026, 10, 26, 8, 0, tzinfo=NY)


def sim(holds_per_hour=0.0):
    # Route "A" so the trains land in the real "ace" feed group
    return Simulation(StaticGtfs(make_gtfs({"A": 240}, n_stops=15)), {"A"}, START, 3600,
                      seed=3, holds_per_hour=holds_per_hour)


def test_trains_keep_their_distance():
    s = sim(holds_per_hour=6)
    by_stop = {}
    for route, _d, stop, _trip, ts in s.true_arrivals():
        by_stop.setdefault((route, stop), []).append(ts)
    for times in by_stop.values():
        times.sort()
        gaps = [b - a for a, b in zip(times, times[1:], strict=False)]
        assert min(gaps) >= MIN_SEPARATION_S - 1e-6


def test_a_hold_causes_a_gap_ahead_and_bunching_behind():
    s = sim(holds_per_hour=6)
    assert s.holds
    kinds = set()
    by_stop = {}
    for route, _d, stop, _trip, ts in s.true_arrivals():
        by_stop.setdefault((route, stop), []).append(ts)
    for times in by_stop.values():
        times.sort()
        for a, b in zip(times, times[1:], strict=False):
            kinds.add(classify(b - a, 240))
    assert {"GAP", "BUNCHING"} <= kinds


def test_feeds_parse_and_only_moving_trains_change():
    s = sim()
    tracker = ChangeTracker()
    now = START.timestamp() + 600
    feeds = build_feeds(s, now)
    _, first = parse_feed(feeds["ace"], "ace")
    assert first and all(snap.direction in ("N", "S") for snap in first)
    tracker.diff("ace", first)
    # Five seconds later almost nothing has moved
    _, again = parse_feed(build_feeds(s, now + 5)["ace"], "ace")
    assert len(tracker.diff("ace", again).changed) < len(first) / 2


def test_stops_leave_the_feed_once_the_train_departs():
    s = sim()
    t = next(t for t in s.trips if t.dep[0] > START.timestamp() + 300)
    mid = len(t.stop_ids) // 2
    before = [p[0] for p in s.predictions(t, t.arr[mid] - 1)]
    after = [p[0] for p in s.predictions(t, t.dep[mid] + 1)]
    assert t.stop_ids[mid] in before and t.stop_ids[mid] not in after
