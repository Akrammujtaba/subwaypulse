"""A subway simulator that publishes GTFS-realtime feeds with a known ground truth.

The live MTA feeds can't tell us when a train *really* arrived, so they can't score
the pipeline. The simulator can: it takes the real static schedule, moves every
train along it with realistic noise and injected incidents, records the true arrival
time of every train at every stop, and serves feeds in the same format as MTA's.

How trains move:
- Each train starts a little early or late and drifts by a few seconds per stop.
- Incidents hold a train at a stop for 4-12 minutes (a sick passenger, a door
  problem). Predictions don't know about a hold until it starts.
- A train can't arrive at a stop less than MIN_SEPARATION_S after the train ahead on
  the same route, so a held train blocks the ones behind it. That creates the real
  pattern: a gap in front of the held train and bunching behind it.

How the feed looks at time t:
- Every train that is running (or leaves its terminal within 10 minutes) appears.
- A train at a platform shows its arrival time with some noise (std dev 15s), because
  the real signal system doesn't timestamp arrivals to the second either.
- It lists the stops it has not left yet, with predicted times. Predictions get
  more accurate as the train gets closer, and miss the unknown part of any hold.
"""

from __future__ import annotations

import random
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

from subwaypulse.schedule import NY, StaticGtfs, scheduled_arrivals

DWELL_S = 30
MIN_SEPARATION_S = 75
VISIBLE_BEFORE_S = 600
# The signal system doesn't know to the second when a train stopped. The feed's
# arrival time for a train at the platform is off by this much (std dev).
OBSERVATION_NOISE_S = 15

# Route -> MTA feed group. Only routes listed here can be simulated.
ROUTE_FEED = {
    "A": "ace", "C": "ace", "E": "ace", "H": "ace", "FS": "ace",
    "B": "bdfm", "D": "bdfm", "F": "bdfm", "FX": "bdfm", "M": "bdfm",
    "G": "g", "J": "jz", "Z": "jz", "L": "l",
    "N": "nqrw", "Q": "nqrw", "R": "nqrw", "W": "nqrw",
    "1": "1234567", "2": "1234567", "3": "1234567", "4": "1234567", "5": "1234567",
    "6": "1234567", "6X": "1234567", "7": "1234567", "7X": "1234567", "GS": "1234567",
    "SI": "si",
}


@dataclass
class Hold:
    trip_key: str
    route_id: str
    direction: str
    stop_id: str
    start: float  # epoch seconds: when the train arrives at the stop and the hold starts
    seconds: int


@dataclass
class SimTrip:
    key: str  # start_date:rt_trip_id
    rt_trip_id: str
    start_date: str
    route_id: str
    direction: str
    stop_ids: list[str]
    sched: list[float]  # epoch seconds
    arr: list[float] = field(default_factory=list)
    dep: list[float] = field(default_factory=list)
    # stop index -> hold seconds, and per-stop prediction error at long range
    holds: dict[int, int] = field(default_factory=dict)
    pred_err: list[float] = field(default_factory=list)
    # how far off the feed's own "arrived at" time is from the true one
    obs_err: list[float] = field(default_factory=list)

    @property
    def feed_id(self) -> str:
        return ROUTE_FEED[self.route_id]


def rt_trip_id(static_trip_id: str) -> str:
    """Static 'ASP26GEN-1038-Sunday-00_000600_1..S03R' -> realtime '000600_1..S03R'."""
    return static_trip_id.split("_", 1)[1] if "_" in static_trip_id else static_trip_id


def _direction(rt_id: str) -> str:
    tail = rt_id.split(".")[-1]
    return tail[0] if tail[:1] in ("N", "S") else "N"


class Simulation:
    def __init__(self, gtfs: StaticGtfs, routes: set[str], start: datetime, duration_s: int,
                 seed: int = 7, holds_per_hour: float = 6.0) -> None:
        self.rng = random.Random(seed)
        self.start = start.timestamp()
        self.end = self.start + duration_s
        self.trips: list[SimTrip] = []
        self.holds: list[Hold] = []
        self._build(gtfs, routes, start, duration_s)
        self._move_trains(holds_per_hour, duration_s)

    # ---------- build ----------
    def _build(self, gtfs: StaticGtfs, routes: set[str], start: datetime, duration_s: int) -> None:
        lo = self.start - 3 * 3600
        hi = self.end + 3600
        local_start = start.astimezone(NY).date()
        days = [local_start - timedelta(days=1), local_start, local_start + timedelta(days=1)]
        stops: dict[tuple[str, date], list[tuple[float, str, str]]] = defaultdict(list)
        for route_id, stop_id, trip_id, ts, sd in scheduled_arrivals(gtfs, days, routes):
            stops[(trip_id, sd)].append((ts.timestamp(), stop_id, route_id))
        for (trip_id, sd), rows in stops.items():
            rows.sort()
            if rows[-1][0] < lo or rows[0][0] > hi:
                continue
            rid = rt_trip_id(trip_id)
            ymd = sd.strftime("%Y%m%d")
            self.trips.append(SimTrip(
                key=f"{ymd}:{rid}", rt_trip_id=rid, start_date=ymd, route_id=rows[0][2],
                direction=_direction(rid), stop_ids=[r[1] for r in rows], sched=[r[0] for r in rows],
            ))
        # Same key twice would mean two copies of one train; keep the first.
        seen: set[str] = set()
        self.trips = [t for t in sorted(self.trips, key=lambda t: t.sched[0])
                      if not (t.key in seen or seen.add(t.key))]

    # ---------- movement ----------
    def _move_trains(self, holds_per_hour: float, duration_s: int) -> None:
        rng = self.rng
        # Pick which trains get held: only trains running inside the evaluation window.
        candidates = [t for t in self.trips
                      if t.sched[0] < self.end - 900 and t.sched[-1] > self.start + 300 and len(t.stop_ids) > 6]
        n_holds = min(len(candidates), round(holds_per_hour * duration_s / 3600))
        for t in rng.sample(candidates, n_holds):
            # Hold somewhere in the middle of the route, at a time inside the window.
            idx = [i for i in range(2, len(t.stop_ids) - 3) if self.start + 120 < t.sched[i] < self.end - 900]
            if idx:
                t.holds[rng.choice(idx)] = rng.randint(240, 720)

        # Order trains per route and direction so the separation rule can look at the one ahead.
        by_line: dict[tuple[str, str], list[SimTrip]] = defaultdict(list)
        for t in self.trips:
            by_line[(t.route_id, t.direction)].append(t)
        for line in by_line.values():
            line.sort(key=lambda t: t.sched[0])
            last_arrival: dict[str, float] = {}
            for t in line:
                delay = rng.uniform(-20, 60)
                for i, sched in enumerate(t.sched):
                    if i > 0:
                        delay = max(-60.0, delay + rng.gauss(1, 6))
                    arr = sched + delay
                    ahead = last_arrival.get(t.stop_ids[i])
                    if ahead is not None and arr < ahead + MIN_SEPARATION_S:
                        arr = ahead + MIN_SEPARATION_S
                        delay = arr - sched
                    if i > 0 and arr < t.dep[-1] + 30:
                        arr = t.dep[-1] + 30  # can't teleport between stops
                        delay = arr - sched
                    dwell = 0 if i == len(t.sched) - 1 else DWELL_S
                    hold = t.holds.get(i, 0)
                    if hold:
                        self.holds.append(Hold(t.key, t.route_id, t.direction, t.stop_ids[i], arr, hold))
                    dep = arr + dwell + hold
                    delay += hold
                    t.arr.append(arr)
                    t.dep.append(dep)
                    last_arrival[t.stop_ids[i]] = arr
                t.pred_err = [rng.gauss(0, 90) for _ in t.sched]
                t.obs_err = [rng.gauss(0, OBSERVATION_NOISE_S) for _ in t.sched]

    # ---------- what the feed shows ----------
    def visible(self, t: SimTrip, now: float) -> bool:
        return t.dep[0] - VISIBLE_BEFORE_S <= now < t.arr[-1]

    def predictions(self, t: SimTrip, now: float) -> list[tuple[str, int | None, int | None]]:
        """(stop_id, predicted arrival, predicted departure) for stops not yet left.

        Predictions are recomputed only when the train arrives at or leaves a stop
        (the signal system sees it move), so between those moments the trip looks the
        same from one snapshot to the next, as it does in the real feeds.
        """
        events = [e for pair in zip(t.arr, t.dep, strict=True) for e in pair if e <= now]
        basis = max(events) if events else t.dep[0] - VISIBLE_BEFORE_S

        def hidden_hold(j: int, hold: int) -> float:
            """How much of the hold at stop j the dispatcher can't see yet."""
            if basis < t.arr[j]:
                return hold
            return max(0.0, t.arr[j] + hold - now)

        out = []
        for i, stop in enumerate(t.stop_ids):
            last = i == len(t.stop_ids) - 1
            if t.dep[i] <= now and not last:
                continue
            if t.arr[i] <= now:
                # At the platform: the arrival already happened and is known exactly.
                # While held, the departure estimate keeps slipping.
                arr = t.arr[i] + t.obs_err[i]
                dep = None if last else max(now, t.dep[i] - hidden_hold(i, t.holds.get(i, 0)))
            else:
                hidden = sum(hidden_hold(j, h) for j, h in t.holds.items() if j < i)
                horizon = min(1.0, max(0.0, (t.arr[i] - basis) / 1800))
                arr = t.arr[i] - hidden + t.pred_err[i] * horizon
                dep = None if last else arr + (t.dep[i] - t.arr[i])
            if i == 0:
                # Like the real feed, the origin terminal only has a departure time.
                out.append((stop, None, int(dep if dep is not None else arr)))
            else:
                out.append((stop, int(arr), None if dep is None else int(dep)))
        return out

    def last_moved(self, t: SimTrip, now: float) -> float:
        """When the train last arrived at or left a stop. Using this (not "now") as the
        vehicle timestamp means an idle train looks the same from one snapshot to the next."""
        events = [e for pair in zip(t.arr, t.dep, strict=True) for e in pair if e <= now]
        return max(events) if events else now

    def vehicle(self, t: SimTrip, now: float) -> tuple[str, str]:
        """(status, stop_id) for the VehiclePosition."""
        for i, stop in enumerate(t.stop_ids):
            if t.arr[i] <= now < t.dep[i]:
                return "STOPPED_AT", stop
            if now < t.arr[i]:
                return ("IN_TRANSIT_TO" if now >= t.dep[0] else "STOPPED_AT"), stop
        return "STOPPED_AT", t.stop_ids[-1]

    def true_arrivals(self, until: float | None = None):
        """Yield (route_id, direction, stop_id, trip_key, arrival_ts) for every real arrival.

        The origin terminal is included, using the departure time: that's when a
        rider at the first stop actually gets a train.
        """
        for t in self.trips:
            for i, stop in enumerate(t.stop_ids):
                ts = t.dep[0] if i == 0 else t.arr[i]
                if until is None or ts <= until:
                    yield t.route_id, t.direction, stop, t.key, ts
