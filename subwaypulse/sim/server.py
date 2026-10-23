"""Serve simulated feeds over HTTP at the same paths MTA uses.

    python -m subwaypulse.sim.server

Point the collector at it with FEED_BASE_URL=http://sim:8090/Dataservice/mtagtfsfeeds.

Settings (environment):
    GTFS_PATH            static schedule zip
    SIM_ROUTES           e.g. "A,C,E,L,G"
    SIM_START            local NYC time to simulate, e.g. "2026-10-26T07:30", or "now"
    SIM_DURATION_MINUTES how long to run (default 90)
    SIM_SPEED            1 = real time; 4 = four simulated seconds per second
    REFRESH_SECONDS      how often (simulated) a new snapshot is published (default 30)
    HOLDS_PER_HOUR       incidents injected per simulated hour (default 6)
    P_STALE, P_ERROR, P_BLINK   noise: serve the previous snapshot, return HTTP 503,
                         drop ~2% of trains from one snapshot
    TRUTH_DIR            where ground truth is written for the evaluator
"""

from __future__ import annotations

import csv
import json
import logging
import os
import random
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote

from google.transit import gtfs_realtime_pb2

from subwaypulse.feeds import SUBWAY_FEEDS
from subwaypulse.schedule import NY, StaticGtfs
from subwaypulse.sim.feedgen import build_feeds
from subwaypulse.sim.model import Simulation

log = logging.getLogger("sim")


class SimClock:
    def __init__(self, sim_start: float, speed: float) -> None:
        self.sim_start = sim_start
        self.speed = speed
        self.wall_start = time.time()

    def now(self) -> float:
        return self.sim_start + (time.time() - self.wall_start) * self.speed


class FeedStore:
    """Holds the current and previous snapshot of each feed."""

    def __init__(self, sim: Simulation, clock: SimClock, refresh_s: float, p_blink: float, rng: random.Random):
        self.sim, self.clock, self.refresh_s, self.p_blink, self.rng = sim, clock, refresh_s, p_blink, rng
        self.current: dict[str, bytes] = {}
        self.previous: dict[str, bytes] = {}
        self.lock = threading.Lock()
        self.snapshots = 0

    def refresh(self) -> None:
        now = self.clock.now()
        drop = None
        if self.rng.random() < self.p_blink:
            visible = [t.key for t in self.sim.trips if self.sim.visible(t, now)]
            drop = set(self.rng.sample(visible, max(1, len(visible) // 50))) if visible else None
        feeds = build_feeds(self.sim, now, drop)
        with self.lock:
            self.previous = self.current
            self.current = feeds
            self.snapshots += 1

    def run(self) -> None:
        while True:
            self.refresh()
            time.sleep(self.refresh_s / self.clock.speed)

    def get(self, feed_id: str, stale: bool) -> bytes:
        with self.lock:
            src = self.previous if stale and self.previous else self.current
            raw = src.get(feed_id)
        if raw is None:
            empty = gtfs_realtime_pb2.FeedMessage()
            empty.header.gtfs_realtime_version = "1.0"
            empty.header.timestamp = int(self.clock.now())
            raw = empty.SerializeToString()
        return raw


def write_truth(sim: Simulation, clock: SimClock, out: Path, routes: list[str]) -> None:
    out.mkdir(parents=True, exist_ok=True)
    with open(out / "arrivals.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["route_id", "direction", "stop_id", "trip_key", "arrival_ts"])
        for row in sim.true_arrivals():
            w.writerow([*row[:4], f"{row[4]:.0f}"])
    with open(out / "holds.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["trip_key", "route_id", "direction", "stop_id", "start_ts", "seconds"])
        for h in sim.holds:
            w.writerow([h.trip_key, h.route_id, h.direction, h.stop_id, f"{h.start:.0f}", h.seconds])
    (out / "meta.json").write_text(json.dumps({
        "sim_start": clock.sim_start, "sim_end": sim.end, "speed": clock.speed,
        "wall_start": clock.wall_start, "routes": routes, "trips": len(sim.trips),
    }, indent=2))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    routes = [r.strip() for r in os.environ.get("SIM_ROUTES", "A,C,E,L,G").split(",") if r.strip()]
    start_env = os.environ.get("SIM_START", "now")
    start = datetime.now(tz=NY) if start_env == "now" else datetime.fromisoformat(start_env).replace(tzinfo=NY)
    duration_s = int(float(os.environ.get("SIM_DURATION_MINUTES", "90")) * 60)
    speed = float(os.environ.get("SIM_SPEED", "1"))
    seed = int(os.environ.get("SIM_SEED", "7"))
    rng = random.Random(seed + 1)

    t0 = time.time()
    gtfs = StaticGtfs(os.environ.get("GTFS_PATH", "/data/gtfs_subway.zip"))
    sim = Simulation(gtfs, set(routes), start, duration_s, seed=seed,
                     holds_per_hour=float(os.environ.get("HOLDS_PER_HOUR", "6")))
    log.info("built %d trips and %d holds for %s in %.1fs", len(sim.trips), len(sim.holds),
             ",".join(routes), time.time() - t0)

    clock = SimClock(start.timestamp(), speed)
    write_truth(sim, clock, Path(os.environ.get("TRUTH_DIR", "/truth")), routes)

    store = FeedStore(sim, clock, float(os.environ.get("REFRESH_SECONDS", "30")),
                      float(os.environ.get("P_BLINK", "0.05")), rng)
    store.refresh()
    threading.Thread(target=store.run, daemon=True).start()

    p_stale = float(os.environ.get("P_STALE", "0.02"))
    p_error = float(os.environ.get("P_ERROR", "0.01"))
    path_to_feed = {f"/Dataservice/mtagtfsfeeds/{unquote(p)}": fid for fid, p in SUBWAY_FEEDS.items()}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            path = unquote(self.path.split("?")[0])
            if path == "/health":
                return self._send(200, b"ok", "text/plain")
            if path == "/clock":
                return self._send(200, json.dumps({"sim_now": clock.now()}).encode(), "application/json")
            fid = path_to_feed.get(path)
            if fid is None:
                return self._send(404, b"not found", "text/plain")
            if rng.random() < p_error:
                return self._send(503, b"busy", "text/plain")
            self._send(200, store.get(fid, stale=rng.random() < p_stale), "application/x-protobuf")

        def _send(self, code: int, body: bytes, ctype: str) -> None:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):  # keep logs quiet
            pass

    port = int(os.environ.get("PORT", "8090"))
    log.info("serving on :%d, sim start %s, speed %sx", port, start.isoformat(), speed)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()


if __name__ == "__main__":
    main()
