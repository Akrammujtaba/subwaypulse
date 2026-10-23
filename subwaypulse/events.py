"""Build TripEvent records (see schemas/trip_event.avsc) from snapshots."""

from __future__ import annotations

import json
from pathlib import Path

from subwaypulse.gtfsrt import TripSnapshot

SCHEMA_PATH = Path(__file__).parent / "schemas" / "trip_event.avsc"


def load_schema() -> dict:
    return json.loads(SCHEMA_PATH.read_text())


def _ms(sec: int | None) -> int | None:
    return None if sec is None else sec * 1000


def trip_event(snap: TripSnapshot, event_type: str, fetched_at_ms: int, feed_ts: int | None = None) -> dict:
    """`feed_ts` overrides the snapshot's own timestamp; VANISHED events use the
    timestamp of the fetch that noticed the trip was gone."""
    return {
        "event_type": event_type,
        "feed_id": snap.feed_id,
        "feed_ts": (feed_ts if feed_ts is not None else snap.feed_ts) * 1000,
        "fetched_at": fetched_at_ms,
        "trip_key": snap.key,
        "trip_id": snap.trip_id,
        "start_date": snap.start_date,
        "route_id": snap.route_id,
        "direction": snap.direction,
        "vehicle_status": snap.vehicle_status,
        "vehicle_stop_id": snap.vehicle_stop_id,
        "vehicle_ts": _ms(snap.vehicle_ts),
        "stops": [
            {"stop_id": s.stop_id, "arrival_ts": _ms(s.arrival_ts), "departure_ts": _ms(s.departure_ts)}
            for s in snap.stops
        ],
    }


def heartbeat(feed_id: str, feed_ts: int, fetched_at_ms: int) -> dict:
    return {
        "event_type": "HEARTBEAT",
        "feed_id": feed_id,
        "feed_ts": feed_ts * 1000,
        "fetched_at": fetched_at_ms,
        "trip_key": None,
        "trip_id": None,
        "start_date": None,
        "route_id": None,
        "direction": None,
        "vehicle_status": None,
        "vehicle_stop_id": None,
        "vehicle_ts": None,
        "stops": [],
    }
