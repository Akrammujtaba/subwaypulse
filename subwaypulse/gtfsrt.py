"""Turn a GTFS-realtime FeedMessage into one snapshot per active trip.

An NYC subway feed carries two entities per train: a TripUpdate (predicted arrival
times for the stops it has not reached yet) and a VehiclePosition (where it is now).
We merge them by trip so downstream code sees one record per train.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

from google.transit import gtfs_realtime_pb2

STATUS_NAMES = {
    0: "INCOMING_AT",
    1: "STOPPED_AT",
    2: "IN_TRANSIT_TO",
}


@dataclass(frozen=True)
class StopTime:
    stop_id: str
    arrival_ts: int | None  # epoch seconds
    departure_ts: int | None


@dataclass
class TripSnapshot:
    feed_id: str
    feed_ts: int  # feed header timestamp, epoch seconds
    trip_id: str
    start_date: str
    route_id: str
    direction: str  # "N" or "S"
    stops: list[StopTime] = field(default_factory=list)
    vehicle_status: str | None = None
    vehicle_stop_id: str | None = None
    vehicle_ts: int | None = None

    @property
    def key(self) -> str:
        return f"{self.start_date}:{self.trip_id}"

    def fingerprint(self) -> str:
        """Hash of everything that matters downstream.

        The feed timestamp and vehicle timestamp are left out on purpose: they change
        on every refresh even when the train has not moved and no prediction changed.
        """
        h = hashlib.blake2b(digest_size=16)
        h.update(f"{self.route_id}|{self.direction}|{self.vehicle_status}|{self.vehicle_stop_id}".encode())
        for s in self.stops:
            h.update(f"|{s.stop_id},{s.arrival_ts},{s.departure_ts}".encode())
        return h.hexdigest()


def direction_of(trip_id: str, stops: list[StopTime]) -> str | None:
    """NYCT trip ids look like `000600_1..S03R`: the letter after `..` is the direction.

    Some trips use a single dot (`..` is padding), so fall back to the suffix of the
    stop ids, which always end in N or S on the subway.
    """
    if "." in trip_id:
        tail = trip_id.split(".")[-1]
        if tail[:1] in ("N", "S"):
            return tail[0]
    for s in stops:
        if s.stop_id[-1:] in ("N", "S"):
            return s.stop_id[-1]
    return None


def _ts(event) -> int | None:
    # proto3-style optional: 0 means not set for these feeds
    return int(event.time) if event.time else None


def parse_feed(raw: bytes, feed_id: str) -> tuple[int, list[TripSnapshot]]:
    """Parse raw protobuf bytes. Returns (feed_ts, snapshots)."""
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(raw)
    feed_ts = int(msg.header.timestamp)

    trips: dict[str, TripSnapshot] = {}

    def get(trip) -> TripSnapshot | None:
        if not trip.trip_id:
            return None
        start_date = trip.start_date or ""
        k = f"{start_date}:{trip.trip_id}"
        snap = trips.get(k)
        if snap is None:
            snap = TripSnapshot(
                feed_id=feed_id,
                feed_ts=feed_ts,
                trip_id=trip.trip_id,
                start_date=start_date,
                route_id=trip.route_id,
                direction="",
            )
            trips[k] = snap
        if trip.route_id and not snap.route_id:
            snap.route_id = trip.route_id
        return snap

    for ent in msg.entity:
        if ent.HasField("trip_update"):
            tu = ent.trip_update
            snap = get(tu.trip)
            if snap is None:
                continue
            snap.stops = [
                StopTime(
                    stop_id=stu.stop_id,
                    arrival_ts=_ts(stu.arrival) if stu.HasField("arrival") else None,
                    departure_ts=_ts(stu.departure) if stu.HasField("departure") else None,
                )
                for stu in tu.stop_time_update
                if stu.stop_id
            ]
        elif ent.HasField("vehicle"):
            v = ent.vehicle
            snap = get(v.trip)
            if snap is None:
                continue
            snap.vehicle_status = STATUS_NAMES.get(v.current_status) if v.HasField("current_status") else None
            snap.vehicle_stop_id = v.stop_id or None
            snap.vehicle_ts = int(v.timestamp) if v.timestamp else None

    out = []
    for snap in trips.values():
        d = direction_of(snap.trip_id, snap.stops)
        if d is None or not snap.route_id:
            # Can't place it on a line and direction; nothing downstream can use it.
            continue
        snap.direction = d
        out.append(snap)
    return feed_ts, out


def content_hash(raw: bytes) -> str:
    """Hash of a feed with its header timestamp cleared.

    Two fetches with the same content hash carry the same information, even though the
    bytes differ because the header timestamp moved.
    """
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.ParseFromString(raw)
    msg.header.ClearField("timestamp")
    return hashlib.blake2b(msg.SerializeToString(deterministic=True), digest_size=16).hexdigest()
