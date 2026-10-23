"""Render the simulation as GTFS-realtime FeedMessages, one per MTA feed group."""

from __future__ import annotations

from google.transit import gtfs_realtime_pb2

from subwaypulse.sim.model import Simulation

_STATUS = {
    "INCOMING_AT": gtfs_realtime_pb2.VehiclePosition.INCOMING_AT,
    "STOPPED_AT": gtfs_realtime_pb2.VehiclePosition.STOPPED_AT,
    "IN_TRANSIT_TO": gtfs_realtime_pb2.VehiclePosition.IN_TRANSIT_TO,
}


def build_feeds(sim: Simulation, now: float, drop: set[str] | None = None) -> dict[str, bytes]:
    """Return {feed_id: serialized FeedMessage} as of `now`.

    `drop` is a set of trip keys to leave out of this one snapshot, which imitates
    trains that blink out of the real feed for a refresh.
    """
    msgs: dict[str, gtfs_realtime_pb2.FeedMessage] = {}
    for t in sim.trips:
        if not sim.visible(t, now) or (drop and t.key in drop):
            continue
        msg = msgs.get(t.feed_id)
        if msg is None:
            msg = msgs[t.feed_id] = gtfs_realtime_pb2.FeedMessage()
            msg.header.gtfs_realtime_version = "1.0"
            msg.header.timestamp = int(now)
        preds = sim.predictions(t, now)
        if not preds:
            continue

        tu_ent = msg.entity.add(id=f"{len(msg.entity) + 1:06d}")
        tu = tu_ent.trip_update
        tu.trip.trip_id = t.rt_trip_id
        tu.trip.start_date = t.start_date
        tu.trip.route_id = t.route_id
        for stop_id, arr, dep in preds:
            stu = tu.stop_time_update.add(stop_id=stop_id)
            if arr is not None:
                stu.arrival.time = arr
            if dep is not None:
                stu.departure.time = dep

        status, stop_id = sim.vehicle(t, now)
        v_ent = msg.entity.add(id=f"{len(msg.entity) + 1:06d}")
        v = v_ent.vehicle
        v.trip.trip_id = t.rt_trip_id
        v.trip.start_date = t.start_date
        v.trip.route_id = t.route_id
        v.current_status = _STATUS[status]
        v.stop_id = stop_id
        v.timestamp = int(sim.last_moved(t, now))
    return {fid: m.SerializeToString() for fid, m in msgs.items()}
