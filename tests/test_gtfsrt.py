from google.transit import gtfs_realtime_pb2

from subwaypulse.changes import ChangeTracker
from subwaypulse.gtfsrt import content_hash, direction_of, parse_feed


def feed(ts: int, trains: list[tuple[str, str, list[tuple[str, int]]]], status_stop: str | None = None,
         moved_at: int = 900) -> bytes:
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "1.0"
    msg.header.timestamp = ts
    for trip_id, route, stops in trains:
        e = msg.entity.add(id=f"tu-{trip_id}")
        e.trip_update.trip.trip_id = trip_id
        e.trip_update.trip.route_id = route
        e.trip_update.trip.start_date = "20261026"
        for sid, t in stops:
            stu = e.trip_update.stop_time_update.add(stop_id=sid)
            stu.arrival.time = t
        v = msg.entity.add(id=f"v-{trip_id}").vehicle
        v.trip.trip_id = trip_id
        v.trip.start_date = "20261026"
        v.current_status = gtfs_realtime_pb2.VehiclePosition.IN_TRANSIT_TO
        v.stop_id = status_stop or stops[0][0]
        v.timestamp = moved_at
    return msg.SerializeToString()


def test_trip_update_and_vehicle_merge_into_one_snapshot():
    ts, snaps = parse_feed(feed(1000, [("000600_A..N54R", "A", [("A02N", 1060), ("A03N", 1180)])]), "ace")
    assert ts == 1000 and len(snaps) == 1
    s = snaps[0]
    assert (s.route_id, s.direction, s.vehicle_status, s.vehicle_stop_id) == ("A", "N", "IN_TRANSIT_TO", "A02N")
    assert [x.stop_id for x in s.stops] == ["A02N", "A03N"]
    assert s.key == "20261026:000600_A..N54R"


def test_direction_falls_back_to_stop_suffix():
    from subwaypulse.gtfsrt import StopTime

    assert direction_of("000600_GS.S01R", []) == "S"
    assert direction_of("weird", [StopTime("901S", 1, None)]) == "S"
    assert direction_of("weird", []) is None


def test_content_hash_ignores_header_timestamp():
    a = feed(1000, [("t1", "A", [("A02N", 1060)])])
    b = feed(1030, [("t1", "A", [("A02N", 1060)])])
    c = feed(1030, [("t1", "A", [("A02N", 1090)])])
    assert a != b
    assert content_hash(a) == content_hash(b)
    assert content_hash(b) != content_hash(c)


def test_change_tracker_sends_only_changed_and_vanished_trips():
    tr = ChangeTracker()
    _, s1 = parse_feed(feed(1000, [("t1", "A", [("A02N", 1060)]), ("t2", "A", [("A05N", 1300)])]), "ace")
    d1 = tr.diff("ace", s1)
    assert len(d1.changed) == 2

    # Next refresh: t1 unchanged apart from timestamps, t2 got a new prediction
    _, s2 = parse_feed(feed(1030, [("t1", "A", [("A02N", 1060)]), ("t2", "A", [("A05N", 1320)])]), "ace")
    d2 = tr.diff("ace", s2)
    assert [s.trip_id for s in d2.changed] == ["t2"] and d2.unchanged == 1

    # t1 finished its run
    _, s3 = parse_feed(feed(1060, [("t2", "A", [("A05N", 1320)])]), "ace")
    d3 = tr.diff("ace", s3)
    assert d3.changed == [] and [s.trip_id for s in d3.vanished] == ["t1"]
