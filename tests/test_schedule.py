from datetime import date

from conftest import make_gtfs

from subwaypulse.schedule import StaticGtfs, parse_gtfs_time, scheduled_headways, service_day_start


def test_headways_match_the_timetable(gtfs_bytes):
    rows = scheduled_headways(StaticGtfs(gtfs_bytes), date(2026, 10, 26), 1)
    by_key = {(r.route_id, r.stop_id, r.hour): r for r in rows}
    assert by_key[("T", "T05N", 8)].scheduled_headway_s == 300
    assert by_key[("U", "U05S", 8)].scheduled_headway_s == 600
    assert by_key[("T", "T05N", 8)].trains == 12


def test_trips_after_midnight_land_on_the_next_local_date(gtfs_bytes):
    # Service runs until 26:00, i.e. 2 AM the next morning.
    rows = scheduled_headways(StaticGtfs(gtfs_bytes), date(2026, 10, 27), 1)
    early = [r for r in rows if r.route_id == "T" and r.stop_id == "T01N" and r.hour == 1]
    assert early and early[0].scheduled_headway_s == 300


def test_service_calendar_is_respected():
    weekday_only = make_gtfs({"T": 300}, service_days="1111100")
    # Sunday: no Sunday service, and Saturday night has none to run past midnight either.
    assert scheduled_headways(StaticGtfs(weekday_only), date(2026, 10, 25), 1) == []
    assert scheduled_headways(StaticGtfs(weekday_only), date(2026, 10, 26), 1) != []


def test_gtfs_time_past_midnight():
    assert parse_gtfs_time("25:10:30") == 25 * 3600 + 10 * 60 + 30


def test_service_day_start_on_dst_change():
    # Nov 1 2026 is the end of daylight saving time in New York.
    start = service_day_start(date(2026, 11, 1))
    assert start.utcoffset().total_seconds() == -4 * 3600  # noon minus 12h is still EDT
