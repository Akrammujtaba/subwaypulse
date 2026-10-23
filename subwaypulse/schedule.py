"""Scheduled headways from the static GTFS schedule.

To say a gap is "too long" we need to know how often trains are supposed to come.
For every route, stop, local date, and hour we take the scheduled arrivals, sort
them, and keep the median time between consecutive trains.

Times in GTFS can run past 24:00 (a 25:10:00 arrival belongs to the previous service
day), so every scheduled time is turned into an absolute local datetime first and
bucketed by its real clock hour. That way a lookup by an observed arrival time never
has to know about service days.

MTA publishes two static files: the regular schedule, and a "supplemented" one that
adds planned service changes for the next seven days and is refreshed hourly. Either
works here; the Airflow DAG uses the supplemented one.
"""

from __future__ import annotations

import csv
import io
import statistics
import zipfile
from collections import defaultdict
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
WEEKDAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]


@dataclass(frozen=True)
class HeadwayRow:
    route_id: str
    stop_id: str
    local_date: str  # YYYY-MM-DD
    hour: int  # 0-23, local clock
    scheduled_headway_s: int
    trains: int  # scheduled arrivals in this hour

    @property
    def key(self) -> str:
        return f"{self.route_id}|{self.stop_id}|{self.local_date}|{self.hour}"


class StaticGtfs:
    """Reads the handful of GTFS files we need from a zip (path or bytes)."""

    def __init__(self, source) -> None:
        self._zip = zipfile.ZipFile(io.BytesIO(source) if isinstance(source, bytes) else source)

    def rows(self, name: str) -> Iterator[dict]:
        with self._zip.open(name) as f:
            yield from csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))

    def has(self, name: str) -> bool:
        return name in self._zip.namelist()

    def active_services(self, day: date) -> set[str]:
        ymd = day.strftime("%Y%m%d")
        active = set()
        if self.has("calendar.txt"):
            for r in self.rows("calendar.txt"):
                if r["start_date"] <= ymd <= r["end_date"] and r[WEEKDAYS[day.weekday()]] == "1":
                    active.add(r["service_id"])
        if self.has("calendar_dates.txt"):
            for r in self.rows("calendar_dates.txt"):
                if r["date"] == ymd:
                    if r["exception_type"] == "1":
                        active.add(r["service_id"])
                    elif r["exception_type"] == "2":
                        active.discard(r["service_id"])
        return active


def parse_gtfs_time(t: str) -> int:
    """'25:10:30' -> seconds after the service day's noon-minus-12h."""
    h, m, s = t.split(":")
    return int(h) * 3600 + int(m) * 60 + int(s)


def service_day_start(day: date) -> datetime:
    """GTFS measures times from 'noon minus 12 hours', which equals local midnight
    except on daylight-saving change days, where it is an hour off. Using it makes
    times correct on those days too."""
    noon = datetime(day.year, day.month, day.day, 12, tzinfo=NY)
    return noon - timedelta(hours=12)


def scheduled_arrivals(gtfs: StaticGtfs, days: Iterable[date], routes: set[str] | None = None):
    """Yield (route_id, stop_id, trip_id, absolute local datetime, service_date) for every
    scheduled stop event on the given service days."""
    days = list(days)
    services_by_day = {d: gtfs.active_services(d) for d in days}
    all_services = set().union(*services_by_day.values()) if days else set()

    trip_info: dict[str, tuple[str, str]] = {}
    for r in gtfs.rows("trips.txt"):
        if r["service_id"] in all_services and (routes is None or r["route_id"] in routes):
            trip_info[r["trip_id"]] = (r["route_id"], r["service_id"])

    for r in gtfs.rows("stop_times.txt"):
        info = trip_info.get(r["trip_id"])
        if info is None:
            continue
        route_id, service_id = info
        secs = parse_gtfs_time(r["arrival_time"] or r["departure_time"])
        for d in days:
            if service_id in services_by_day[d]:
                yield route_id, r["stop_id"], r["trip_id"], service_day_start(d) + timedelta(seconds=secs), d


def scheduled_headways(gtfs: StaticGtfs, first_day: date, n_days: int,
                       routes: set[str] | None = None) -> list[HeadwayRow]:
    """Median scheduled headway per (route, stop, local date, hour) for local dates
    first_day .. first_day + n_days - 1.

    Service days start one day earlier, because trips from the previous evening run
    past midnight into the first local date.
    """
    service_days = [first_day + timedelta(days=i) for i in range(-1, n_days)]
    wanted_dates = {first_day + timedelta(days=i) for i in range(n_days)}

    times: dict[tuple[str, str], list[datetime]] = defaultdict(list)
    for route_id, stop_id, _trip, ts, _sd in scheduled_arrivals(gtfs, service_days, routes):
        times[(route_id, stop_id)].append(ts)

    gaps: dict[tuple[str, str, str, int], list[float]] = defaultdict(list)
    for (route_id, stop_id), ts_list in times.items():
        ts_list.sort()
        for prev, cur in zip(ts_list, ts_list[1:], strict=False):
            local = cur.astimezone(NY)
            if local.date() not in wanted_dates:
                continue
            gap = (cur - prev).total_seconds()
            if gap <= 0:
                continue  # two trips timetabled at the same second; not a real headway
            gaps[(route_id, stop_id, local.date().isoformat(), local.hour)].append(gap)

    return [
        HeadwayRow(route_id, stop_id, d, hour, int(statistics.median(g)), len(g))
        for (route_id, stop_id, d, hour), g in sorted(gaps.items())
    ]


def stops_and_routes(gtfs: StaticGtfs) -> tuple[list[dict], list[dict]]:
    stops = [
        {"stop_id": r["stop_id"], "stop_name": r["stop_name"], "lat": float(r["stop_lat"]),
         "lon": float(r["stop_lon"]), "parent_station": r.get("parent_station") or None}
        for r in gtfs.rows("stops.txt")
    ]
    routes = [
        {"route_id": r["route_id"], "short_name": r["route_short_name"],
         "long_name": r["route_long_name"], "color": r.get("route_color") or None}
        for r in gtfs.rows("routes.txt")
    ]
    return stops, routes
