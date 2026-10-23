"""Shared fixtures. Everything here is generated: no MTA data is checked in."""

from __future__ import annotations

import io
import zipfile

import pytest


def make_gtfs(routes: dict[str, int] | None = None, n_stops: int = 12, first: str = "05:00:00",
              last: str = "26:00:00", run_s: int = 120, service_days: str = "1111111") -> bytes:
    """A small static GTFS zip: each route runs both directions along its own line of
    stops, with one train every `routes[route]` seconds between `first` and `last`."""
    routes = routes or {"T": 300}

    def secs(t: str) -> int:
        h, m, s = t.split(":")
        return int(h) * 3600 + int(m) * 60 + int(s)

    def fmt(s: int) -> str:
        return f"{s // 3600:02d}:{s % 3600 // 60:02d}:{s % 60:02d}"

    days = ",".join(service_days)
    files = {
        "agency.txt": "agency_id,agency_name,agency_url,agency_timezone\nX,Test,http://example.com,America/New_York\n",
        "calendar.txt": "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
                        f"Daily,{days},20260101,20271231\n",
        "calendar_dates.txt": "service_id,date,exception_type\n",
    }
    stops = ["stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station"]
    route_rows = ["route_id,agency_id,route_short_name,route_long_name,route_type,route_color"]
    trips = ["route_id,trip_id,service_id,trip_headsign,direction_id,shape_id"]
    stop_times = ["trip_id,stop_id,arrival_time,departure_time,stop_sequence"]
    for route, headway in routes.items():
        route_rows.append(f"{route},X,{route},{route} Line,1,000000")
        ids = [f"{route}{i:02d}" for i in range(1, n_stops + 1)]
        for sid in ids:
            stops.append(f"{sid},Stop {sid},40.7,-73.9,1,")
            for d in "NS":
                stops.append(f"{sid}{d},Stop {sid},40.7,-73.9,,{sid}")
        t = secs(first)
        while t <= secs(last):
            for d, seq in (("N", ids), ("S", ids[::-1])):
                tid = f"TEST-Daily-00_{t // 60 * 100:06d}_{route}..{d}01R"
                trips.append(f"{route},{tid},Daily,End,{0 if d == 'N' else 1},{route}..{d}01R")
                for k, sid in enumerate(seq):
                    at = fmt(t + k * run_s)
                    stop_times.append(f"{tid},{sid}{d},{at},{at},{k + 1}")
            t += headway
    files["stops.txt"] = "\n".join(stops) + "\n"
    files["routes.txt"] = "\n".join(route_rows) + "\n"
    files["trips.txt"] = "\n".join(trips) + "\n"
    files["stop_times.txt"] = "\n".join(stop_times) + "\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, body in files.items():
            z.writestr(name, body)
    return buf.getvalue()


@pytest.fixture
def gtfs_bytes() -> bytes:
    return make_gtfs({"T": 300, "U": 600})
