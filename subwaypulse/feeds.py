"""The NYC subway GTFS-realtime feeds.

MTA splits the subway into one feed per group of lines. The feeds are public and,
at the time of writing, need no API key. The base URL is configurable so the same
collector can poll the local replay server in tests and demos.
"""

import os

MTA_BASE_URL = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds"

# feed id -> path under the base URL
SUBWAY_FEEDS: dict[str, str] = {
    "ace": "nyct%2Fgtfs-ace",
    "bdfm": "nyct%2Fgtfs-bdfm",
    "g": "nyct%2Fgtfs-g",
    "jz": "nyct%2Fgtfs-jz",
    "nqrw": "nyct%2Fgtfs-nqrw",
    "l": "nyct%2Fgtfs-l",
    "1234567": "nyct%2Fgtfs",
    "si": "nyct%2Fgtfs-si",
}


def feed_urls(base_url: str | None = None, only: str | None = None) -> dict[str, str]:
    """Return {feed_id: url}. `only` is a comma-separated list of feed ids."""
    base = (base_url or os.environ.get("FEED_BASE_URL") or MTA_BASE_URL).rstrip("/")
    wanted = {f.strip() for f in only.split(",")} if only else set(SUBWAY_FEEDS)
    unknown = wanted - set(SUBWAY_FEEDS)
    if unknown:
        raise ValueError(f"unknown feed ids: {sorted(unknown)}")
    return {fid: f"{base}/{path}" for fid, path in SUBWAY_FEEDS.items() if fid in wanted}
