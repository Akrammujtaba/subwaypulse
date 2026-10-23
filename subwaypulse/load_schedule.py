"""Load the static schedule: download it, compute scheduled headways, publish them.

    python -m subwaypulse.load_schedule

Writes to two places:
- Kafka `schedule.headways` (compacted, keyed by route|stop|date|hour). The Flink job
  reads it as broadcast state, so a refreshed schedule reaches the running job
  without a restart.
- Postgres `ref.*` tables (headways, stops, routes), which Trino and dbt join against.

Airflow runs this every morning for today and the next day. It's also safe to run by
hand: Kafka keeps the latest value per key and the Postgres load replaces each date.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

from subwaypulse.schedule import NY, HeadwayRow, StaticGtfs, scheduled_headways, stops_and_routes

log = logging.getLogger("load_schedule")

SUPPLEMENTED_URL = "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_supplemented.zip"


def fetch_gtfs(url: str, cache: Path) -> bytes:
    """Download the zip, keeping a local copy. Falls back to the copy if MTA is down."""
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_bytes(resp.content)
        return resp.content
    except requests.RequestException as e:
        if cache.exists():
            log.warning("download failed (%s); using cached %s", e, cache)
            return cache.read_bytes()
        raise


def publish_kafka(rows: list[HeadwayRow], bootstrap: str, topic: str) -> None:
    from confluent_kafka import Producer

    p = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True, "linger.ms": 100,
                  "compression.type": "zstd"})
    for r in rows:
        value = {"route_id": r.route_id, "stop_id": r.stop_id, "local_date": r.local_date,
                 "hour": r.hour, "scheduled_headway_s": r.scheduled_headway_s, "trains": r.trains}
        p.produce(topic, key=r.key, value=json.dumps(value))
        p.poll(0)
    remaining = p.flush(60)
    if remaining:
        raise RuntimeError(f"{remaining} schedule messages not delivered")


def load_postgres(rows: list[HeadwayRow], stops: list[dict], routes: list[dict], dsn: str) -> None:
    import psycopg

    dates = sorted({r.local_date for r in rows})
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS ref;
            CREATE TABLE IF NOT EXISTS ref.scheduled_headways (
                route_id text NOT NULL, stop_id text NOT NULL, local_date date NOT NULL,
                hour smallint NOT NULL, scheduled_headway_s integer NOT NULL, trains integer NOT NULL,
                loaded_at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (route_id, stop_id, local_date, hour));
            CREATE TABLE IF NOT EXISTS ref.stops (
                stop_id text PRIMARY KEY, stop_name text, lat double precision, lon double precision,
                parent_station text);
            CREATE TABLE IF NOT EXISTS ref.routes (
                route_id text PRIMARY KEY, short_name text, long_name text, color text);
        """)
        cur.execute("DELETE FROM ref.scheduled_headways WHERE local_date = ANY(%s::date[])", (dates,))
        with cur.copy("COPY ref.scheduled_headways (route_id, stop_id, local_date, hour, "
                      "scheduled_headway_s, trains) FROM STDIN") as copy:
            for r in rows:
                copy.write_row((r.route_id, r.stop_id, r.local_date, r.hour, r.scheduled_headway_s, r.trains))
        cur.executemany(
            "INSERT INTO ref.stops VALUES (%(stop_id)s, %(stop_name)s, %(lat)s, %(lon)s, %(parent_station)s) "
            "ON CONFLICT (stop_id) DO UPDATE SET stop_name = EXCLUDED.stop_name, lat = EXCLUDED.lat, "
            "lon = EXCLUDED.lon, parent_station = EXCLUDED.parent_station", stops)
        cur.executemany(
            "INSERT INTO ref.routes VALUES (%(route_id)s, %(short_name)s, %(long_name)s, %(color)s) "
            "ON CONFLICT (route_id) DO UPDATE SET short_name = EXCLUDED.short_name, "
            "long_name = EXCLUDED.long_name, color = EXCLUDED.color", routes)
        conn.commit()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    url = os.environ.get("GTFS_URL", SUPPLEMENTED_URL)
    cache = Path(os.environ.get("GTFS_CACHE", "/data/gtfs.zip"))
    start_env = os.environ.get("SCHEDULE_START_DATE", "today")
    first = datetime.now(tz=NY).date() if start_env == "today" else date.fromisoformat(start_env)
    days = int(os.environ.get("SCHEDULE_DAYS", "2"))

    t0 = time.time()
    gtfs = StaticGtfs(fetch_gtfs(url, cache))
    rows = scheduled_headways(gtfs, first, days)
    stops, routes = stops_and_routes(gtfs)
    log.info("%d headway rows for %s .. %s in %.1fs", len(rows), first, first + timedelta(days=days - 1),
             time.time() - t0)

    publish_kafka(rows, os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092"),
                  os.environ.get("SCHEDULE_TOPIC", "schedule.headways"))
    if os.environ.get("REF_DSN"):
        load_postgres(rows, stops, routes, os.environ["REF_DSN"])
    log.info("done in %.1fs", time.time() - t0)


if __name__ == "__main__":
    main()
