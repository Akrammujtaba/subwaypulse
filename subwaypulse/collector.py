"""Poll the subway feeds, keep only what changed, and publish it to Kafka.

    python -m subwaypulse.collector

Per fetch:
1. GET the feed (with If-None-Match when the server gave an ETag).
2. Skip it entirely if its content is the same as last time (only the header
   timestamp moved). Otherwise archive the raw bytes.
3. Diff trips against the previous fetch and publish only changed and vanished trips
   as Avro TripEvents, keyed by trip so one train always lands on one partition.
4. Send a HEARTBEAT to every partition so Flink's event time keeps moving even on a
   partition that got no changes.

Everything counted here (bytes that would have been sent without the diff, bytes
actually sent) is exported to Prometheus, which is where the cost numbers come from.
"""

from __future__ import annotations

import io
import logging
import os
import signal
import time
from dataclasses import dataclass

import fastavro
import requests
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from subwaypulse.archive import RawArchive
from subwaypulse.changes import ChangeTracker
from subwaypulse.events import heartbeat, load_schema, trip_event
from subwaypulse.feeds import feed_urls
from subwaypulse.gtfsrt import content_hash, parse_feed

log = logging.getLogger("collector")

FETCHES = Counter("sp_feed_fetches_total", "Feed fetches", ["feed", "outcome"])
FETCH_SECONDS = Histogram("sp_feed_fetch_seconds", "Feed fetch time", ["feed"])
FEED_AGE = Gauge("sp_feed_age_seconds", "Now minus feed header timestamp", ["feed"])
FEED_BYTES = Counter("sp_feed_bytes_total", "Raw feed bytes downloaded", ["feed"])
TRIPS_SEEN = Counter("sp_trips_seen_total", "Trip snapshots parsed", ["feed"])
TRIPS_SENT = Counter("sp_trip_events_sent_total", "Trip events published", ["feed", "type"])
TRIPS_ACTIVE = Gauge("sp_trips_active", "Trips in the latest fetch", ["feed"])
NAIVE_BYTES = Counter("sp_naive_event_bytes_total", "Avro bytes if every trip were sent on every fetch", ["feed"])
SENT_BYTES = Counter("sp_sent_event_bytes_total", "Avro bytes actually sent (payload only)", ["feed"])
ARCHIVE_IN = Gauge("sp_archive_bytes_in", "Raw bytes handed to the archive since start")
ARCHIVE_OUT = Gauge("sp_archive_bytes_out", "Compressed bytes written to S3 since start")
ARCHIVE_OBJECTS = Gauge("sp_archive_objects", "Objects written to S3 since start")


@dataclass
class FeedState:
    etag: str | None = None
    last_hash: str | None = None
    last_feed_ts: int = 0


class Collector:
    def __init__(self, urls: dict[str, str], publish, archive: RawArchive | None, schema: dict,
                 session: requests.Session | None = None, timeout: float = 10.0) -> None:
        """`publish(key, value_dict, partition=None)` sends one event."""
        self.urls = urls
        self.publish = publish
        self.archive = archive
        self.tracker = ChangeTracker()
        self.state = {fid: FeedState() for fid in urls}
        self.http = session or requests.Session()
        self.timeout = timeout
        self._parsed_schema = fastavro.parse_schema(schema)

    def _avro_size(self, record: dict) -> int:
        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, self._parsed_schema, record)
        return buf.tell()

    def poll_feed(self, feed_id: str, now_ms: int | None = None) -> str:
        """Fetch and process one feed. Returns an outcome label (also a metric)."""
        st = self.state[feed_id]
        headers = {"If-None-Match": st.etag} if st.etag else {}
        t0 = time.monotonic()
        try:
            resp = self.http.get(self.urls[feed_id], headers=headers, timeout=self.timeout)
        except requests.RequestException as e:
            log.warning("fetch %s failed: %s", feed_id, e)
            return self._count(feed_id, "error")
        finally:
            FETCH_SECONDS.labels(feed_id).observe(time.monotonic() - t0)

        if resp.status_code == 304:
            return self._count(feed_id, "not_modified")
        if resp.status_code != 200:
            log.warning("fetch %s returned HTTP %s", feed_id, resp.status_code)
            return self._count(feed_id, f"http_{resp.status_code}")

        raw = resp.content
        st.etag = resp.headers.get("ETag")
        FEED_BYTES.labels(feed_id).inc(len(raw))
        now_ms = now_ms if now_ms is not None else int(time.time() * 1000)

        try:
            feed_ts, snaps = parse_feed(raw, feed_id)
            h = content_hash(raw)
        except Exception as e:  # a corrupt or truncated body; never crash the loop
            log.warning("parse %s failed: %s", feed_id, e)
            return self._count(feed_id, "parse_error")

        FEED_AGE.labels(feed_id).set(max(0.0, now_ms / 1000 - feed_ts))
        if feed_ts < st.last_feed_ts:
            # A stale cache node served an older copy. Using it would move trains backwards.
            return self._count(feed_id, "stale")
        if h == st.last_hash:
            st.last_feed_ts = feed_ts
            self._heartbeat(feed_id, feed_ts, now_ms)
            return self._count(feed_id, "unchanged")

        st.last_hash = h
        st.last_feed_ts = feed_ts
        if self.archive is not None:
            self.archive.add(feed_id, now_ms, raw)
            ARCHIVE_IN.set(self.archive.bytes_in)
            ARCHIVE_OUT.set(self.archive.bytes_out)
            ARCHIVE_OBJECTS.set(self.archive.objects)

        TRIPS_SEEN.labels(feed_id).inc(len(snaps))
        TRIPS_ACTIVE.labels(feed_id).set(len(snaps))
        diff = self.tracker.diff(feed_id, snaps)

        changed_keys = {s.key for s in diff.changed}
        for snap in snaps:
            rec = trip_event(snap, "UPDATE", now_ms)
            size = self._avro_size(rec)
            NAIVE_BYTES.labels(feed_id).inc(size)
            if snap.key in changed_keys:
                self.publish(snap.key, rec)
                SENT_BYTES.labels(feed_id).inc(size)
                TRIPS_SENT.labels(feed_id, "UPDATE").inc()
        for snap in diff.vanished:
            rec = trip_event(snap, "VANISHED", now_ms, feed_ts=feed_ts)
            self.publish(snap.key, rec)
            SENT_BYTES.labels(feed_id).inc(self._avro_size(rec))
            TRIPS_SENT.labels(feed_id, "VANISHED").inc()

        self._heartbeat(feed_id, feed_ts, now_ms)
        return self._count(feed_id, "changed")

    def _heartbeat(self, feed_id: str, feed_ts: int, now_ms: int) -> None:
        self.publish(None, heartbeat(feed_id, feed_ts, now_ms), broadcast=True)

    @staticmethod
    def _count(feed_id: str, outcome: str) -> str:
        FETCHES.labels(feed_id, outcome).inc()
        return outcome


def _kafka_publisher(bootstrap: str, registry_url: str, topic: str, schema: dict):
    import json

    from confluent_kafka import SerializingProducer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from confluent_kafka.serialization import StringSerializer

    registry = SchemaRegistryClient({"url": registry_url})
    producer = SerializingProducer({
        "bootstrap.servers": bootstrap,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": AvroSerializer(registry, json.dumps(schema)),
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "zstd",
        "linger.ms": 50,
    })
    partitions = sorted(producer.list_topics(topic, timeout=30).topics[topic].partitions)

    def publish(key, value, broadcast=False):
        if broadcast:
            for p in partitions:
                producer.produce(topic, key=None, value=value, partition=p)
        else:
            producer.produce(topic, key=key, value=value)
        producer.poll(0)

    return publish, producer


def _s3_put(bucket: str):
    import boto3
    from botocore.config import Config

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT") or None,
        config=Config(s3={"addressing_style": "path"}),
    )

    def put(key: str, data: bytes) -> None:
        s3.put_object(Bucket=bucket, Key=key, Body=data)

    return put


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    urls = feed_urls(only=os.environ.get("FEEDS") or None)
    poll_seconds = float(os.environ.get("POLL_SECONDS", "15"))
    schema = load_schema()

    publish, producer = _kafka_publisher(
        os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092"),
        os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8085"),
        os.environ.get("TRIP_TOPIC", "subway.trip_events"),
        schema,
    )
    archive = None
    if os.environ.get("ARCHIVE_BUCKET"):
        archive = RawArchive(_s3_put(os.environ["ARCHIVE_BUCKET"]),
                             flush_seconds=int(os.environ.get("ARCHIVE_FLUSH_SECONDS", "300")))

    start_http_server(int(os.environ.get("METRICS_PORT", "8000")))
    collector = Collector(urls, publish, archive, schema)
    log.info("polling %d feeds every %ss: %s", len(urls), poll_seconds, ", ".join(urls))

    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    while not stopping:
        cycle_start = time.monotonic()
        for feed_id in urls:
            collector.poll_feed(feed_id)
        producer.flush(10)
        time.sleep(max(0.0, poll_seconds - (time.monotonic() - cycle_start)))

    if archive is not None:
        archive.flush()
    producer.flush(30)
    log.info("stopped")


if __name__ == "__main__":
    main()
