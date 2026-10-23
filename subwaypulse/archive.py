"""Raw feed archive on S3, written in segments.

Every distinct feed snapshot is kept so any day can be replayed through the pipeline
later. Two choices keep this cheap:

- A fetch whose content matches the previous one (only the header timestamp moved)
  is not stored again.
- Snapshots are buffered per feed and written as one zstd-compressed segment every
  few minutes, instead of one tiny S3 object per fetch. S3 charges per PUT request, so
  ~8 feeds x 4 fetches a minute as single objects would be ~46,000 PUTs a day.

Segment format: repeated [8-byte big-endian fetch time ms][4-byte length][protobuf].
"""

from __future__ import annotations

import io
import struct
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime

import zstandard

_HEADER = struct.Struct(">qI")


@dataclass
class _Segment:
    started: float
    first_ts: int
    buf: io.BytesIO = field(default_factory=io.BytesIO)
    count: int = 0


class RawArchive:
    def __init__(self, put, prefix: str = "raw", flush_seconds: int = 300, level: int = 10) -> None:
        """`put(key, data)` writes one object; injected so tests need no S3."""
        self._put = put
        self._prefix = prefix.rstrip("/")
        self._flush_seconds = flush_seconds
        self._cctx = zstandard.ZstdCompressor(level=level)
        self._segments: dict[str, _Segment] = {}
        self.bytes_in = 0
        self.bytes_out = 0
        self.objects = 0

    def add(self, feed_id: str, fetched_at_ms: int, raw: bytes, now: float | None = None) -> None:
        now = time.time() if now is None else now
        seg = self._segments.get(feed_id)
        if seg is None:
            seg = self._segments[feed_id] = _Segment(started=now, first_ts=fetched_at_ms)
        seg.buf.write(_HEADER.pack(fetched_at_ms, len(raw)))
        seg.buf.write(raw)
        seg.count += 1
        self.bytes_in += len(raw)
        if now - seg.started >= self._flush_seconds:
            self.flush(feed_id)

    def flush(self, feed_id: str | None = None) -> None:
        for fid in [feed_id] if feed_id else list(self._segments):
            seg = self._segments.pop(fid, None)
            if seg is None or seg.count == 0:
                continue
            data = self._cctx.compress(seg.buf.getvalue())
            t = datetime.fromtimestamp(seg.first_ts / 1000, tz=UTC)
            key = (
                f"{self._prefix}/feed={fid}/date={t:%Y-%m-%d}/hour={t:%H}/"
                f"{seg.first_ts}-{seg.count}.seg.zst"
            )
            self._put(key, data)
            self.bytes_out += len(data)
            self.objects += 1


def read_segment(data: bytes) -> Iterator[tuple[int, bytes]]:
    """Yield (fetched_at_ms, raw protobuf) from a compressed segment."""
    raw = zstandard.ZstdDecompressor().decompressobj().decompress(data)
    pos = 0
    while pos < len(raw):
        ts, n = _HEADER.unpack_from(raw, pos)
        pos += _HEADER.size
        yield ts, raw[pos : pos + n]
        pos += n
