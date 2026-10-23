"""Keep only what changed between two fetches of the same feed.

Each feed is re-published every few seconds, but most trains have not moved and most
predictions are the same as last time. Sending every trip on every fetch would make
Kafka, Flink, and Iceberg do the same work over and over. The tracker remembers a
fingerprint per trip and lets through only trips whose fingerprint changed, plus a
one-off "vanished" marker when a trip drops out of the feed.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from subwaypulse.gtfsrt import TripSnapshot


@dataclass
class FeedDiff:
    changed: list[TripSnapshot] = field(default_factory=list)
    unchanged: int = 0
    vanished: list[TripSnapshot] = field(default_factory=list)


class ChangeTracker:
    """Per-feed memory of the last fingerprint seen for each trip.

    State lives in memory. After a restart every trip is sent once more, which is
    safe because the Flink job ignores a stop it has already recorded.
    """

    def __init__(self) -> None:
        self._last: dict[str, dict[str, tuple[str, TripSnapshot]]] = {}

    def diff(self, feed_id: str, snapshots: list[TripSnapshot]) -> FeedDiff:
        prev = self._last.get(feed_id, {})
        now: dict[str, tuple[str, TripSnapshot]] = {}
        out = FeedDiff()
        for snap in snapshots:
            fp = snap.fingerprint()
            now[snap.key] = (fp, snap)
            old = prev.get(snap.key)
            if old is not None and old[0] == fp:
                out.unchanged += 1
            else:
                out.changed.append(snap)
        for key, (_, last_snap) in prev.items():
            if key not in now:
                out.vanished.append(last_snap)
        self._last[feed_id] = now
        return out

    def tracked(self, feed_id: str) -> int:
        return len(self._last.get(feed_id, {}))
