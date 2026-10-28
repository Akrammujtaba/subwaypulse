# Live results

The full stack on all 8 of the MTA's subway feeds, on one laptop. The run went from Monday, Oct 26, 2026 at 6:30 PM to Tuesday, Oct 27 at 11:38 PM, New York time, so it covers Tuesday's morning and evening rush. The machine stayed awake the whole time.

Below is the output of `docker compose run --rm live-report --since 2026-10-26T22:30`, unedited. The live feeds can't say when a train really arrived, so these numbers show coverage, speed and cost. Accuracy is measured on the simulator ([simulator-results.md](simulator-results.md)).

---

# Live run

Arrivals from 2026-10-26 22:30 to 2026-10-28 03:38 UTC (29.1 hours).

| Metric | Value |
|---|---|
| Trains in the latest fetch | 364 |
| Arrivals inferred | 276,203 (10,885 trips, 1,026 stops) |
| Headways measured | 269,373 (98.7% with a schedule to compare to) |
| Bunching / gaps (by headway) | 6,264 / 9,638 |
| Gaps flagged while still open | 10,641 |
| Arrival available after it happened, p50 / p95 | 21s / 57s |
| Bunching flagged after the second train arrived, p50 / p95 | 114s / 122s |
| Gap flagged after the next train became overdue, p50 / p95 | 54s / 63s |
| Feed outages detected / gap alerts held back for them (since Flink started) | 14 / 404 |
| Feed age, median across feeds | 5s |
| Last Flink checkpoint | 0.3s |

**Cost side (collector counters since its last start)**

| Metric | Value |
|---|---|
| Feed fetches | 56,400 (0.6% had no new content, 0.0% failed or stale) |
| Feed bytes downloaded | 3,959.1 MB |
| Kafka event bytes sent vs. sending every trip every fetch | 691.23 MB vs. 2,198.64 MB (68.6% saved) |
| Raw archive: distinct snapshots vs. written to S3 | 3,949.1 MB vs. 112.0 MB in 2592 objects |

Generated 2026-10-28 03:38 UTC by `python -m subwaypulse.live_report`.

---

## Reading it

- **Coverage.** 276,203 arrivals at 1,026 stops in 29 hours. 98.7% of headways had a scheduled headway to compare against. The rest had no scheduled headway for that stop and hour in the static schedule, so they are kept but not judged.
- **Speed.** Half of all arrivals were in the tables 21 seconds after the train arrived, and 95% within 57 seconds. Gap alerts went out 54 seconds (median) after the next train became overdue.
- **Outages.** The pipeline saw 14 stretches where a feed went more than 2 minutes without a successful fetch, and held back 404 gap alerts that would have been raised across them. Without that, a feed outage reads as hundreds of missing trains (see bug 7 in [design-decisions.md](design-decisions.md)).
- **Change-only publishing.** On the live feeds almost every fetch has something new (only 0.6% were unchanged, against 48% in the simulator), so the saving comes from sending only the trips that changed: 691 MB to Kafka instead of 2,199 MB, 68.6% less.
- **Raw archive.** 3,949 MB of distinct snapshots went to S3 as 112 MB in 2,592 compressed objects: 97% fewer bytes, and 95% fewer objects than one per fetch (56,400).
- **Not shown:** whether each live gap or bunching alert was real. There is no ground truth on the live feeds. That is what the simulator is for.
