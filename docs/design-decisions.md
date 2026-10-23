# Design decisions

Why SubwayPulse is built the way it is, written as the questions a reviewer or interviewer would ask.

---

### The feed never says "the train arrived". How do you get arrivals?

Each train's TripUpdate lists the stops it hasn't reached yet, with predicted times. When a stop drops off the front of that list, the train has served it. The best estimate of *when* is the last time predicted for that stop, which was made moments earlier while the train was at or next to the platform.

`ArrivalInference` compares consecutive updates for one trip (keyed state in Flink) and handles the cases that would otherwise create fake arrivals:

| Case | What happens |
|---|---|
| Stop disappears while still predicted 5+ minutes out | Reroute or skipped stop. Counted as `skippedStops`, not an arrival. |
| Stop disappears from the middle of the list | Not passed. Only stops before the train's new first stop count. |
| Train blinks out of the feed for one refresh | `VANISHED` records only stops already due. When it comes back, the stops already recorded are not emitted again. |
| Collector restarts and re-sends every trip | Each (trip, stop) is emitted once; the set of emitted stops is in keyed state. |
| An older update for a trip arrives after a newer one | Ignored and counted (`outOfOrderTripUpdates`). |

Trip state expires 45 minutes (event time) after a trip's last update, so it doesn't grow with every train that ever ran.

### Why send only changed trips instead of every trip on every fetch?

Each feed is republished every few seconds, and between two fetches most trains haven't moved and most predictions haven't changed. Sending all of them would make Kafka, Flink, and Iceberg reprocess the same rows over and over.

The collector fingerprints each trip (route, stops, predicted times, vehicle status; deliberately *not* the timestamps, which change every refresh) and publishes only trips whose fingerprint changed, plus a one-off `VANISHED` record when a trip leaves the feed. A fetch whose content didn't change at all is dropped before parsing.

It also counts what it *would* have sent without the diff (`sp_naive_event_bytes_total`) next to what it sent (`sp_sent_event_bytes_total`), so the saving is measured on every run instead of estimated once.

### If only changes are sent, how does event time keep moving?

Flink's watermark is per Kafka partition. A partition that gets no changed trips for a while would hold the watermark back for the whole job, and with it every timer: headway release and gap detection both depend on timers.

After every fetch, the collector sends a small `HEARTBEAT` record to **every** partition. Watermarks are assigned at the source from all records, heartbeats included, and heartbeats are dropped right after. (This was a known gap in ReconStream, where timeouts waited for the next real event.)

### Why Flink here and not Spark Structured Streaming?

The core logic is per-key state plus timers: "remember this train's last stop list", "hold this arrival until the watermark passes it", "fire if no train comes by time T". Flink's `KeyedProcessFunction` expresses exactly that, with event-time timers per key. In Spark that needs `flatMapGroupsWithState` and micro-batch timing, which would push gap detection latency up to the batch interval.

Gap detection in particular needs a timer per stop that fires *while nothing is happening*. That's the case micro-batches handle worst.

### How are out-of-order arrivals handled?

Arrivals at one stop come from different trains, which sit on different Kafka partitions (keyed by trip), so they reach `HeadwayMonitor` in no fixed order. Each arrival is buffered until the watermark passes its arrival time plus a release delay (default 60s), then released in arrival-time order.

- Longer delay: tolerates more disorder, detects later.
- An arrival that shows up after a *later* train at the same stop was already released is counted (`lateArrivals`) and not used, rather than producing a negative or misleading headway.

Both knobs (`OUT_OF_ORDER_SECONDS`, `RELEASE_DELAY_SECONDS`) are environment variables so the trade-off can be measured with the evaluator.

### How do you detect a gap that's still happening?

A gap is usually reported only when the late train finally shows up. By then riders have been waiting on the platform for ten minutes.

After each released arrival, `HeadwayMonitor` sets an event-time timer for when the next train would be overdue (scheduled headway plus max(5 min, one more headway)). If the timer fires before another train arrives, `GAP_OPENED` goes out immediately. The next arrival closes it with `GAP_CLOSED` and the real headway.

At the end of service there is no next train, so the timer would raise a false gap at every stop. The timer checks the schedule for the hour it fires in, and stays quiet if no service is scheduled then.

### Why is the schedule broadcast state instead of a lookup?

Thresholds depend on how often trains are *supposed* to come at that stop, at that hour, on that date. The schedule loader computes median scheduled headways from MTA's static GTFS and publishes them to a compacted Kafka topic. Flink reads it as broadcast state, so:

- the running job picks up a new schedule (the supplemented feed includes planned service changes) without a restart
- lookups are local memory reads, not a database call per arrival

Rows are keyed by route, stop, local date, and hour. The loader converts GTFS times (which can run past 24:00 for after-midnight trips) to real local clock times first, so a lookup never needs to know about service days. It also uses GTFS's "noon minus 12 hours" day start, which differs from midnight on daylight-saving change days.

### Exactly-once or at-least-once?

| Output | Guarantee | Why |
|---|---|---|
| Iceberg tables | Exactly-once | The Iceberg sink commits on checkpoint, together with Kafka offsets. A replay after a crash doesn't duplicate rows. |
| `subway.incidents` (Kafka alerts) | At-least-once | Exactly-once Kafka would hold every alert until the next checkpoint (30s). Each incident has a deterministic id (`route|stop|type|previous arrival`), so a consumer can drop repeats. |

### What does it cost to commit every 30 seconds?

Every checkpoint adds small Parquet files and a snapshot to each table, about 120 per table per hour. Left alone, reads slow down and old snapshots keep deleted files alive.

The hourly maintenance DAG compacts recent partitions, expires snapshots, and removes orphan files, then writes file counts before and after to `ops.maintenance_log`. Table properties also delete old `metadata.json` files after each commit.

The checkpoint interval is the main freshness vs. file-count knob. 30 seconds keeps the tables close to the stream; the maintenance job makes that affordable.

### Why a custom Avro deserializer in the Flink job?

Flink's Confluent Avro format pulls the Confluent client from Confluent's own Maven repository. The registry's wire format is small: a zero byte, a 4-byte schema id, then the Avro body. `RegistryAvroDeserializer` fetches the writer schema by id once, caches it, and decodes into the reader schema compiled into the job. With the registry set to `BACKWARD` compatibility, the collector can add an optional field without breaking a running job.

The Avro schema file lives in one place (`subwaypulse/schemas/trip_event.avsc`). The Python collector registers it, and the Java classes are generated from it at build time.

### How do you know the pipeline is right?

The live feeds can't tell you when a train *really* arrived, so they can't score the pipeline. The simulator can. It moves trains along the real static schedule with drift, injected holds, and a minimum separation between trains, so a held train causes a gap in front of it and bunching behind it, the way it happens on real lines. It serves feeds in MTA's format, adds noise (stale snapshots, HTTP 503s, trains that blink out for one refresh, imprecise platform arrival times), and writes the true arrival time of every train at every stop.

The evaluator recomputes true incidents from those arrivals with the same rules and the same schedule, then measures recall, precision, and detection latency. `tests/test_rules_parity.py` checks that the Java and Python rule implementations agree on a table of edge cases, so the pipeline isn't scored against different rules than it runs.

### Bugs worth remembering

**1. Deleting a timer deleted someone else's timer.** Flink keeps one timer per key and timestamp. When a new arrival came in, `HeadwayMonitor` deleted the old gap timer. When that timestamp happened to equal another arrival's release time, the release timer went with it, and that arrival was never processed. A unit test with three arrivals 4 minutes apart caught it (the gap threshold was exactly 9 minutes, landing on the third train's release time). The fix: never delete gap timers; let them fire and ignore them if they're no longer current.

**2. Every headway was UNSCHEDULED on the first real run.** Two causes stacked up. Pruning old dates from the broadcast schedule ran a full scan on *every* row, which made loading ~70,000 rows quadratic, so the schedule took minutes to arrive. Meanwhile trip events, read from the other Kafka source, raced ahead and were judged against an empty schedule. Pruning now runs only when a newer date shows up, and arrivals are held while the schedule state is still empty.

**3. Quality checks failed only in simulator runs.** The checks compared arrival time with the wall-clock time the job detected it. A simulator run replays a different day, so every row looked days late. The checks now compare two event times (arrival vs. the feed update that revealed it), which is the real invariant and holds in both modes.

**4. Four in ten gap alerts were false, and the evaluator found out why.** The first full simulator run caught every gap, but only 58.5% of gap alerts were real. Matching each false alert to the ground truth showed two causes:

- *Held trains.* A train held at a platform only counted as arrived once it left, up to 12 minutes later, so the station looked empty the whole time. The train reports `STOPPED_AT` that station in its VehiclePosition; arrivals now use that.
- *A race with the release buffer.* The gap timer fired while the next train's arrival was already in Flink but still waiting for its release. The timer now checks the waiting arrivals first.

Same window, same seed: real gap alerts went from 58.5% to 79.5%, and arrivals became available after 24 seconds (median) instead of 48.

**5. The top of the hour.** Many of the remaining false alerts were around 8:00. The gap timer judged a gap on the schedule in force when the previous train arrived (say every 7 minutes), but the headway status, and the path that opens and closes a gap at once, used the schedule at the next train's arrival (every 5 minutes). Both now use the previous train's. Real gap alerts: 79.5% to 87.9%.

**6. The evaluator had bugs too.** It only counted a gap if a later train eventually came, so a gap that never closed was scored as a false alarm. And it left incidents out of the truth when the first train passed before the feed started, but still counted the pipeline's alerts for them. Both fixed; the scoring rules are in `subwaypulse/evaluate.py`.

What's left after all this (run 3, 65 simulated minutes): 6 false gap alerts. Four have true headways within 30 seconds of the threshold, where ±15 seconds of platform-time noise decides it. Two come from missed arrivals at a line's last stop, which is on the roadmap.
