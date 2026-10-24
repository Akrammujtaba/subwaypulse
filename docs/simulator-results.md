# Simulator results

Raw evaluator output for the runs quoted in the README. Each run replays the same simulated morning (2026-10-26, 07:00 to 09:30 NYC time, seed 7, lines A, C, E, G, L) in real time, with Docker Compose on a 4-core, 16 GB machine.

## Run 3: current code, full run

Simulated routes A, C, E, G, L, 587 trips, speed 1.0x. Scored window 2026-10-26 07:10 to 09:25 NYC time (135 simulated minutes).

| Metric | Value |
|---|---|
| Real arrivals found | 6324 / 6336 (99.8%) |
| Arrivals with no real counterpart | 0 |
| Arrival time error p50 / p95 | 9s / 29s |
| Arrival available after it happened p50 / p95 | 23s / 46s |
| Bunching detected (recall) | 411 / 508 (80.9%) |
| Bunching precision | 85.6% (69 false) |
| Bunching detection latency p50 / p95 | 120s / 149s |
| Gaps detected while open (recall) | 142 / 150 (94.7%) |
| Gap precision | 90.4% (15 false) |
| Gap detection latency p50 / p95 | 59s / 85s |

Thresholds: bunching at max(90s, 30% of scheduled headway), e.g. 90s on a 4-minute schedule; gap at scheduled + max(5 min, scheduled), e.g. 540s.
Latencies are in simulated seconds from when the problem became knowable.

## Same 65-minute window, before and after the fixes

Scored with the evaluator as of the second run. The last row adds the evaluator fix described in [design decisions](design-decisions.md) (bug 6).

| Run | Code | Real gap alerts | Gaps caught while open | Arrival known after it happened (p50) |
|---|---|---|---|---|
| 1 | before fixes | 58.5% (44 false) | 62 / 62 | 48s |
| 2 | held trains + release race fixed | 79.5% (15 false) | 58 / 62 | 24s |
| 3 | + top-of-hour fix | 87.9% (8 false) | 58 / 62 | 24s |
| 3 | same, corrected evaluator | 90.6% (6 false) | 58 / 62 | 24s |
