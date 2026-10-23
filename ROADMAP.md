# Roadmap

Planned improvements, roughly smallest first. Each one is a self-contained change with a test.

## Small

- [ ] **Station names in alerts.** Join `ref.stops` in a small Kafka Streams or Flink lookup so `subway.incidents` messages say "Bedford Av" instead of `L08N`.
- [ ] **Evaluator: CSV output** next to the markdown table, for plotting.
- [ ] **Grafana panel: incidents by route** using a route label on the Flink counters.
- [ ] **dbt test: arrivals per trip are in stop order.** A train shouldn't reach stop 10 before stop 9.
- [ ] **Schedule loader: skip publishing unchanged rows** by comparing with what's already in Postgres.

## Medium

- [ ] **Measure the release-delay trade-off.** Run the evaluator at `RELEASE_DELAY_SECONDS` = 15, 30, 60 and chart detection latency against late arrivals dropped.
- [ ] **Use VehiclePosition `STOPPED_AT` as a second arrival signal** and compare accuracy with stop-list inference in the simulator.
- [ ] **Replay tool for the raw archive.** Read a day of segments from S3 and serve them through the simulator's HTTP server at any speed, so a real day can be re-run through a new job version.
- [ ] **RocksDB state backend with incremental checkpoints**, and measure checkpoint size and duration against the hashmap backend.
- [ ] **Late arrivals: correct instead of drop.** Insert the late train, emit corrected headways for both neighbours, and retract an incident it invalidates.

## Larger

- [ ] **All 8 feed groups at once** and document where the bottleneck moves (collector, Kafka partitions, or Flink parallelism).
- [ ] **Deploy with `infra/terraform`** on one ARM instance with S3, and record a week of real cost from AWS Cost Explorer.
- [ ] **Bus feeds.** MTA Bus Time uses the same GTFS-realtime format, but needs an API key and has far more vehicles.
