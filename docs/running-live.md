# Running it on the live feeds

This is how the live numbers in the README were produced, and how to reproduce them.

**Needs:** Docker Desktop with about **8 GB of memory** assigned (Windows: WSL 2 backend), and an internet connection. MTA's subway feeds are public and need no API key.

```bash
git clone https://github.com/Akrammujtaba/subwaypulse.git
cd subwaypulse
docker compose up -d --build        # first build takes ~10 minutes (Maven + images)
docker compose ps                   # wait until everything is Up / Exited (0)
```

Then leave it running. A few hours covers a rush period; a full weekday is better.

| What | Where |
|---|---|
| Grafana dashboard | http://localhost:3000 |
| Flink UI | http://localhost:8082 |
| Airflow | http://localhost:8081 (admin / admin) |
| Trino | http://localhost:8080 |

Useful checks while it runs:

```bash
docker compose logs -f collector          # one line per problem; quiet is good
docker compose exec trino trino --execute \
  "select route_id, status, count(*) from lake.silver.headways group by 1, 2 order by 1, 2"
```

When you're done watching:

```bash
docker compose run --rm live-report       # writes evaluation/results-live.md
docker compose down                       # keeps the data; add -v to wipe it
```

`evaluation/results-live.md` is the source for every live number in the README. The raw feed archive stays in your local object store and is not committed: MTA's terms allow hosting the data, but the repo only carries code.

## Laptop sleep

If the laptop sleeps, the collector stops polling and the feeds' timestamps jump when it wakes. The pipeline copes (the watermark just moves forward), but the hours spent asleep have no data. For a clean run, keep the machine awake.
