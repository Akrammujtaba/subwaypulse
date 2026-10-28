# What it costs to run

SubwayPulse is sized to run on one small machine. This page shows what it actually used, and what that machine costs on AWS.

## Measured footprint

Measured with `docker stats` every minute for 77 minutes of a simulator run (A, C, E, G and L lines, 3 feeds polled every 15 seconds). Averages are across the run; memory is the peak.

| Service | CPU (avg, % of one core) | Memory (peak) |
|---|---|---|
| Kafka (KRaft, single broker) | 21.3% | 703 MiB |
| Flink TaskManager | 7.4% | 953 MiB |
| Trino | 4.6% | 4,038 MiB |
| SeaweedFS (S3) | 1.8% | 254 MiB |
| Simulator | 1.8% | 44 MiB |
| Flink JobManager | 1.7% | 614 MiB |
| Postgres | 1.5% | 67 MiB |
| Schema Registry | 1.3% | 264 MiB |
| Collector | 0.2% | 61 MiB |
| Grafana, Prometheus | 0.2% | 95 MiB |
| **Total** | **42% of one core** | **about 7 GiB** |

Two things stand out:

- **The streaming path is cheap.** Collector, Kafka, and Flink together average well under half a core.
- **Trino is most of the memory.** It reserves a 4 GB heap whether or not anyone is querying. In production it would be started on demand, or replaced with Athena.

These numbers come from 3 of the 8 subway feeds. The 29-hour live run covered all 8; its numbers are in [live-results.md](live-results.md).

## On AWS

`infra/terraform` provisions one `t4g.xlarge` (4 vCPU, 16 GiB) that runs the whole stack with Docker Compose, plus an S3 bucket for the lake and the raw archive.

| Item | Price (us-east-1) | Per month |
|---|---|---|
| t4g.xlarge, on demand, 24/7 | $0.1344 / hour | $98.11 |
| gp3 root volume, 40 GB | $0.08 / GB-month | $3.20 |
| S3 Standard storage | $0.023 / GB-month | depends on volume (see below) |
| S3 PUT requests | $0.005 / 1,000 | depends on commit rate (see below) |

Prices checked on 2026-10-23: EC2 from [aws-pricing.com](https://aws-pricing.com/t4g.xlarge.html) and [Holori](https://calculator.holori.com/aws/ec2/t4g.xlarge/us-east-1); S3 from [Upstash's 2026 S3 pricing summary](https://upstash.com/blog/aws-s3-pricing-explained-2026.md), which cites [AWS's S3 pricing page](https://aws.amazon.com/s3/pricing/). Check current prices before relying on them.

The Terraform also sets an AWS Budgets alert, so a misconfiguration shows up as an email rather than a bill.

## Where the savings come from

The design choices that keep the bill small, each measured by a counter rather than estimated:

| Choice | What it saves | How it's measured |
|---|---|---|
| Publish only trips that changed | Kafka, Flink, and Iceberg work on changes, not on every train every fetch | `sp_sent_event_bytes_total` vs. `sp_naive_event_bytes_total` |
| Drop fetches whose content didn't change | Unchanged fetches are never parsed or sent | `sp_feed_fetches_total{outcome="unchanged"}` |
| Archive raw feeds as compressed 5-minute segments | Far fewer S3 objects (PUTs) than one object per fetch | `sp_archive_objects`, `sp_archive_bytes_in` vs. `_out` |
| Hourly compaction and snapshot expiry | Small files from 30-second commits don't pile up | `ops.maintenance_log` (file counts before and after) |
| Raw archive to Glacier Instant Retrieval after 30 days, deleted after 365 | Old raw data costs a fraction of Standard | S3 lifecycle rule in `infra/terraform/main.tf` |

The live run reports all of these in one table (`docker compose run --rm live-report`).

## What the live run says about S3

From the 29.1-hour live run on all 8 feeds ([live-results.md](live-results.md)), for the raw archive only:

| | 29.1 hours | Per 30 days, at the same rate | Cost per month |
|---|---|---|---|
| Bytes written | 112 MB | about 2.8 GB added | about $0.06 for each month's data in S3 Standard |
| Objects (PUTs) | 2,592 | about 64,000 | about $0.32 |

One object per fetch would have been 56,400 PUTs in the same 29 hours (about 1.4 million a month, about $7). The Iceberg tables add their own writes on every 30-second checkpoint, which these counters don't include.
