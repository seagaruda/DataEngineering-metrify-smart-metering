# PostgreSQL + TimescaleDB on Huawei Cloud vs Oracle for AMI Projects

## Overview

Advanced Metering Infrastructure (AMI) is fundamentally a time-series problem. Millions of smart meters report readings at 15-minute or hourly intervals, 24 hours a day. The database at the center of an AMI platform must handle continuous high-volume inserts, fast time-range queries, long-term data retention, and growing device counts — all without planned downtime.

Traditional AMI deployments have relied on Oracle in a primary/standby configuration. This document makes the case that **Huawei Cloud RDS for PostgreSQL with TimescaleDB** is a technically superior and dramatically more cost-effective alternative for modern AMI platforms such as Gurux.DLMS.AMI.

---

## Huawei Cloud RDS for PostgreSQL: TimescaleDB Built In

Huawei Cloud RDS for PostgreSQL (versions 12 through 15) ships with TimescaleDB as a built-in plugin. No separate installation, no custom builds, no OS-level access required. Enabling it is a single SQL statement run against your target database:

```sql
CREATE EXTENSION IF NOT EXISTS timescaledb;
```

Huawei Cloud handles the `shared_preload_libraries` configuration automatically. From that point, you have the full TimescaleDB feature set available inside a fully managed, cloud-native PostgreSQL instance — with automated backups, high availability, and point-in-time recovery included.

Official documentation: [Huawei Cloud RDS for PostgreSQL Plugin Overview](https://support.huaweicloud.com/intl/en-us/usermanual-rds/rds_09_0043.html)

---

## Why TimescaleDB Is the Right Engine for AMI Data

Meter reading data has a well-defined shape: a device identifier, a timestamp, and one or more measured values. This pattern repeats billions of times across the life of an AMI deployment. TimescaleDB is purpose-built for exactly this workload.

### Hypertables: Automatic Time Partitioning

A TimescaleDB hypertable automatically partitions data into time-based chunks (configurable — e.g., one chunk per week). When a query asks for the last 30 days of readings for a specific meter, the query planner touches only the relevant chunks and skips everything else. On a table with years of history, this is the difference between a millisecond response and a full table scan.

```sql
-- Create the meter readings table as a hypertable
CREATE TABLE meter_readings (
    meter_id    TEXT        NOT NULL,
    ts          TIMESTAMPTZ NOT NULL,
    active_kwh  DOUBLE PRECISION,
    reactive_kvarh DOUBLE PRECISION
);

SELECT create_hypertable('meter_readings', 'ts');
```

### Native Compression: 90–97% Size Reduction

Meter data is highly repetitive — the same meters reporting similar values at regular intervals. TimescaleDB's columnar compression achieves 90–97% size reduction on this kind of data. A dataset that would occupy 1 TB uncompressed can fit in 30–100 GB. For an AMI deployment growing to millions of endpoints, this directly controls storage costs.

```sql
-- Enable compression on chunks older than 7 days
ALTER TABLE meter_readings SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'meter_id'
);

SELECT add_compression_policy('meter_readings', INTERVAL '7 days');
```

### Continuous Aggregates: Pre-Computed Rollups

AMI billing and demand analytics require hourly, daily, and monthly aggregations. With continuous aggregates, TimescaleDB pre-computes these rollups incrementally as new data arrives. Billing queries that would otherwise scan months of raw readings return in milliseconds.

```sql
-- Pre-compute hourly totals, updated automatically
CREATE MATERIALIZED VIEW hourly_consumption
WITH (timescaledb.continuous) AS
SELECT
    meter_id,
    time_bucket('1 hour', ts) AS hour,
    SUM(active_kwh) AS total_kwh
FROM meter_readings
GROUP BY meter_id, hour;

SELECT add_continuous_aggregate_policy('hourly_consumption',
    start_offset => INTERVAL '3 hours',
    end_offset   => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
```

### Automated Data Retention

Utilities typically retain raw 15-minute readings for 2–3 years and aggregated data for longer. TimescaleDB's retention policies drop old raw chunks automatically while preserving aggregates — no manual partition management, no DBA intervention.

```sql
SELECT add_retention_policy('meter_readings', INTERVAL '2 years');
```

### `time_bucket()`: Native Interval Aggregation

The `time_bucket()` function makes interval-based queries natural SQL:

```sql
-- 15-minute demand intervals for a specific meter, last 24 hours
SELECT
    time_bucket('15 minutes', ts) AS interval,
    SUM(active_kwh) AS demand_kwh
FROM meter_readings
WHERE meter_id = 'METER_001'
  AND ts > NOW() - INTERVAL '24 hours'
GROUP BY interval
ORDER BY interval;
```

---

## PostgreSQL Cloud Database vs Oracle Primary/Standby

### Scalability: Horizontal vs Vertical

Oracle primary/standby is a **single-writer architecture**. The standby exists for disaster recovery and, with Active Data Guard (an additional paid option), for read offload. All writes go to one node. When write throughput exceeds what that node can handle, the only option is to scale up — bigger CPU, more RAM, faster storage — with a maintenance window.

PostgreSQL on Huawei Cloud scales differently:

- **Read replicas** distribute query load across multiple nodes with no additional licensing cost.
- **Huawei GaussDB** (distributed PostgreSQL) shards data across nodes for write-scale-out, handling petabyte-scale AMI deployments.
- **PgBouncer** connection pooling handles thousands of concurrent AMI head-end connections efficiently.
- New nodes are added online. No downtime. No maintenance window.

For an AMI deployment that starts with 100,000 meters and grows to 5 million, this difference is decisive. PostgreSQL grows with the deployment. Oracle requires a hardware upgrade cycle.

### Cost: The Oracle Licensing Problem

Oracle Enterprise Edition list price is approximately **$47,500 per processor core** (2024). A modest AMI database server with two 16-core CPUs carries a license cost of roughly **$760,000** before annual support fees (typically 22% of license value per year, ~$167,000/year).

Features that are standard in PostgreSQL and TimescaleDB require separate paid options in Oracle:

| Feature | PostgreSQL + TimescaleDB | Oracle |
|---|---|---|
| Time-series partitioning | Built into TimescaleDB (free) | Oracle Partitioning option: ~$11,500/core |
| Columnar compression | Built into TimescaleDB (free) | Oracle Advanced Compression: ~$11,500/core |
| Read replicas for analytics | Streaming replication (free) | Active Data Guard: additional license |
| Multi-node write scaling | Citus / GaussDB (cloud pricing) | Oracle RAC: ~$23,000/core additional |
| License audit risk | None | Significant — Oracle audits are common |

A 5-year TCO comparison for a mid-scale AMI deployment (32 cores, HA, analytics read replica) typically shows **60–80% cost savings** with PostgreSQL over Oracle, based on published EDB and industry analyses.

### Operational Complexity

Oracle requires certified DBAs (Oracle Certified Professional). These are expensive, increasingly scarce, and tied to a single vendor ecosystem. PostgreSQL expertise is widely available, well-documented, and supported by a large open-source community.

Huawei Cloud RDS for PostgreSQL is a fully managed service: automated patching, backups, failover, and monitoring are handled by the platform. The operational burden on the AMI team is minimal compared to running Oracle on-premise or on IaaS.

### Time-Series Capability Gap

Oracle has no native equivalent to:
- TimescaleDB hypertables (automatic time partitioning with chunk exclusion)
- Continuous aggregates (incremental pre-computation)
- `time_bucket()` (native interval aggregation)
- Automatic compression policies for time-ordered data
- Automatic data retention with aggregate preservation

Implementing equivalent functionality in Oracle requires custom partitioning schemes, materialized view refresh jobs, and manual maintenance — all of which require DBA time and introduce operational risk.

---

## Migration Path: Oracle to PostgreSQL

For AMI projects currently running on Oracle, migration tooling is mature:

- **ora2pg** (open source): Converts Oracle schema, data, and PL/SQL stored procedures to PostgreSQL format. Handles the majority of AMI schema migrations automatically.
- **EDB Migration Toolkit**: Commercial tool with Oracle compatibility layer for complex migrations.
- **Huawei Cloud DRS** (Data Replication Service): Managed online migration from Oracle to RDS PostgreSQL with minimal downtime.

The migration sequence for an AMI platform:

1. Export schema with ora2pg, review and adjust
2. Convert stored procedures and application queries
3. Load historical data into PostgreSQL hypertables
4. Run parallel validation (both databases receiving writes, outputs compared)
5. Cut over with a short maintenance window
6. Enable TimescaleDB compression and continuous aggregates post-migration

---

## Summary

| Dimension | Huawei Cloud RDS PostgreSQL + TimescaleDB | Oracle Primary/Standby |
|---|---|---|
| **TimescaleDB support** | Built-in, enable with one SQL command | Not available |
| **Time-series partitioning** | Automatic hypertables | Manual, paid option |
| **Compression** | 90–97% native, automatic | Paid option, less effective on time-series |
| **Continuous aggregates** | Built-in, incremental | Manual materialized views |
| **Horizontal scaling** | Read replicas + GaussDB sharding | Standby is DR only; RAC is expensive |
| **License cost** | Open source; cloud service fees only | ~$47,500/core + 22%/year support |
| **Audit risk** | None | Significant |
| **Managed service** | Fully managed on Huawei Cloud | Requires DBA team |
| **Growing meter fleet** | Add nodes online, no downtime | Scale up hardware, maintenance window |

For a platform like Gurux.DLMS.AMI — where device counts grow continuously, data volumes compound over years, and 24/7 availability is non-negotiable — Huawei Cloud RDS for PostgreSQL with TimescaleDB is the correct database foundation. It handles the time-series workload natively, scales horizontally without downtime, and eliminates the licensing cost and operational complexity that Oracle imposes.

---

*References:*
- *Huawei Cloud RDS for PostgreSQL Plugin Documentation: support.huaweicloud.com/intl/en-us/usermanual-rds/rds_09_0043.html*
- *TimescaleDB Documentation: docs.timescale.com*
- *TimescaleDB Compression: docs.timescale.com/use-timescale/latest/compression/*
- *EDB PostgreSQL vs Oracle TCO Analysis: enterprisedb.com/blog/postgresql-vs-oracle-cost-comparison*
- *ora2pg Migration Tool: ora2pg.darold.net*
- *Huawei GaussDB Distributed Database: huaweicloud.com/intl/en-us/product/gaussdb.html*
