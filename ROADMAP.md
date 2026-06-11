# Roadmap

Status snapshots and design details live in `MIGRATION_PLAN.md`,
`TOUCHPOINT_INVENTORY.md`, and `SQL_LINEAGE_PLAN.md`. This file tracks what's next.

## Hardening

- [x] CI: Scala suites + PySpark e2e on every push/PR; docker notebook validation on main
- [ ] Versioned release (`v4.0.0` tag, jar + fastutil as release artifacts)
- [ ] Maven Central / PyPI publishing
- [ ] Multi-machine cluster validation (everything is verified up to `local-cluster`)
- [ ] Cross-version test matrix before claiming Spark 4.2 support

## Capability

- [ ] **BigDebug primitives** on this foundation (ICSE '16): crash-culprit isolation
      (catch-in-tap + backward trace), guarded watchpoints (predicate taps with
      driver-pushed updates via the plugin framework), simulated breakpoints
- [ ] DataSource V2 scans (the gateway to Iceberg / Delta tables)
- [ ] Writes as result taps (`INSERT INTO`, `df.write`)
- [ ] Distributed trace joins for SQL (TraceCursor currently collects lineage tables
      to the driver; reuse the RDD machinery's selective join)
- [ ] At-scale benchmark (TPC-H subset; capture overhead + trace-latency curves)
- [ ] RDD capture-overhead optimization (+55% today; LAggregator per-merge allocation,
      toString hashing — SQL path proves ~10% is achievable)
- [ ] `PartitionPruningRDD` on RDD trace hops (restores the fork's empty-partition skip)
- [ ] RDD surfaces ported but unvalidated: sortByKey, replay, union/zip/coalesce taps
- [ ] Non-1:1 Python nodes (mapInPandas, UDTFs, grouped applies)
- [ ] Spark Connect (expose capture/trace as Connect commands)

## Adoption

- [x] End-to-end notebooks (SQL, PySpark, RDD) in a reproducible Docker image
- [ ] Hosted docs / tutorial
- [ ] Demo video / talk material
