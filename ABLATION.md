# Performance tuning flags & ablation study

Titian's five capture/trace optimizations each sit behind a runtime flag. **By default
all are on** (the most optimized configuration); the flags let you turn any one *off*,
which is used both for the ablation study below and as an escape hatch if a legacy code
path is ever needed for debugging or compatibility.

These are first-class, supported configs on `main` — not a fork or a branch.

## The flags

`spark.titian.ablation` is a comma-separated list (a cluster conf — set it at
submit/session-build time). Each entry turns ONE optimization off, restoring its
pre-optimization implementation. An empty/absent value (the default) = everything on.

| flag | optimization it disables | legacy behavior restored |
|---|---|---|
| `legacyHash` | P1a | murmur3 over `key.toString` bytes (per-record String alloc) |
| `boxedCombiner` | P1b | a fresh `Tuple2` per combine instead of a mutable carrier |
| `boxedJoinBuffer` | P2 | `ArrayBuffer[Any]` of nested boxed tuples in the join tap |
| `boxedBlocks` | P3 | one boxed `(id, value)` tuple per row in lineage blocks |
| `driverTrace` | P4 | collect whole blocks to the driver and filter there; no locality |

`spark.titian.lineage.storageLevel` (default `MEMORY_AND_DISK_SER`) is the related
storage knob — set it to `MEMORY_AND_DISK` for the deserialized block format.

```bash
# default: fully optimized, nothing to set
spark-submit --conf spark.sql.extensions=org.apache.spark.sql.lineage.TitianSQLExtension ...

# disable one optimization (e.g. to measure P3, or to fall back)
spark-submit --conf spark.titian.ablation=boxedBlocks ...
```

## Ablation benchmark

```bash
bin/sbt 'examples/runMain edu.vt.bigdebug.examples.AblationBenchmark [rows] [iters]'
```

Runs every configuration (all-on, each-one-off leave-one-out, and fully-legacy) over
an RDD `reduceByKey` and a SQL join+aggregate+ORDER BY/LIMIT, with a fresh
`SparkContext` per configuration (the flags are read once per `SparkEnv`). Reports
median wall time over interleaved off/on pairs, capture-side JVM GC time, single-row
trace latency, and lineage footprint; writes `tpcds/ablation.csv`.

**Run on quiet, dedicated hardware.** On a laptop the capture wall-time deltas
(5–15%) are swamped by run-to-run noise — see the main README's performance caveat.
The deterministic signals (footprint via `lineageSize`, GC time, trace latency) are
the trustworthy ones at small scale; the wall-time overheads need a cluster and the
at-scale TPC-DS data (`tpcds/gen_data.py 10`) to be defensible. Randomize the
configuration order across repetitions.

## Semantic preservation

Every flag is performance-only: each legacy path must produce **byte-identical
lineage** to the optimized one. This is proved by running the full Scala suite (which
asserts exact witness sets and exact trace outputs) under each flag forced on:

```bash
sh scripts/verify_ablation.sh
```

Spark auto-loads `spark.*` JVM system properties into `SparkConf`, so the script
injects `-Dspark.titian.ablation=<flags>` into every forked test session. A green
suite under a flag means that legacy path is semantically indistinguishable from the
optimized one. Results in [`ABLATION_RESULTS.md`](ABLATION_RESULTS.md).
