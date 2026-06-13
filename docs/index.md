# Titian on Spark 4

**Record-level data provenance for Apache Spark.** Run a job with capture enabled,
then interactively trace any output record **backward** to the exact input records
that produced it — through shuffles, aggregations, and joins — and **forward** from
any input record to everything it influenced.

This repository is the modern continuation of two 2016 research systems:

- **Titian** — *Titian: Data Provenance Support in Spark*, PVLDB 9(3), 2016
  ([paper](https://www.vldb.org/pvldb/vol9/p216-interlandi.pdf)).
- **BigDebug** — *BigDebug: Debugging Primitives for Interactive Big Data Processing
  in Spark*, ICSE 2016 ([paper](https://dl.acm.org/doi/10.1145/2884781.2884813)).

The original implementation forked Spark 1.2/2.1. This is a ground-up migration of
Titian to an **attach-as-a-library** for stock **Apache Spark 4.1.x** (Scala 2.13,
JDK 17+) — no forked Spark, no patched jars — extended with native **Spark SQL /
DataFrame** provenance that did not exist in the original.

## Where to go

<div class="grid cards" markdown>

- :material-rocket-launch: **[Usage](usage.md)** — SQL, RDD, and PySpark quick starts.
- :material-bug-check: **[BigSift](bigsift.md)** — automated isolation of the minimal
  fault-inducing input records (provenance + delta debugging).
- :material-cog: **[Getting started](install.md)** — build, test, deploy, notebooks.
- :material-book-open-variant: **[Command & flag reference](reference.md)** — every
  task, script, config flag, and env var.
- :material-tune: **[Ablation & performance](ablation.md)** — the tuning flags and the
  optimization study.
- :material-table-check: **[TPC-DS coverage](tpcds.md)** — the coverage harness and
  re-execution oracle.
- :material-hammer-wrench: **[Developer guide](developer-guide.md)** — architecture
  and how to extend capture.
- :material-api: **[API reference (Scaladoc)](api/index.html)** — class- and
  method-level docs.

</div>

## Highlights

- **Library, not fork.** Capture integrates through sanctioned Spark 4 surfaces (RDD
  subclassing, task-completion listeners, `spark.sql.extensions`) plus a thin,
  documented internals layer. Jobs run on stock Spark.
- **Record-level provenance, the paper's design.** Compact `(inputId, outputId)`
  associations captured **only at stage boundaries**, stitched lazily at trace time by
  recursive joins; trace cursors are routed by the partition id packed into each
  record id, and lineage tables are joined with `zipPartitions` — never reshuffled.
- **Spark SQL provenance via whole-stage codegen.** Tap operators implement
  `CodegenSupport`, so per-record capture is *fused into Spark's generated Java*;
  lineage is held as compact primitive-array blocks in the executors' block managers.
- **Joins trace into both inputs** (`goBack(path = i)`), including broadcast hash joins
  with **exact** probe-row provenance; outer/semi/anti joins handled; `DISTINCT`
  returns all duplicate witnesses; window functions at peer-set granularity.
- **Fail-loud coverage policy.** Any operator outside the verified set aborts capture
  with a clear error. No silent wrong lineage.
- **Cluster-validated.** The test suite includes `local-cluster` runs (separate
  executor JVMs: real serialization, classpath distribution, remote block fetches).

## What's covered today

| | RDD API | SQL / DataFrames |
|---|---|---|
| map / filter / project pipelines | ✅ | ✅ |
| reduceByKey / aggregateByKey / GROUP BY (expressions, HAVING, global) | ✅ | ✅ |
| groupByKey / DISTINCT (all duplicate witnesses) | ✅ | ✅ |
| join — both branches via `goBack(path)` | ✅ (cogroup) | ✅ SMJ / SHJ / **broadcast (exact probe rows)**; inner + outer + semi/anti |
| multi-hop (join → aggregate) backward & forward | ✅ | ✅ |
| explode / count-distinct chains | n/a | ✅ |
| window functions | n/a | ✅ (peer-set granularity) |
| Python UDFs (batch/Arrow eval) | n/a | ✅ exact (id re-threading across batching) |
| cached DataFrames (`df.cache()`) | n/a | ✅ as source boundaries |
| Parquet/ORC (columnar) scans | n/a | ✅ ids at the ColumnarToRow boundary |
| ROLLUP / CUBE / GROUPING SETS | n/a | ✅ visible-key granularity |
| `UNION ALL` — branches via `goBack(path)` | ported, unvalidated | ✅ exact (partition-routed) |
| `ORDER BY ... LIMIT` | n/a | ✅ sort-key granularity (exact when keys unique) |
| AQE on/off, whole-stage codegen on/off | n/a | ✅ both modes tested |
| sortByKey, DSv2 scans, mapInPandas/UDTFs, writes | ported, unvalidated | ❌ fail loudly |

Trace granularity across shuffles is **key-hash level** (all records sharing the key),
exactly as in the paper; broadcast-join probe sides are exact. **[TPC-DS coverage:
61/103 queries](tpcds.md)**, with a re-execution oracle confirming recall.

## Performance

2M rows, `local[2]`, interleaved off/on, median of 7 (`CaptureOverheadBenchmark`) —
small-scale, directional until the at-scale benchmark lands:

| metric | value |
|---|---|
| RDD reduceByKey capture overhead | ~10% |
| SQL aggregate / join capture overhead | ~25–60% (laptop noise band) |
| backward trace, 1 result row (join + agg) | ~1.0 s |
| lineage block footprint (join + agg) | 22.9 MB |

The optimization pass cut the lineage footprint **4.3×** and trace latency **~1.5×**.
Each optimization is individually switchable for study or fallback — see
**[Ablation & performance](ablation.md)**.

## Development process

This codebase was developed through **AI-assisted development ("vibecoding")**: the
migration and the new SQL engine were implemented by
[Claude Code](https://claude.com/claude-code) under the direction and review of the
maintainers, against the semantics of the original papers and implementation, with the
original test programs as the acceptance bar. AI-generated commits carry
`Co-Authored-By` trailers.
