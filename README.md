# Titian on Spark 4 — Record-Level Data Provenance for Apache Spark

**Titian** provides *record-level* data provenance for Apache Spark: run a job with
capture enabled, then interactively trace any output record **backward** to the exact
input records that produced it — through shuffles, aggregations, and joins — and
**forward** from any input record to everything it influenced.

This repository is the modern continuation of the research systems published in 2016:

- **Titian** — *Titian: Data Provenance Support in Spark*, PVLDB 9(3), 2016
  ([paper](https://www.vldb.org/pvldb/vol9/p216-interlandi.pdf)), and its extended
  journal version in the VLDB Journal (2018).
- **BigDebug** — *BigDebug: Debugging Primitives for Interactive Big Data Processing
  in Spark*, ICSE 2016 ([paper](https://dl.acm.org/doi/10.1145/2884781.2884813)).

The original implementation was a fork of Spark 1.2/2.1
([maligulzar/bigdebug](https://github.com/maligulzar/bigdebug)). This repository is a
ground-up migration of Titian to an **attach-as-a-library** for stock **Apache Spark
4.1.x** (Scala 2.13, JDK 17+) — no forked Spark sources, no patched jars — extended
with native **Spark SQL / DataFrame** provenance that did not exist in the original.
The BigDebug interactive-debugging primitives (watchpoints, simulated breakpoints,
crash-culprit isolation) are planned follow-on work on the same foundation.

## Highlights

- **Library, not fork.** Capture integrates through sanctioned Spark 4 surfaces (RDD
  subclassing, task-completion listeners, `spark.sql.extensions`) plus a thin,
  documented internals layer. Jobs run on stock Spark.
- **Record-level provenance, the paper's design.** Compact `(inputId, outputId)`
  associations captured **only at stage boundaries**, stitched lazily at trace time by
  recursive joins; trace cursors are routed by the partition id packed into each record
  id, and lineage tables are joined with `zipPartitions` — never reshuffled (the
  distributed selective join of the VLDB paper, preserved and regression-tested).
- **Spark SQL provenance via whole-stage codegen.** Tap operators implement
  `CodegenSupport`, so per-record capture code is *fused into Spark's generated Java*.
  Measured capture overhead for a SQL `GROUP BY` aggregate: **under 10%**.
- **Joins trace into both inputs** (`goBack(path = i)`), including broadcast hash joins
  with **exact** probe-row provenance; outer/semi/anti joins handled; `DISTINCT`
  returns all duplicate witnesses (control-flow dependencies); window functions at
  peer-set granularity; explode and count-distinct chains thread exactly.
- **Fail-loud coverage policy.** Any operator outside the verified set aborts capture
  with a clear error. No silent wrong lineage.
- **Cluster-validated.** The test suite includes `local-cluster` runs (separate
  executor JVMs: real serialization, classpath distribution, remote lineage-block
  fetches).

## Quick start

Requirements: JDK 17+, sbt 1.10+, Spark 4.1.x (`spark-core`/`spark-sql` are
`provided`). The `local-cluster` tests additionally need a Spark 4.1.2 binary
distribution unpacked at `tools/spark-4.1.2-bin-hadoop3/` (see `build.sbt`).

```bash
sbt test          # 33 tests: RDD suites, local-cluster suite, SQL suites
```

### RDD API

```scala
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._

val lc = new LineageContext(sparkContext)
lc.setCaptureLineage(true)

val totals = lc.textFile("sales.txt", 4)
  .map { line => val p = line.split(","); (p(0), p(1).toInt) }
  .reduceByKey(_ + _)
val output = totals.collectWithId()

lc.setCaptureLineage(false)

// trace the suspicious aggregate back to its exact input lines
var lin = totals.getLineage()
lin = lin.filter(_ == suspiciousId)
lin = lin.goBack().goBack()
lin.show().collect().foreach(println)   // the culprit source lines
```

### SQL / DataFrames

```scala
// spark.sql.extensions=org.apache.spark.sql.lineage.TitianSQLExtension
spark.conf.set("spark.titian.sql.capture", "true")

val df = spark.sql(
  """SELECT c.name, SUM(o.amount) AS total
    |FROM orders o JOIN customers c ON o.cid = c.cid
    |GROUP BY c.name""".stripMargin)

import org.apache.spark.sql.lineage.TitianSQL
val output = TitianSQL.collectWithLineage(df)
val bobId  = output.find(_._1.getString(0) == "Bob").get._2

var cursor = TitianSQL.trace(df, Seq(bobId))
cursor = cursor.goBack()        // through the aggregation exchange
val orders    = cursor.goBack(0).show()   // culprit rows in orders
val customers = cursor.goBack(1).show()   // culprit rows in customers
```

### PySpark

```python
# spark-submit --jars titian.jar,fastutil.jar \
#   --conf spark.sql.extensions=org.apache.spark.sql.lineage.TitianSQLExtension \
#   --py-files titian.py your_app.py
from titian import Titian

t = Titian(spark)
t.enable_capture()

df = spark.sql("SELECT category, SUM(amount) AS total FROM sales GROUP BY category")
rows_with_ids = t.collect_with_lineage(df)

bad_id = next(i for r, i in rows_with_ids if r.total > 100000)
cursor = t.trace(df, [bad_id]).go_back()
print(cursor.show(full=True))     # witness source records, as dicts
t.release_lineage(df)             # drop this query's lineage blocks
```

Python UDFs are traced *exactly* (the eval nodes are 1:1; Titian re-threads record
ids across their batching). Cached DataFrames (`df.cache()`) act as source
boundaries: traces stop at, and `show()` re-reads, the cached rows. The PySpark
end-to-end test is `python/tests/run.sh`.

Runnable demos live in [`examples/`](examples/) (`SalesAnalysis`,
`OrderCustomerJoin`, `CaptureOverheadBenchmark`).

### Deploying on a cluster

```
spark-submit \
  --jars titian.jar,fastutil.jar \
  --conf spark.sql.extensions=org.apache.spark.sql.lineage.TitianSQLExtension \
  ...
```

Everything else is `provided` by Spark. Lineage blocks live in executor BlockManagers:
avoid dynamic allocation during a capture/trace session (see caveats).

## What's covered today

| | RDD API | SQL / DataFrames |
|---|---|---|
| map / filter / project pipelines | ✅ | ✅ |
| reduceByKey / aggregateByKey / combineByKey / GROUP BY (incl. expressions, HAVING, global aggregates) | ✅ | ✅ |
| groupByKey / DISTINCT (all duplicate witnesses) | ✅ | ✅ |
| join — both branches via `goBack(path)` | ✅ (cogroup) | ✅ SMJ / SHJ / **broadcast (exact probe rows)**; inner + outer + semi/anti |
| multi-hop (e.g. join → aggregate) backward & forward | ✅ | ✅ |
| explode / count-distinct chains | n/a | ✅ |
| window functions | n/a | ✅ (peer-set granularity) |
| Python UDFs (batch/Arrow eval) | n/a | ✅ exact (id re-threading across batching) |
| cached DataFrames (`df.cache()`) | n/a | ✅ as source boundaries |
| Parquet/ORC (columnar) scans | n/a | ✅ ids at the ColumnarToRow boundary |
| ROLLUP / CUBE / GROUPING SETS | n/a | ✅ visible-key granularity |
| AQE on/off, whole-stage codegen on/off | n/a | ✅ both modes tested |
| sortByKey, DSv2 scans, mapInPandas/UDTFs, writes | ported, unvalidated | ❌ fail loudly |

Trace granularity across shuffles is **key-hash granularity** (all records sharing the
key), exactly as in the paper; broadcast-join probe sides are exact.

## Performance (preliminary)

2M rows, `local[2]`, median of 3 (`CaptureOverheadBenchmark`) — small-scale numbers,
constants exaggerated; treat as directional until the at-scale benchmark lands:

| workload | capture off | capture on | overhead |
|---|---|---|---|
| SQL GROUP BY aggregate | 1.30 s | 1.43 s | **+9.8%** |
| SQL join + aggregate | 2.01 s | 2.80 s | +39% |
| RDD reduceByKey | 0.59 s | 0.92 s | +55% (optimization pass pending) |

## Documentation

- [`MIGRATION_PLAN.md`](MIGRATION_PLAN.md) — strategy, status, and the full **caveats
  and known limitations** (read before deploying).
- [`TOUCHPOINT_INVENTORY.md`](TOUCHPOINT_INVENTORY.md) — every file the original fork
  modified, mapped to its Spark 4 replacement mechanism.
- [`SQL_LINEAGE_PLAN.md`](SQL_LINEAGE_PLAN.md) — the SQL provenance design, phase
  status, and its twelve caveats.

## Development process

This codebase was developed through **AI-assisted development ("vibecoding")**: the
migration and the new SQL engine were implemented by [Claude Code](https://claude.com/claude-code)
(Anthropic) working under the direction and review of the maintainers, against the
semantics defined by the original papers and implementation — with the original test
programs as the acceptance bar, and all design decisions, caveats, and deviations
recorded in the documents above. AI-generated commits are attributed with
`Co-Authored-By` trailers.

## Citation

If you use this work, please cite the original papers:

```bibtex
@article{titian-pvldb16,
  author  = {Matteo Interlandi and Kshitij Shah and Sai Deep Tetali and
             Muhammad Ali Gulzar and Seunghyun Yoo and Miryung Kim and
             Todd Millstein and Tyson Condie},
  title   = {Titian: Data Provenance Support in Spark},
  journal = {Proc. VLDB Endow.},
  volume  = {9},
  number  = {3},
  pages   = {216--227},
  year    = {2015}
}

@inproceedings{bigdebug-icse16,
  author    = {Muhammad Ali Gulzar and Matteo Interlandi and Seunghyun Yoo and
               Sai Deep Tetali and Tyson Condie and Todd Millstein and Miryung Kim},
  title     = {BigDebug: Debugging Primitives for Interactive Big Data Processing
               in Spark},
  booktitle = {ICSE},
  year      = {2016}
}
```

## License

Apache License 2.0 — this project derives from the original Titian/BigDebug fork of
Apache Spark and retains its license and headers.
