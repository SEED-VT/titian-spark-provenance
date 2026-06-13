# Titian on Spark 4 — Record-Level Data Provenance for Apache Spark

[![CI](https://github.com/SEED-VT/titian-spark-provenance/actions/workflows/ci.yml/badge.svg)](https://github.com/SEED-VT/titian-spark-provenance/actions/workflows/ci.yml)

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
  `CodegenSupport`, so per-record capture code is *fused into Spark's generated Java*;
  lineage is held as compact primitive-array blocks in the executors' block managers.
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

### Notebooks (reproducible via Docker)

Three end-to-end notebooks — [SQL](notebooks/titian_sql.ipynb),
[PySpark + Python UDFs](notebooks/titian_pyspark.ipynb), and the
[RDD API](notebooks/titian_rdd.ipynb) — each with assertions, packaged in a
self-contained image (Spark 4.1.2, JDK 17, the Titian jar built from source):

```bash
git clone https://github.com/SEED-VT/titian-spark-provenance.git
cd titian-spark-provenance
docker build -t titian -f docker/Dockerfile .
docker run --rm -p 8888:8888 titian        # JupyterLab at http://localhost:8888
docker run --rm titian validate            # execute all notebooks headlessly
```

Without Docker (JDK 17+, sbt, Python >= 3.9):

```bash
sbt package                                # builds target/scala-2.13/titian_*.jar
pip install pyspark==4.1.2 jupyterlab
cd notebooks && jupyter lab
```

The bootstrap cell self-locates the jar, fastutil (coursier cache), the data, and
spark-shell (pip pyspark); override with `TITIAN_HOME` / `TITIAN_JAR` /
`FASTUTIL_JAR` / `SPARK_HOME` if your layout differs. Make sure `JAVA_HOME` points
at JDK 17+.

CI runs the full Scala suites, the PySpark end-to-end test, and the Docker notebook
validation (see `.github/workflows/ci.yml`).

### Deploying on a cluster

```
spark-submit \
  --jars titian.jar,fastutil.jar \
  --conf spark.sql.extensions=org.apache.spark.sql.lineage.TitianSQLExtension \
  ...
```

Everything else is `provided` by Spark. Lineage blocks live in executor BlockManagers:
avoid dynamic allocation during a capture/trace session (see
[`INSTALL.md`](INSTALL.md)).

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
| `UNION ALL` — branches via `goBack(path)` | ported, unvalidated | ✅ exact (no tap needed; partition-routed) |
| `ORDER BY ... LIMIT` (TakeOrderedAndProject) | n/a | ✅ sort-key granularity (exact when keys unique) |
| AQE on/off, whole-stage codegen on/off | n/a | ✅ both modes tested |
| sortByKey, DSv2 scans, mapInPandas/UDTFs, writes | ported, unvalidated | ❌ fail loudly |

Trace granularity across shuffles is **key-hash granularity** (all records sharing the
key), exactly as in the paper; broadcast-join probe sides are exact. Bare `LIMIT` /
`df.show()` (positional selection, no sort key) still skips capture.

### TPC-DS coverage

A coverage harness runs every TPC-DS v1.4 query (the 103 files Apache Spark ships in
its own test suite) against Titian: capture off vs on must produce identical answers,
then one result row is traced backward to source rows. See
[`tpcds/`](tpcds/) and the scoreboard in `TPCDSCoverage`:

```bash
pip install duckdb && python3 tpcds/gen_data.py 0.2   # parquet via DuckDB's dsdgen
sbt 'examples/runMain edu.vt.bigdebug.examples.TPCDSCoverage'
```

Current scoreboard (sf=0.2, Spark 4.1.2, AQE on):

| outcome | queries |
|---|---|
| **PASS** — identical answers + 1-row backward trace to source rows | **61 / 103** |
| UNSUPPORTED — capture aborts loudly on an out-of-scope operator | 41 |
| ERROR — q90 only: `DIVIDE_BY_ZERO` in the *baseline* at this tiny scale factor | 1 |

A re-execution **oracle** (`--oracle`) further verifies ground truth per passing
query: one result row is traced through every branch to all sources, each traced
table is replaced by only its witness rows, and the query is re-run — the traced
row must reproduce identically. Result: **45 / 45 oracle-checkable queries pass
recall, 0 failures** (16 skipped: scalar subqueries or empty results at sf0.2).
The oracle caught three silent wrong-lineage bugs on first deployment (fused
aggregate pairs, aligned unions, fan-out broadcast joins) — all fixed; details in
[`tpcds/`](tpcds/).

The unsupported buckets, by blocking operator: shuffle/broadcast capture fed by an
untapped shuffle (34 — Titian's soundness guard refuses to record stale row ids;
needs mid-pipeline re-keying — plus a few range-partitioned global sorts),
`WindowGroupLimitExec` (3, `rank() <= n` pushdown), existence-join BHJ (3),
inline-computed sort keys (2), global sort without LIMIT (1). All of these fail
loudly — never silent wrong lineage.

## Performance (preliminary)

2M rows, `local[2]`, interleaved off/on pairs, median of 7
(`CaptureOverheadBenchmark`) — small-scale numbers, constants exaggerated; treat as
directional until the at-scale benchmark lands:

| metric | value |
|---|---|
| RDD reduceByKey capture overhead | ~10% |
| SQL GROUP BY aggregate capture overhead | ~25–45% (laptop noise band) |
| SQL join + aggregate capture overhead | ~45–60% (laptop noise band) |
| backward trace, 1 result row (join + agg) | ~1.0 s |
| lineage block footprint (join + agg) | 22.9 MB |

The 2026-06 optimization pass (allocation-free capture, compact primitive-array
lineage blocks, executor-side trace filtering with block-locality scheduling) cut the
lineage footprint **4.3×** (99.2 → 22.9 MB) and trace latency **~1.5×** on this
workload; capture wall-time overheads on a laptop are dominated by run-to-run noise.
Lineage block storage is tunable with `spark.titian.lineage.storageLevel`
(default `MEMORY_AND_DISK_SER`); inspect a query's footprint with
`TitianSQL.lineageSize(df)`.

Each optimization can be toggled off individually with `spark.titian.ablation` (all
on by default) — for ablation studies or as a fallback. Every legacy path is verified
to produce byte-identical lineage (full suite green under each flag). See
[`ABLATION.md`](ABLATION.md).

## Documentation

- **API reference (Scaladoc)** — `sbt doc` → `target/scala-2.13/api/index.html`. Full
  code/usage/optimization reference: usage overview, per-package guides, the public
  trace API grouped by task, and the capture-engine internals. CI uploads it as the
  `scaladoc-api` artifact on every run.
- [`INSTALL.md`](INSTALL.md) — build, test, cluster deployment, and notebook setup.
- [`ABLATION.md`](ABLATION.md) — the `spark.titian.ablation` tuning flags and study.

### Known limitations (short version)

- Trace granularity across shuffles is key-hash level (all records sharing the key);
  broadcast-join probe sides are exact.
- `show()` resolves traces by deterministic re-scan — valid while source files are
  unchanged (file sources only; DSv2/Iceberg/Delta not yet supported).
- Lineage lives in executor BlockManagers: avoid dynamic allocation during a
  capture/trace session; release per-query lineage with `releaseLineage`.
- Unsupported operators (e.g. `mapInPandas`, UDTFs, non-equi joins, writes) abort
  capture loudly; display/`LIMIT` queries skip capture.
- Validated on Spark 4.1.x; other versions untested.

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
