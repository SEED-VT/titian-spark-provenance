# Titian on Spark 4 — Record-Level Data Provenance for Apache Spark

[![CI](https://github.com/SEED-VT/titian-spark-provenance/actions/workflows/ci.yml/badge.svg)](https://github.com/SEED-VT/titian-spark-provenance/actions/workflows/ci.yml)

**Titian** provides *record-level* data provenance for Apache Spark: run a job with
capture enabled, then interactively trace any output record **backward** to the exact
input records that produced it — through shuffles, aggregations, and joins — and
**forward** from any input record to everything it influenced.

This repository is the modern continuation of two 2016 research systems —
**Titian** (*Data Provenance Support in Spark*, [PVLDB 9(3)](https://www.vldb.org/pvldb/vol9/p216-interlandi.pdf))
and **BigDebug** (*Debugging Primitives for Interactive Big Data Processing in Spark*,
[ICSE 2016](https://dl.acm.org/doi/10.1145/2884781.2884813)). The original
implementation forked Spark 1.2/2.1; this is a ground-up migration of Titian to an
**attach-as-a-library** for stock **Apache Spark 4.1.x** (Scala 2.13, JDK 17+) — no
forked Spark, no patched jars — extended with native **Spark SQL / DataFrame**
provenance that did not exist in the original.

## 📖 Documentation

**[seed-vt.github.io/titian-spark-provenance](https://seed-vt.github.io/titian-spark-provenance/)**
— usage, command & flag reference, ablation/tuning, TPC-DS coverage, developer guide,
and the Scaladoc API. Build it locally with `mkdocs serve` (manual) and `sbt doc`
(API); see [`docs/`](docs/).

## Highlights

- **Library, not fork.** Capture integrates through sanctioned Spark 4 surfaces (RDD
  subclassing, task-completion listeners, `spark.sql.extensions`) plus a thin,
  documented internals layer. Jobs run on stock Spark.
- **Record-level provenance, the paper's design.** Compact `(inputId, outputId)`
  associations at stage boundaries, stitched lazily at trace time by recursive joins;
  lineage tables joined with `zipPartitions` — never reshuffled.
- **Spark SQL provenance via whole-stage codegen.** Tap operators implement
  `CodegenSupport`, so per-record capture is fused into Spark's generated Java.
- **Joins trace into both inputs**, including broadcast joins with **exact** probe-row
  provenance; outer/semi/anti joins, `DISTINCT`, windows, `UNION ALL`,
  `ORDER BY ... LIMIT` all handled. **[TPC-DS: 61/103 queries](https://seed-vt.github.io/titian-spark-provenance/tpcds/)**.
- **Fail-loud coverage policy.** Any operator outside the verified set aborts capture
  with a clear error. No silent wrong lineage.
- **Cluster-validated.** The suite includes `local-cluster` runs (separate executor
  JVMs: real serialization, classpath distribution, remote block fetches).

## Quick start

```scala
// spark.sql.extensions=org.apache.spark.sql.lineage.TitianSQLExtension
spark.conf.set("spark.titian.sql.capture", "true")

val df = spark.sql("SELECT category, SUM(amount) AS total FROM sales GROUP BY category")

import org.apache.spark.sql.lineage.TitianSQL
val output = TitianSQL.collectWithLineage(df)              // (Row, lineage id)
val cursor = TitianSQL.trace(df, Seq(output.head._2))
cursor.goBack().show().foreach(println)                    // witness source rows
TitianSQL.releaseLineage(df)
```

RDD and PySpark quick starts, and the full API, are in the
[documentation](https://seed-vt.github.io/titian-spark-provenance/usage/).

```bash
sbt test     # RDD suites, local-cluster suite, SQL suites, TPC-DS/oracle harness
```

## Development process

This codebase was developed through **AI-assisted development ("vibecoding")**: the
migration and the new SQL engine were implemented by
[Claude Code](https://claude.com/claude-code) (Anthropic) under the direction and
review of the maintainers, against the semantics defined by the original papers and
implementation, with the original test programs as the acceptance bar. AI-generated
commits are attributed with `Co-Authored-By` trailers.

## Citation

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
