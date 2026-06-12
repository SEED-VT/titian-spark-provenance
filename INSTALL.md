# Installing and Running Titian

## Requirements

- JDK **17+** (Spark 4 requirement)
- sbt 1.10+
- Apache Spark **4.1.x** (`spark-core` / `spark-sql` are `provided` — Titian attaches
  to your existing Spark)
- Python **3.9+** for PySpark use

## Build

```bash
git clone https://github.com/SEED-VT/titian-spark-provenance.git
cd titian-spark-provenance
sbt package          # -> target/scala-2.13/titian_2.13-<version>.jar
```

Titian has one bundled runtime dependency, **fastutil** (`sbt package` resolves it
into the coursier cache; on Linux `~/.cache/coursier`, on macOS
`~/Library/Caches/Coursier`).

## Run the test suites

```bash
sbt test             # Scala suites, incl. local-cluster mode
sh python/tests/run.sh   # PySpark end-to-end (needs SPARK_HOME, see below)
```

The `local-cluster` suite and the PySpark test need a Spark binary distribution
unpacked at `tools/spark-4.1.2-bin-hadoop3/` (or set `SPARK_HOME`):

```bash
mkdir -p tools
curl -sL https://archive.apache.org/dist/spark/spark-4.1.2/spark-4.1.2-bin-hadoop3.tgz | tar xz -C tools
```

## Use in a Spark application

```
spark-submit \
  --jars titian.jar,fastutil.jar \
  --conf spark.sql.extensions=org.apache.spark.sql.lineage.TitianSQLExtension \
  --conf spark.titian.sql.capture=true \
  your-app.jar
```

- **Scala SQL/DataFrames**: `org.apache.spark.sql.lineage.TitianSQL`
  (`collectWithLineage`, `trace` → cursor with `goBack(path)/goNext/show`).
- **PySpark**: ship `python/titian.py` with `--py-files`; use `titian.Titian(spark)`.
- **RDD API**: wrap your `SparkContext` in
  `org.apache.spark.lineage.LineageContext`; see the README quick-start.

Toggle capture per query with `spark.titian.sql.capture` (`true`/`false`). With the
flag off the extension is a verified no-op.

## Notebooks

Three end-to-end notebooks (SQL, PySpark + Python UDFs, RDD) live in
[`notebooks/`](notebooks/).

**Docker (recommended)** — fully self-contained, builds the jar from source:

```bash
docker build -t titian -f docker/Dockerfile .
docker run --rm -p 8888:8888 titian        # JupyterLab at http://localhost:8888
docker run --rm titian validate            # execute all notebooks headlessly
```

**Local**:

```bash
sbt package
pip install pyspark==4.1.2 jupyterlab
cd notebooks && jupyter lab
```

The notebooks' bootstrap cell self-locates the jar, fastutil, data, and spark-shell;
override with `TITIAN_HOME` / `TITIAN_JAR` / `FASTUTIL_JAR` / `SPARK_HOME` if needed.
Make sure `JAVA_HOME` points at JDK 17+.

## TPC-DS coverage harness

```bash
pip install duckdb
python3 tpcds/gen_data.py 0.2          # ~200 MB of Parquet under tpcds/data/sf0.2
sbt 'examples/runMain edu.vt.bigdebug.examples.TPCDSCoverage'
# or a subset / other data dir:
sbt 'examples/runMain edu.vt.bigdebug.examples.TPCDSCoverage <dataDir> <queryDir> q3,q7'
```

Per query it checks capture-on answers against capture-off, traces one result row
back to a scan, and prints a PASS / UNSUPPORTED / ERROR scoreboard.

## Cluster deployment notes

- Ship exactly two jars (`titian.jar`, `fastutil.jar`); everything else comes from
  Spark.
- Lineage is materialized into executor BlockManagers. **Avoid dynamic allocation /
  executor preemption during a capture-and-trace session** — losing an executor loses
  its lineage partitions.
- `show()` resolves traces by deterministically re-scanning the source files: results
  are valid while the input files are unchanged.
- Capture supports a defined operator set and **fails loudly** on anything outside it
  (clear `TitianUnsupportedOperatorException`) — never silent wrong lineage. Display
  queries (`df.show()`, `LIMIT`) skip capture.
- Release a query's lineage blocks when done: `TitianSQL.releaseLineage(df)` /
  `t.release_lineage(df)`.
