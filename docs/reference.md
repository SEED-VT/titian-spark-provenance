# Command & flag reference

Every build task, script, runtime flag, and environment variable in one place. Use
`bin/sbt` (not a system `sbt`) — it pins the project-local JDK 17.

## Build & test

```bash
bin/sbt compile              # compile the library
bin/sbt test                 # full Scala suite (incl. local-cluster tests)
bin/sbt package              # -> target/scala-2.13/titian_2.13-<version>.jar
bin/sbt doc                  # Scaladoc -> target/scala-2.13/api/index.html
sh python/tests/run.sh       # PySpark end-to-end (needs SPARK_HOME)
```

## Examples & benchmarks

Example mains run from the repo root (relative data paths resolve).

```bash
bin/sbt 'examples/runMain edu.vt.bigdebug.examples.SalesAnalysis'
bin/sbt 'examples/runMain edu.vt.bigdebug.examples.OrderCustomerJoin'

# capture-overhead micro-benchmark (rows, default 2,000,000)
bin/sbt 'examples/runMain edu.vt.bigdebug.examples.CaptureOverheadBenchmark 2000000'

# ablation benchmark: every optimization config (rows, iters)
bin/sbt 'examples/runMain edu.vt.bigdebug.examples.AblationBenchmark 2000000 7'

# TPC-DS coverage scoreboard ([dataDir] [queryDir] [qN,qM] [--oracle])
bin/sbt 'examples/runMain edu.vt.bigdebug.examples.TPCDSCoverage'
bin/sbt 'examples/runMain edu.vt.bigdebug.examples.TPCDSCoverage \
  tpcds/data/sf0.2 tpcds/queries q3,q88 --oracle'
```

## TPC-DS data & ablation verification

```bash
pip install duckdb
python3 tpcds/gen_data.py 0.2    # Parquet via DuckDB dsdgen -> tpcds/data/sf0.2/
sh scripts/verify_ablation.sh    # full suite under every ablation flag (semantics)
```

## Notebooks (Docker)

```bash
docker build -t titian -f docker/Dockerfile .
docker run --rm -p 8888:8888 titian      # JupyterLab at http://localhost:8888
docker run --rm titian validate          # execute all notebooks headlessly
```

## Documentation site

```bash
pip install mkdocs-material
mkdocs serve                 # live preview at http://localhost:8000
mkdocs build                 # static site -> site/
```

## Runtime configuration flags

| flag | values | effect |
|---|---|---|
| `spark.sql.extensions` | `org.apache.spark.sql.lineage.TitianSQLExtension` | register SQL capture |
| `spark.titian.sql.capture` | `true` \| `false` (default `false`) | toggle SQL capture per query; off = verified no-op |
| `spark.titian.lineage.storageLevel` | a `StorageLevel` (default `MEMORY_AND_DISK_SER`) | where lineage blocks live |
| `spark.titian.ablation` | CSV of `legacyHash`, `boxedCombiner`, `boxedJoinBuffer`, `boxedBlocks`, `driverTrace` (default empty) | turn individual optimizations **off** — see [Ablation](ablation.md) |

### spark-submit template

```bash
spark-submit \
  --jars titian.jar,fastutil.jar \
  --conf spark.sql.extensions=org.apache.spark.sql.lineage.TitianSQLExtension \
  --conf spark.titian.sql.capture=true \
  your-app.jar
```

## Environment variables

| variable | purpose |
|---|---|
| `JAVA_HOME` | JDK 17+ (the `bin/sbt` wrapper sets this to the project's `tools/`) |
| `SPARK_HOME` | a Spark binary distribution; required by local-cluster tests and the PySpark e2e (`tools/spark-4.1.2-bin-hadoop3` by default) |
| `SPARK_SCALA_VERSION` | `2.13` — for the local-cluster launcher |
| `PYSPARK_PYTHON` | interpreter for the PySpark tests |
| `TITIAN_HOME` / `TITIAN_JAR` / `FASTUTIL_JAR` | notebook bootstrap overrides |
