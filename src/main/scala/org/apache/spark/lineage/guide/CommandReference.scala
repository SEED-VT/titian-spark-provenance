/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.lineage.guide

/**
 * =Command & flag catalog=
 *
 * Every build task, script, runtime flag, and environment variable in one place. Use
 * `bin/sbt` (not a system `sbt`) — it pins the project-local JDK 17.
 *
 * ==Build & test==
 * {{{
 * bin/sbt compile                 // compile the library
 * bin/sbt test                    // full Scala suite (incl. local-cluster tests)
 * bin/sbt package                 // -> target/scala-2.13/titian_2.13-<version>.jar
 * bin/sbt doc                     // Scaladoc -> target/scala-2.13/api/index.html
 * sh python/tests/run.sh          // PySpark end-to-end (needs SPARK_HOME)
 * }}}
 *
 * ==Examples & benchmarks== ([[org.apache.spark.lineage.guide]] runs from repo root)
 * {{{
 * bin/sbt 'examples/runMain edu.vt.bigdebug.examples.SalesAnalysis'
 * bin/sbt 'examples/runMain edu.vt.bigdebug.examples.OrderCustomerJoin'
 *
 * // capture-overhead micro-benchmark (rows, default 2,000,000)
 * bin/sbt 'examples/runMain edu.vt.bigdebug.examples.CaptureOverheadBenchmark 2000000'
 *
 * // ablation benchmark: every optimization config (rows, iters)
 * bin/sbt 'examples/runMain edu.vt.bigdebug.examples.AblationBenchmark 2000000 7'
 *
 * // TPC-DS coverage scoreboard ([dataDir] [queryDir] [qN,qM] [--oracle])
 * bin/sbt 'examples/runMain edu.vt.bigdebug.examples.TPCDSCoverage'
 * bin/sbt 'examples/runMain edu.vt.bigdebug.examples.TPCDSCoverage \
 *   tpcds/data/sf0.2 tpcds/queries q3,q88 --oracle'
 * }}}
 *
 * ==TPC-DS data & ablation verification==
 * {{{
 * pip install duckdb
 * python3 tpcds/gen_data.py 0.2   // Parquet via DuckDB dsdgen -> tpcds/data/sf0.2/
 * sh scripts/verify_ablation.sh   // full suite under every ablation flag (semantics)
 * }}}
 *
 * ==Notebooks (Docker)==
 * {{{
 * docker build -t titian -f docker/Dockerfile .
 * docker run --rm -p 8888:8888 titian      // JupyterLab at http://localhost:8888
 * docker run --rm titian validate          // execute all notebooks headlessly
 * }}}
 *
 * ==Runtime configuration flags==
 *   - `spark.sql.extensions` = `org.apache.spark.sql.lineage.TitianSQLExtension`
 *     — registers SQL capture. ([[org.apache.spark.sql.lineage.TitianSQLExtension]])
 *   - `spark.titian.sql.capture` = `true` | `false` — toggle SQL capture per query
 *     (default `false`; off = verified no-op).
 *   - `spark.titian.lineage.storageLevel` — where lineage blocks live (default
 *     `MEMORY_AND_DISK_SER`).
 *   - `spark.titian.ablation` — comma-separated optimization opt-outs (default empty =
 *     fully optimized): `legacyHash`, `boxedCombiner`, `boxedJoinBuffer`,
 *     `boxedBlocks`, `driverTrace`. See [[org.apache.spark.lineage]] for what each
 *     disables.
 *
 * ==spark-submit template==
 * {{{
 * spark-submit \
 *   --jars titian.jar,fastutil.jar \
 *   --conf spark.sql.extensions=org.apache.spark.sql.lineage.TitianSQLExtension \
 *   --conf spark.titian.sql.capture=true \
 *   your-app.jar
 * }}}
 *
 * ==Environment variables==
 *   - `JAVA_HOME` — JDK 17+ (the `bin/sbt` wrapper sets this to the project's tools/).
 *   - `SPARK_HOME` — a Spark binary distribution; required by the local-cluster tests
 *     and the PySpark e2e (`tools/spark-4.1.2-bin-hadoop3` by default).
 *   - `SPARK_SCALA_VERSION` = `2.13` — for the local-cluster launcher.
 *   - `PYSPARK_PYTHON` — interpreter for the PySpark tests.
 *   - `TITIAN_HOME` / `TITIAN_JAR` / `FASTUTIL_JAR` — notebook bootstrap overrides.
 *
 * @see [[org.apache.spark.lineage.guide.DeveloperGuide]] for architecture & extension.
 */
object CommandReference
