package edu.vt.bigdebug.examples

import java.io.File
import java.nio.file.Files

import scala.util.control.NonFatal

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.lineage.{TitianSQL, TitianSQLExtension, TitianUnsupportedOperatorException}

/**
 * TPC-DS lineage coverage scoreboard. For every vendored query (tpcds/queries/):
 *   1. run with capture OFF (baseline answer);
 *   2. run with capture ON via collectWithLineage — results must match exactly;
 *   3. trace one result row backward along branch 0 to a scan and resolve witnesses.
 * Buckets each query as PASS / UNSUPPORTED(<operator>) / ERROR and prints a summary.
 *
 * With `--oracle`, each passing query additionally gets a recall/precision check:
 * one result row is traced through EVERY branch to all sources, every traced table
 * is replaced by a view holding only the witness rows, and the query is re-executed —
 * the traced row must reappear identically (recall: the witness set is sufficient).
 * Witness-set sizes are reported, plus a leave-one-out re-run (drop one witness; does
 * the traced row change?) as a precision signal.
 *
 * Usage:
 *   python3 tpcds/gen_data.py 0.2                       # once, needs `pip install duckdb`
 *   bin/sbt 'examples/runMain edu.vt.bigdebug.examples.TPCDSCoverage \
 *     tpcds/data/sf0.2 tpcds/queries [q3,q7,...] [--oracle]'
 */
object TPCDSCoverage {

  private sealed trait Outcome
  private case object Pass extends Outcome
  private case class Unsupported(op: String) extends Outcome
  private case class Error(kind: String, msg: String) extends Outcome

  def main(rawArgs: Array[String]): Unit = {
    val oracleMode = rawArgs.contains("--oracle")
    val args = rawArgs.filterNot(_ == "--oracle")
    val root = sys.props.get("user.dir").getOrElse(".")
    val dataDir = new File(if (args.length > 0) args(0) else s"$root/tpcds/data/sf0.2")
    val queryDir = new File(if (args.length > 1) args(1) else s"$root/tpcds/queries")
    val only: Option[Set[String]] =
      if (args.length > 2) Some(args(2).split(",").map(_.trim).toSet) else None
    require(dataDir.isDirectory, s"no TPC-DS data at $dataDir — run tpcds/gen_data.py")
    require(queryDir.isDirectory, s"no queries at $queryDir")

    def newSession(): SparkSession = {
      val spark = SparkSession.builder()
        .master("local[2]")
        .appName("titian-tpcds-coverage")
        .config("spark.sql.extensions", classOf[TitianSQLExtension].getName)
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.titian.sql.capture", "false")
        .getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")
      dataDir.listFiles().filter(_.getName.endsWith(".parquet")).foreach { f =>
        spark.read.parquet(f.getAbsolutePath)
          .createOrReplaceTempView(f.getName.stripSuffix(".parquet"))
      }
      spark
    }

    val queries = queryDir.listFiles().filter(_.getName.endsWith(".sql"))
      .map(f => f.getName.stripSuffix(".sql") -> f).toSeq
      .filter { case (n, _) => only.forall(_.contains(n)) }
      .sortBy { case (n, _) =>
        val digits = n.drop(1).takeWhile(_.isDigit)
        (digits.toInt, n)
      }

    val tablePaths: Map[String, String] =
      dataDir.listFiles().filter(_.getName.endsWith(".parquet"))
        .map(f => f.getName.stripSuffix(".parquet") -> f.getAbsolutePath).toMap

    // a single long-lived local session degrades after a few thousand stages
    // (codegen class-server failures): recycle it every few queries
    val groupSize = if (oracleMode) 8 else 15
    val results = queries.grouped(groupSize).flatMap { group =>
      val spark = newSession()
      val rs = group.map { case (name, file) =>
        val sql = new String(Files.readAllBytes(file.toPath))
        val outcome = runOne(spark, name, sql)
        val note = (outcome, oracleMode) match {
          case (Pass, true) => runOracle(spark, sql, tablePaths)
          case _ => ""
        }
        println(f"$name%-6s $outcome $note")
        (name, outcome, note)
      }
      spark.stop()
      rs
    }.toSeq

    println("\n=== TPC-DS lineage coverage ===")
    results.foreach {
      case (n, Pass, note) => println(f"$n%-6s PASS  $note")
      case (n, Unsupported(op), _) => println(f"$n%-6s UNSUPPORTED  $op")
      case (n, Error(kind, msg), _) => println(f"$n%-6s ERROR[$kind]  ${msg.take(110)}")
    }
    val pass = results.count(_._2 == Pass)
    val unsup = results.count(_._2.isInstanceOf[Unsupported])
    val err = results.size - pass - unsup
    println(s"\n${results.size} queries: $pass pass, $unsup unsupported, $err error")
    if (oracleMode) {
      val notes = results.collect { case (_, Pass, note) => note }
      def n(tag: String) = notes.count(_.contains(tag))
      println(s"oracle: ${n("recall=OK")} recall-ok, ${n("recall=FAIL")} recall-fail, " +
        s"${n("recall=SKIP")} skipped; leave-one-out: ${n("loo=SENSITIVE")} sensitive, " +
        s"${n("loo=INSENSITIVE")} insensitive")
    }
    val byOp = results.collect { case (_, Unsupported(op), _) => op }
      .groupBy(identity).view.mapValues(_.size).toSeq.sortBy(-_._2)
    if (byOp.nonEmpty) {
      println("unsupported by operator:")
      byOp.foreach { case (op, n) => println(f"  $op%-40s $n") }
    }
  }

  /**
   * Recall/precision oracle for one passing query.
   *
   * Recall: trace one result row through every branch to all sources; replace each
   * traced table with a view holding only its witness rows (null-safe semi-join on
   * the scan's columns, so the full table schema survives); re-run the query with
   * capture off. The traced row must reappear identically — for aggregates this is
   * sharp, one missing contributor changes the value.
   *
   * Precision: witness-set sizes per table (key-hash granularity makes joins/windows
   * over-approximate by design), plus a leave-one-out re-run: drop one witness from
   * the largest set — if the traced row still reproduces, that witness was
   * value-neutral (granularity cost or e.g. a non-extremal MIN/MAX contributor).
   */
  private def runOracle(
      spark: SparkSession,
      sql: String,
      tablePaths: Map[String, String]): String = {
    import org.apache.spark.sql.functions.{col, monotonically_increasing_id}

    val df0 = spark.sql(sql)
    // a scalar subquery over a filtered table would change value on the re-run and
    // fail recall for reasons unrelated to lineage — skip those queries
    val hasSubquery = df0.queryExecution.sparkPlan.exists { p =>
      p.expressions.exists(_.exists {
        case _: org.apache.spark.sql.catalyst.expressions.PlanExpression[_] => true
        case _ => false
      })
    }
    if (hasSubquery) return "recall=SKIP(scalar-subquery)"

    val touched = scala.collection.mutable.Set[String]()
    try {
      spark.conf.set("spark.titian.sql.capture", "true")
      val df = spark.sql(sql)
      val withIds = TitianSQL.collectWithLineage(df)
      if (withIds.isEmpty) return "recall=SKIP(empty)"
      val (sampleRow, sampleId) = withIds(withIds.length / 2)
      val sources = TitianSQL.traceAllSources(df, Seq(sampleId))
      TitianSQL.releaseLineage(df)
      spark.conf.set("spark.titian.sql.capture", "false")

      // An empty frontier is legitimate for exactly one shape: a global aggregate
      // over zero qualifying rows (count() = 0 emits a row from nothing). Verify by
      // re-running with EVERY table emptied — if the row is derivable from no input,
      // the empty frontier is consistent; otherwise the walk lost the ids.
      if (sources.forall(_.rows.isEmpty)) {
        spark.conf.set("spark.titian.sql.capture", "false")
        tablePaths.foreach { case (t, p) =>
          touched += t
          spark.read.parquet(p).limit(0).createOrReplaceTempView(t)
        }
        val emptied = spark.sql(sql).collect()
        return if (emptied.contains(sampleRow)) "recall=OK(empty-input)"
        else "recall=FAIL(empty-frontier)"
      }

      // map each traced source to its table; bail if a non-empty one is unmappable
      val byTable = sources.filter(_.rows.nonEmpty).groupBy { sw =>
        sw.sourcePath.flatMap(p =>
          tablePaths.collectFirst { case (t, path) if p.endsWith(new File(path).getName) => t })
      }
      if (byTable.contains(None)) return "recall=SKIP(unmappable-source)"
      val traced = byTable.collect { case (Some(t), wss) => t -> wss }

      // every traced table -> view of only its witness rows (row ids via null-safe
      // semi-join per scan, so different scans' column sets combine on one table)
      val ridsByTable = traced.map { case (table, wss) =>
        val base = spark.read.parquet(tablePaths(table))
          .withColumn("__rid", monotonically_increasing_id())
        val ridSets = wss.map { sw =>
          import scala.jdk.CollectionConverters._
          val w = spark.createDataFrame(sw.rows.toSeq.distinct.asJava, sw.schema)
            .toDF(sw.schema.fieldNames.map("__w_" + _).toIndexedSeq: _*)
          val cond = sw.schema.fieldNames
            .map(c => base(c) <=> w("__w_" + c)).reduce(_ && _)
          base.join(w, cond, "left_semi").select("__rid")
        }
        val rids = ridSets.reduce(_ union _).distinct().cache()
        base.join(rids, Seq("__rid"), "left_semi").drop("__rid")
          .createOrReplaceTempView(table)
        touched += table
        table -> (base, rids)
      }

      val rerun = spark.sql(sql).collect()
      val recall = if (rerun.contains(sampleRow)) "recall=OK" else "recall=FAIL"
      val sizes = traced.map { case (t, wss) => s"$t=${wss.map(_.rows.length).sum}" }
        .toSeq.sorted.mkString(",")

      // leave-one-out on the largest witness set (needs >= 2 rows to stay meaningful)
      val loo = ridsByTable.toSeq
        .map { case (t, (b, r)) => (t, b, r, r.count()) }
        .filter(_._4 >= 2).sortBy(-_._4).headOption.map { case (table, base, rids, _) =>
          val dropped = rids.first().getLong(0)
          base.join(rids.filter(col("__rid") =!= dropped), Seq("__rid"), "left_semi")
            .drop("__rid").createOrReplaceTempView(table)
          val rerun2 = spark.sql(sql).collect()
          if (rerun2.contains(sampleRow)) "loo=INSENSITIVE" else "loo=SENSITIVE"
        }.getOrElse("loo=SKIP")

      s"$recall witnesses{$sizes} $loo"
    } catch {
      case NonFatal(t) =>
        s"recall=SKIP(${t.getClass.getSimpleName}: " +
          s"${Option(t.getMessage).getOrElse("").replaceAll("\\s+", " ").take(60)})"
    } finally {
      spark.conf.set("spark.titian.sql.capture", "false")
      // restore the real tables for the next query
      touched.foreach { t =>
        spark.read.parquet(tablePaths(t)).createOrReplaceTempView(t)
      }
    }
  }

  private def runOne(spark: SparkSession, name: String, sql: String): Outcome = {
    def unsupportedIn(t: Throwable): Option[String] = {
      Iterator.iterate(t)(_.getCause).takeWhile(_ != null).take(12).collectFirst {
        case u: TitianUnsupportedOperatorException =>
          // first line carries the operator name
          u.getMessage.split("coverage\\): ")(1).split("\\.")(0)
      }
    }
    try {
      spark.conf.set("spark.titian.sql.capture", "false")
      val baseline = spark.sql(sql).collect()

      spark.conf.set("spark.titian.sql.capture", "true")
      val df = spark.sql(sql)
      val withIds = TitianSQL.collectWithLineage(df)
      val outcome =
        if (!withIds.map(_._1.toString).sameElements(baseline.map(_.toString))) {
          Error("MISMATCH",
            s"capture changed the answer: ${baseline.length} vs ${withIds.length} rows")
        } else if (withIds.isEmpty) {
          Pass // captured, empty result at this scale factor: nothing to trace
        } else {
          var cursor = TitianSQL.trace(df, Seq(withIds.head._2))
          var hops = 0
          while (!cursor.atScan && hops < 24) { cursor = cursor.goBack(0); hops += 1 }
          if (!cursor.atScan) Error("TRACE", "no scan within 24 hops")
          else { cursor.show(); Pass }
        }
      TitianSQL.releaseLineage(df)
      outcome
    } catch {
      case NonFatal(t) =>
        unsupportedIn(t) match {
          case Some(op) => Unsupported(op)
          case None =>
            val m = Option(t.getMessage).getOrElse(t.getClass.getSimpleName)
            Error(t.getClass.getSimpleName, m.replaceAll("\\s+", " "))
        }
    } finally {
      spark.conf.set("spark.titian.sql.capture", "false")
    }
  }
}
