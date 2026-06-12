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
 * Usage:
 *   python3 tpcds/gen_data.py 0.2                       # once, needs `pip install duckdb`
 *   bin/sbt 'examples/runMain edu.vt.bigdebug.examples.TPCDSCoverage \
 *     tpcds/data/sf0.2 tpcds/queries [q3,q7,...]'
 */
object TPCDSCoverage {

  private sealed trait Outcome
  private case object Pass extends Outcome
  private case class Unsupported(op: String) extends Outcome
  private case class Error(kind: String, msg: String) extends Outcome

  def main(args: Array[String]): Unit = {
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

    // a single long-lived local session degrades after a few thousand stages
    // (codegen class-server failures): recycle it every few queries
    val results = queries.grouped(15).flatMap { group =>
      val spark = newSession()
      val rs = group.map { case (name, file) =>
        val sql = new String(Files.readAllBytes(file.toPath))
        val r = name -> runOne(spark, name, sql)
        println(f"${r._1}%-6s ${r._2}")
        r
      }
      spark.stop()
      rs
    }.toSeq

    println("\n=== TPC-DS lineage coverage ===")
    results.foreach {
      case (n, Pass) => println(f"$n%-6s PASS")
      case (n, Unsupported(op)) => println(f"$n%-6s UNSUPPORTED  $op")
      case (n, Error(kind, msg)) => println(f"$n%-6s ERROR[$kind]  ${msg.take(110)}")
    }
    val pass = results.count(_._2 == Pass)
    val unsup = results.count(_._2.isInstanceOf[Unsupported])
    val err = results.size - pass - unsup
    println(s"\n${results.size} queries: $pass pass, $unsup unsupported, $err error")
    val byOp = results.collect { case (_, Unsupported(op)) => op }
      .groupBy(identity).view.mapValues(_.size).toSeq.sortBy(-_._2)
    if (byOp.nonEmpty) {
      println("unsupported by operator:")
      byOp.foreach { case (op, n) => println(f"  $op%-40s $n") }
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
