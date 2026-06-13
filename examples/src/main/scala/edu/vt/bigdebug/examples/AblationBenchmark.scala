package edu.vt.bigdebug.examples

import java.io.{File, PrintWriter}

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.lineage.{TitianSQL, TitianSQLExtension}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._

/**
 * Measures each optimization's contribution by re-running the capture/trace/footprint
 * benchmarks under every ablation configuration (fresh SparkContext per configuration
 * — the `spark.titian.ablation` flags are read once per SparkEnv).
 *
 * Configurations: all optimizations on (baseline), each ablation alone
 * (leave-one-out), and all ablations (the fully legacy implementation).
 *
 * Per configuration and workload, reports median wall time over interleaved
 * off/on pairs, capture-side JVM GC time, single-row trace latency, and lineage
 * footprint. Emits CSV to stdout (lines prefixed CSV,) and tpcds/ablation.csv.
 *
 * Run on quiet, dedicated hardware:
 *   bin/sbt 'examples/runMain edu.vt.bigdebug.examples.AblationBenchmark [rows] [iters]'
 */
object AblationBenchmark {

  private val configs: Seq[(String, String)] = Seq(
    "all-optimizations" -> "",
    "no-P1a-hash" -> "legacyHash",
    "no-P1b-combiner" -> "boxedCombiner",
    "no-P2-joinbuffer" -> "boxedJoinBuffer",
    "no-P3-blocks" -> "boxedBlocks",
    "no-P4-exectrace" -> "driverTrace",
    "fully-legacy" -> "legacyHash,boxedCombiner,boxedJoinBuffer,boxedBlocks,driverTrace")

  private val categories = Array(
    "electronics", "furniture", "groceries", "toys", "books", "garden", "sports",
    "clothing", "music", "auto")

  def main(args: Array[String]): Unit = {
    val rows = if (args.nonEmpty) args(0).toInt else 2000000
    val iterations = if (args.length > 1) args(1).toInt else 7
    val dataDir = new File(s"/tmp/titian-bench-$rows")
    if (!dataDir.exists()) generate(dataDir, rows)

    val out = new StringBuilder(
      "config,workload,off_ms,on_ms,overhead_pct,gc_on_ms,trace_ms,lineage_mem_mb\n")
    configs.foreach { case (name, flags) =>
      runConfig(name, flags, dataDir, iterations, out)
    }
    println(out.toString.linesIterator.map("CSV," + _).mkString("\n"))
    val f = new File("tpcds/ablation.csv")
    val w = new PrintWriter(f); w.print(out); w.close()
    println(s"written: ${f.getAbsolutePath}")
  }

  private def generate(dir: File, rows: Int): Unit = {
    dir.mkdirs()
    val rnd = new scala.util.Random(42)
    for (f <- 0 until 8) {
      val w = new PrintWriter(new File(dir, s"part-$f.csv"))
      var i = 0
      while (i < rows / 8) {
        w.println(s"${categories(rnd.nextInt(categories.length))},${rnd.nextInt(1000)}")
        i += 1
      }
      w.close()
    }
  }

  private def time[T](body: => T): Long = {
    val t0 = System.nanoTime(); body; (System.nanoTime() - t0) / 1000000
  }
  private def median(xs: Seq[Long]): Long = xs.sorted.apply(xs.length / 2)

  /** Sums task JVM GC time between resets — capture-side allocation pressure. */
  private class GcListener extends SparkListener {
    val total = new java.util.concurrent.atomic.AtomicLong()
    override def onTaskEnd(t: SparkListenerTaskEnd): Unit =
      if (t.taskMetrics != null) total.addAndGet(t.taskMetrics.jvmGCTime)
  }

  private def runConfig(
      name: String, flags: String, dataDir: File, iterations: Int,
      out: StringBuilder): Unit = {
    println(s"\n=== config: $name [${if (flags.isEmpty) "none" else flags}] ===")

    // ---- RDD workload (flags affecting: legacyHash, boxedCombiner) ----
    val conf = new SparkConf().setMaster("local[2]").setAppName(s"ablation-$name")
      .set("spark.titian.ablation", flags)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val gc = new GcListener; sc.addSparkListener(gc)
    val lc = new LineageContext(sc)
    def rddRun(capture: Boolean): Long = {
      lc.setCaptureLineage(capture)
      val t = time {
        lc.textFile(dataDir.getAbsolutePath, 8).map { line =>
          val c = line.indexOf(',')
          (line.substring(0, c), line.substring(c + 1).toInt)
        }.reduceByKey(_ + _).collect()
      }
      if (capture) { lc.setCaptureLineage(false); lc.releaseLineage() }
      t
    }
    rddRun(false); rddRun(true) // warmup
    val rddPairs = (1 to iterations).map { _ =>
      val off = rddRun(false)
      gc.total.set(0); val on = rddRun(true); (off, on, gc.total.get())
    }
    report(out, name, "rdd_reduceByKey",
      rddPairs.map(_._1), rddPairs.map(_._2), median(rddPairs.map(_._3)), -1, -1)
    sc.stop()

    // ---- SQL workload (flags affecting: boxedJoinBuffer, boxedBlocks, driverTrace) --
    val spark = SparkSession.builder().master("local[2]")
      .appName(s"ablation-sql-$name")
      .config("spark.sql.extensions", classOf[TitianSQLExtension].getName)
      .config("spark.titian.ablation", flags)
      .config("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .config("spark.sql.shuffle.partitions", "8")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val gc2 = new GcListener; spark.sparkContext.addSparkListener(gc2)
    spark.read.schema("category STRING, amount INT")
      .csv(dataDir.getAbsolutePath).createOrReplaceTempView("sales")
    val dimDir = s"${dataDir.getAbsolutePath}-dim"
    if (!new File(dimDir).exists()) {
      spark.createDataFrame(
        categories.zipWithIndex.map { case (c, i) => (c, s"owner-$i") }.toSeq)
        .toDF("category", "owner").write.mode("overwrite").csv(dimDir)
    }
    spark.read.schema("category STRING, owner STRING").csv(dimDir)
      .createOrReplaceTempView("dim")
    val joinSql =
      """SELECT d.owner, SUM(s.amount) AS total FROM sales s
        |JOIN dim d ON s.category = d.category GROUP BY d.owner
        |ORDER BY total DESC LIMIT 5""".stripMargin

    def sqlRun(capture: Boolean): Long = {
      spark.conf.set("spark.titian.sql.capture", capture.toString)
      val df = spark.sql(joinSql)
      val t = time { df.collect() }
      if (capture) TitianSQL.releaseLineage(df)
      t
    }
    sqlRun(false); sqlRun(true) // warmup
    val sqlPairs = (1 to iterations).map { _ =>
      val off = sqlRun(false)
      gc2.total.set(0); val on = sqlRun(true); (off, on, gc2.total.get())
    }

    // trace latency + footprint on one kept capture
    spark.conf.set("spark.titian.sql.capture", "true")
    val df = spark.sql(joinSql)
    val withIds = TitianSQL.collectWithLineage(df)
    val traceMs = median((1 to 3).map { _ =>
      time {
        var c = TitianSQL.trace(df, Seq(withIds.head._2))
        while (!c.atScan) c = c.goBack(0)
      }
    })
    val (mem, _) = TitianSQL.lineageSize(df)
    TitianSQL.releaseLineage(df)
    report(out, name, "sql_join_agg_limit",
      sqlPairs.map(_._1), sqlPairs.map(_._2), median(sqlPairs.map(_._3)),
      traceMs, mem / 1024 / 1024)
    spark.stop()
  }

  private def report(
      out: StringBuilder, config: String, workload: String,
      off: Seq[Long], on: Seq[Long], gcOn: Long, traceMs: Long, memMb: Long): Unit = {
    val (mOff, mOn) = (median(off), median(on))
    val pct = (mOn - mOff) * 100.0 / mOff
    println(f"$workload%-22s off=$mOff%5d ms  on=$mOn%5d ms  overhead=$pct%6.1f%%  " +
      f"gcOn=$gcOn%4d ms  trace=$traceMs%5d ms  mem=$memMb%4d MB")
    out.append(f"$config,$workload,$mOff,$mOn,$pct%.1f,$gcOn,$traceMs,$memMb%n")
  }
}
