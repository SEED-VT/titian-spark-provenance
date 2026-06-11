package edu.vt.bigdebug.examples

import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.lineage.TitianSQLExtension

/**
 * Capture-overhead micro-benchmark: runs each workload with lineage capture off and
 * on, reports median wall time and overhead. Workloads: RDD reduceByKey, SQL groupBy
 * aggregate, SQL join+aggregate. Synthetic data is generated under /tmp.
 *
 * Run with: bin/sbt 'examples/runMain edu.vt.bigdebug.examples.CaptureOverheadBenchmark [rows]'
 */
object CaptureOverheadBenchmark {

  private val categories = Array(
    "electronics", "furniture", "groceries", "toys", "books", "garden", "sports",
    "clothing", "music", "auto")

  def main(args: Array[String]): Unit = {
    val rows = if (args.nonEmpty) args(0).toInt else 2000000
    val iterations = if (args.length > 1) args(1).toInt else 7

    val dataDir = new File(s"/tmp/titian-bench-${rows}")
    if (!dataDir.exists()) generate(dataDir, rows)

    println(s"\n=== Titian capture-overhead benchmark ($rows rows, " +
      s"median of $iterations) ===\n")

    benchRDD(dataDir, iterations)
    benchSQL(dataDir, iterations)
  }

  private def generate(dir: File, rows: Int): Unit = {
    dir.mkdirs()
    val files = 8
    val perFile = rows / files
    val rnd = new scala.util.Random(42)
    for (f <- 0 until files) {
      val w = new PrintWriter(new File(dir, s"part-$f.csv"))
      var i = 0
      while (i < perFile) {
        val cat = categories(rnd.nextInt(categories.length))
        w.println(s"$cat,${rnd.nextInt(1000)}")
        i += 1
      }
      w.close()
    }
    println(s"generated ${files} files in $dir")
  }

  private def time[T](body: => T): Long = {
    val t0 = System.nanoTime()
    body
    (System.nanoTime() - t0) / 1000000
  }

  private def median(xs: Seq[Long]): Long = xs.sorted.apply(xs.length / 2)

  private def report(name: String, off: Seq[Long], on: Seq[Long]): Unit = {
    val (mOff, mOn) = (median(off), median(on))
    val pct = (mOn - mOff) * 100.0 / mOff
    println(f"$name%-28s off: $mOff%6d ms   on: $mOn%6d ms   overhead: $pct%6.1f%%")
  }

  private def benchRDD(dataDir: File, iterations: Int): Unit = {
    val sc = new SparkContext(
      new SparkConf().setMaster("local[2]").setAppName("titian-bench-rdd"))
    sc.setLogLevel("WARN")
    val lc = new LineageContext(sc)

    def run(capture: Boolean): Long = {
      lc.setCaptureLineage(capture)
      val t = time {
        val pairs = lc.textFile(dataDir.getAbsolutePath, 8).map { line =>
          val c = line.indexOf(',')
          (line.substring(0, c), line.substring(c + 1).toInt)
        }
        pairs.reduceByKey(_ + _).collect()
      }
      if (capture) {
        // finalize capture (trace-DAG reconstruction) outside the timed region and
        // drop the blocks so iterations don't fill the memory store
        lc.setCaptureLineage(false)
        lc.releaseLineage()
      }
      t
    }

    run(capture = false); run(capture = true) // warmup
    // interleave off/on so JIT and page-cache warmup spread evenly over both
    val pairs = (1 to iterations).map(_ => (run(capture = false), run(capture = true)))
    report("RDD reduceByKey", pairs.map(_._1), pairs.map(_._2))
    sc.stop()
  }

  private def benchSQL(dataDir: File, iterations: Int): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("titian-bench-sql")
      .config("spark.sql.extensions", classOf[TitianSQLExtension].getName)
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.sql.adaptive.skewJoin.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    spark.read.schema("category STRING, amount INT")
      .csv(dataDir.getAbsolutePath).createOrReplaceTempView("sales")
    spark.createDataFrame(
      categories.zipWithIndex.map { case (c, i) => (c, s"owner-$i") }.toSeq)
      .toDF("category", "owner").write.mode("overwrite")
      .csv(s"$dataDir-dim")
    spark.read.schema("category STRING, owner STRING")
      .csv(s"$dataDir-dim").createOrReplaceTempView("dim")

    import org.apache.spark.sql.lineage.TitianSQL

    def run(sql: String, capture: Boolean): Long = {
      spark.conf.set("spark.titian.sql.capture", capture.toString)
      val df = spark.sql(sql)
      val t = time { df.collect() }
      // drop blocks so iterations don't fill the memory store
      if (capture) TitianSQL.releaseLineage(df)
      t
    }

    def bench(name: String, sql: String): Unit = {
      run(sql, capture = false); run(sql, capture = true) // warmup
      // interleave off/on so JIT and page-cache warmup spread evenly over both
      val pairs = (1 to iterations).map(_ => (run(sql, capture = false), run(sql, capture = true)))
      report(name, pairs.map(_._1), pairs.map(_._2))
    }

    val aggSql = "SELECT category, SUM(amount), COUNT(*) FROM sales GROUP BY category"
    val joinSql =
      """SELECT d.owner, SUM(s.amount) FROM sales s
        |JOIN dim d ON s.category = d.category GROUP BY d.owner""".stripMargin

    bench("SQL groupBy aggregate", aggSql)
    bench("SQL join + aggregate", joinSql)

    // one captured run kept alive: trace latency and lineage footprint
    {
      spark.conf.set("spark.titian.sql.capture", "true")
      val df = spark.sql(joinSql)
      val rowsWithIds = TitianSQL.collectWithLineage(df)
      val tTrace = time {
        var c = TitianSQL.trace(df, Seq(rowsWithIds.head._2))
        while (!c.atScan) c = c.goBack(0)
      }
      val (mem, disk) = TitianSQL.lineageSize(df)
      println(f"SQL join backward trace (1 row): $tTrace%d ms   lineage blocks: " +
        f"${mem / 1024.0 / 1024.0}%.1f MB mem, ${disk / 1024.0 / 1024.0}%.1f MB disk")
      TitianSQL.releaseLineage(df)
    }

    spark.stop()
  }
}
