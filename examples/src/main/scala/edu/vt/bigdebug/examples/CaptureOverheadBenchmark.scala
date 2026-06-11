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
    val iterations = 3

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
      time {
        val pairs = lc.textFile(dataDir.getAbsolutePath, 8).map { line =>
          val c = line.indexOf(',')
          (line.substring(0, c), line.substring(c + 1).toInt)
        }
        pairs.reduceByKey(_ + _).collect()
      }
    }

    run(capture = false) // warmup
    val off = (1 to iterations).map(_ => run(capture = false))
    val on = (1 to iterations).map(_ => run(capture = true))
    report("RDD reduceByKey", off, on)
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

    def run(sql: String, capture: Boolean): Long = {
      spark.conf.set("spark.titian.sql.capture", capture.toString)
      time { spark.sql(sql).collect() }
    }

    val aggSql = "SELECT category, SUM(amount), COUNT(*) FROM sales GROUP BY category"
    val joinSql =
      """SELECT d.owner, SUM(s.amount) FROM sales s
        |JOIN dim d ON s.category = d.category GROUP BY d.owner""".stripMargin

    run(aggSql, capture = false) // warmup
    report("SQL groupBy aggregate",
      (1 to iterations).map(_ => run(aggSql, capture = false)),
      (1 to iterations).map(_ => run(aggSql, capture = true)))

    run(joinSql, capture = false) // warmup
    report("SQL join + aggregate",
      (1 to iterations).map(_ => run(joinSql, capture = false)),
      (1 to iterations).map(_ => run(joinSql, capture = true)))

    spark.stop()
  }
}
