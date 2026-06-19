package edu.vt.bigdebug.examples

import org.apache.spark.SparkContext
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.rdd.Lineage
import org.apache.spark.bigsift.BigSift

/**
 * BigSift on the SoCC '17 Weather Analysis benchmark (maligulzar/BigSiftUI).
 *
 * Input rows are `zip,date,snow` (snow as e.g. `100mm` or `0.4ft`). For each month-date
 * and each year, the job computes the snow delta `max - min`; a delta over 6000 mm is
 * treated as an anomaly.
 *
 * The injected fault is a `90in` reading (90 inches of snow). `convert_to_mm` has the
 * original's bug — it treats every non-`mm` unit as *feet* (`× 304.8`), so `90in`
 * becomes 27432 mm instead of 2286 mm, blowing the delta past 6000. BigSift isolates
 * that `90in` record (plus a low companion reading, since the fault is a max−min
 * delta).
 *
 *   bin/sbt 'examples/runMain edu.vt.bigdebug.examples.WeatherAnalysisDemo'
 */
object WeatherAnalysisDemo {

  /** snow string -> millimetres (BigSiftUI WeatherAnalysis.convert_to_mm). */
  def toMm(s: String): Float = {
    val unit = s.substring(s.length - 2)
    val v = s.substring(0, s.length - 2).toFloat
    if (unit == "mm") v else v * 304.8f
  }

  /**
   * Per month-date and per year: delta = max snow - min snow.
   *
   * Parsing is defensive (malformed lines are skipped). Besides being good practice,
   * this keeps BigSift's delta-debugging re-runs clean: on the RDD path BigSift
   * validates its provenance by re-running the job, and a best-effort trace can hand
   * back a non-source string — without the guard the parser would throw (caught by
   * BigSift, but noisily logged by Spark).
   */
  def job(in: Lineage[String]): Lineage[(String, Float)] =
    in.flatMap { s =>
      try {
        val t = s.split(",")
        val date = t(1)
        val snow = toMm(t(2))
        val slash = date.lastIndexOf("/")
        if (slash < 1) Iterator.empty
        else Iterator((date.substring(0, slash - 1), snow), (date.substring(slash), snow))
      } catch { case _: Throwable => Iterator.empty }
    }.groupByKey().map { case (k, vs) => (k, vs.max - vs.min) }

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "weather-analysis-demo")
    sc.setLogLevel("ERROR")
    val lc = new LineageContext(sc)
    val data = if (args.nonEmpty) args(0) else "src/test/resources/socc_weather.csv"

    println("=== Weather Analysis: explain the anomalous snow delta (> 6000mm) ===")
    val r = new BigSift(lc).run(
      lc.textFile(data, 2), job,
      (o: (String, Float)) => o._2 > 6000f)            // failure: delta over 6000 mm

    println(s"faulty outputs (key, delta):  ${r.faultyOutputs.mkString(", ")}")
    println(s"candidates (provenance):      ${r.provenanceSize} rows")
    println(s"fault-inducing input(s):")
    r.faultInducingInputs.foreach(row => println(s"    $row"))

    sc.stop()
  }
}
