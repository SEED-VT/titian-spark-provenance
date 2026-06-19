package edu.vt.bigdebug.examples

import org.apache.spark.SparkContext
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.rdd.Lineage
import org.apache.spark.bigsift.BigSift

/**
 * BigSift on the SoCC '17 Weather Analysis benchmark (maligulzar/BigSiftUI).
 *
 * Input rows are `zip,date,snow` (snow as e.g. `100mm` or `12in`). For each month-date
 * and each year, the job computes the snow delta `max - min`; a delta over 6000 mm is
 * treated as an anomaly. One corrupt reading (`50000mm`) blows up the delta, and
 * BigSift isolates the record(s) responsible.
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

  /** per month-date and per year: delta = max snow - min snow. */
  def job(in: Lineage[String]): Lineage[(String, Float)] =
    in.flatMap { s =>
      val t = s.split(",")
      val date = t(1)
      val snow = toMm(t(2))
      val year = date.substring(date.lastIndexOf("/"))
      val monthDate = date.substring(0, date.lastIndexOf("/") - 1)
      Iterator((monthDate, snow), (year, snow))
    }.groupByKey().map { case (k, vs) => (k, vs.max - vs.min) }

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "weather-analysis-demo")
    sc.setLogLevel("ERROR")
    val lc = new LineageContext(sc)
    val data = if (args.nonEmpty) args(0) else "examples/data/weather.csv"

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
