/*
 * The SoCC '17 subject programs from the BigSift evaluation (maligulzar/BigSiftUI):
 * Weather Analysis and Student Info, reproduced faithfully (same pipeline and the same
 * `failure` oracle as the original), each with one injected fault. BigSift must isolate
 * the fault-inducing record(s). The Airport Transit benchmark is covered by
 * [[BigSiftVerificationSuite]]; AirQuality (an empty stub) and WordCount (no fault
 * oracle) carry no debugging scenario in the original repo.
 */
package org.apache.spark.bigsift

import org.apache.spark.SparkContext
import org.apache.spark.lineage.{LineageContext, LocalLineageContext}
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.rdd.Lineage
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Job pipelines as a top-level serializable object, so the Spark closures capture only
 * this object (not the non-serializable scalatest suite).
 */
object SoccBenchmarks {

  def convertToMm(s: String): Float = {
    val unit = s.substring(s.length - 2)
    val v = s.substring(0, s.length - 2).toFloat
    if (unit == "mm") v else v * 304.8f
  }

  def weatherJob(in: Lineage[String]): Lineage[(String, Float)] =
    in.flatMap { s =>
      val t = s.split(",")
      val date = t(1)
      val snow = convertToMm(t(2))
      val year = date.substring(date.lastIndexOf("/"))
      val monthDate = date.substring(0, date.lastIndexOf("/") - 1)
      Iterator((monthDate, snow), (year, snow))
    }.groupByKey().map { case (k, vs) => (k, vs.max - vs.min) }

  def studentJob(in: Lineage[String]): Lineage[(Int, Double)] =
    in.map { line => val l = line.split(" "); (l(4).toInt, l(3).toInt) }
      .groupByKey()
      .map { case (grade, ages) =>
        val itr = ages.toIterator
        var avg = 0.0; var num = 1
        while (itr.hasNext) { avg = avg + (itr.next() - avg) / num; num += 1 }
        (grade, avg)
      }
}

class SoccBenchmarksSuite extends AnyFunSuite with LocalLineageContext with Matchers {

  private def ctx(name: String): BigSift = {
    lc = new LineageContext(new SparkContext("local[2]", name))
    new BigSift(lc)
  }

  // ---- Weather Analysis -----------------------------------------------------
  // input: zip,date,snow ; per month-date and per year, delta = max snow - min snow;
  // failure: a delta greater than 6000 mm (BigSiftUI WeatherAnalysis.failure).
  test("Weather Analysis — isolates the anomalous snow reading (delta > 6000mm)") {
    val bs = ctx("socc-weather")
    val csv = "src/test/resources/socc_weather.csv"
    val corrupt = "90210,12/25/2015,90in"
    val r = bs.run(lc.textFile(csv, 2), SoccBenchmarks.weatherJob,
      (o: (String, Float)) => o._2 > 6000f)

    r.faultyOutputs.map(_._2).max should be > 6000f
    // the delta fault needs the huge reading paired with a low one; the corrupt
    // record is always part of the minimal cause
    r.faultInducingInputs should contain (corrupt)
    r.faultInducingInputs.size should be <= 3
  }

  // ---- Student Info ---------------------------------------------------------
  // input: name name gender age grade major ; moving-average age per grade;
  // failure: grade > 3 or average age outside [18, 25] (BigSiftUI StudentInfo.failure).
  test("Student Info — isolates the corrupt age that skews a grade's average") {
    val bs = ctx("socc-student")
    val txt = "src/test/resources/socc_student.txt"
    val corrupt = "g h male 99 0 CS"
    val r = bs.run(lc.textFile(txt, 2), SoccBenchmarks.studentJob,
      (o: (Int, Double)) => o._1 > 3 || o._2 > 25 || o._2 < 18)

    r.faultyOutputs.map(_._1) should contain (0)            // grade 0's average is off
    r.faultInducingInputs should equal (Seq(corrupt))       // the age-99 student
  }
}
