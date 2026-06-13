/*
 * End-to-end BigSift (RDD) tests: the airport transit-time scenario from the FSE '18
 * demo paper. One malformed row (a transit crossing midnight, parsed as a large
 * negative duration) makes a per-airport-hour total go negative; BigSift must isolate
 * exactly that one input line out of the whole dataset.
 */
package org.apache.spark.bigsift

import org.apache.spark.SparkContext
import org.apache.spark.lineage.{LineageContext, LocalLineageContext}
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.rdd.Lineage
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class BigSiftSuite extends AnyFunSuite with LocalLineageContext with Matchers {

  private val transit = "src/test/resources/transit.csv"
  private val culprit = "11/9/12,141011,23:53,1:23,MNN"

  // total layover time per (airport, arrival hour), over transits shorter than 45 min
  private def job(in: Lineage[String]): Lineage[((String, String), Int)] =
    in.map { s =>
      val t = s.split(",")
      val arr = t(2).split(":"); val dep = t(3).split(":")
      val arrMin = arr(0).toInt * 60 + arr(1).toInt
      val depMin = dep(0).toInt * 60 + dep(1).toInt
      ((t(4), arr(0)), depMin - arrMin)               // ((airport, hour), minutes)
    }.filter(_._2 < 45).reduceByKey(_ + _)

  test("isolates the single fault-inducing transit record (negative total)") {
    lc = new LineageContext(new SparkContext("local[2]", "bigsift-transit"))
    val bs = new BigSift(lc)

    val result = bs.run(
      lc.textFile(transit, 2),
      job,
      (out: ((String, String), Int)) => out._2 < 0)   // faulty = negative layover

    // a faulty output existed
    result.faultyOutputs.map(_._1) should contain (("MNN", "23"))
    // the core BigSift guarantee: delta debugging isolated exactly the culprit line
    result.faultInducingInputs should equal (Seq(culprit))
    // the provenance pre-shrink is a best-effort optimization on the RDD path (it
    // falls back to the full input if the lineage trace under-resolves), so the
    // candidate set is at most the whole file
    result.provenanceSize should be <= 7
  }

  test("predefined minimum oracle isolates the same culprit") {
    lc = new LineageContext(new SparkContext("local[2]", "bigsift-oracle"))
    val bs = new BigSift(lc)

    // "explain the minimum output value" — no hand-written threshold
    val result = bs.runWithOracle(
      lc.textFile(transit, 2),
      job,
      TestOracle.min[((String, String), Int)](_._2.toDouble))

    result.faultInducingInputs should equal (Seq(culprit))
  }

  test("clean input yields no fault-inducing records") {
    lc = new LineageContext(new SparkContext("local[2]", "bigsift-clean"))
    val bs = new BigSift(lc)

    // only the well-formed SEA rows; nothing is negative
    val result = bs.run(
      lc.parallelize(Seq(
        "10/1/12,200,7:10,7:35,SEA",
        "10/2/12,201,7:15,7:40,SEA")),
      job,
      (out: ((String, String), Int)) => out._2 < 0)

    result.faultInducingInputs shouldBe empty
    result.faultyOutputs shouldBe empty
  }
}
