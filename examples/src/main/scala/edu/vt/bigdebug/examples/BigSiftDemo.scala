package edu.vt.bigdebug.examples

import org.apache.spark.SparkContext
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.rdd.Lineage
import org.apache.spark.bigsift.{BigSift, TestOracle}

/**
 * BigSift demo (the FSE '18 airport scenario): total layover time per airport-hour for
 * transits under 45 minutes. One malformed row (a transit crossing midnight, parsed as
 * a large negative duration) drives a total negative; BigSift isolates that one row.
 *
 *   bin/sbt 'examples/runMain edu.vt.bigdebug.examples.BigSiftDemo'
 */
object BigSiftDemo {

  def job(in: Lineage[String]): Lineage[((String, String), Int)] =
    in.map { s =>
      val t = s.split(",")
      val arr = t(2).split(":"); val dep = t(3).split(":")
      ((t(4), arr(0)), (dep(0).toInt * 60 + dep(1).toInt) - (arr(0).toInt * 60 + arr(1).toInt))
    }.filter(_._2 < 45).reduceByKey(_ + _)

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "bigsift-demo")
    sc.setLogLevel("ERROR")
    val lc = new LineageContext(sc)
    val data = "src/test/resources/transit.csv"

    println("=== BigSift: explain the negative layover total ===")
    val r1 = new BigSift(lc).run(
      lc.textFile(data, 2), job,
      (out: ((String, String), Int)) => out._2 < 0)
    println(s"faulty outputs:  ${r1.faultyOutputs.mkString(", ")}")
    println(s"candidates (provenance): ${r1.provenanceSize} rows")
    println(s"fault-inducing input(s): ${r1.faultInducingInputs.mkString(" | ")}")

    println("\n=== BigSift with a predefined 'minimum' oracle ===")
    val r2 = new BigSift(lc).runWithOracle(
      lc.textFile(data, 2), job,
      TestOracle.min[((String, String), Int)](_._2.toDouble))
    println(s"fault-inducing input(s): ${r2.faultInducingInputs.mkString(" | ")}")

    sc.stop()
  }
}
