package edu.vt.bigdebug.examples

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._

/**
 * Small end-to-end Titian demo on stock Spark 4.
 *
 * Sums sales per category, spots the implausible total, and uses record-level lineage
 * to trace it back to the exact corrupt input line — then traces forward from that line
 * to every output it influenced.
 *
 * Run with: bin/sbt 'examples/run examples/data/sales.txt'
 */
object SalesAnalysis {

  def main(args: Array[String]): Unit = {
    val input = args.headOption.getOrElse("examples/data/sales.txt")

    val sc = new SparkContext(
      new SparkConf().setMaster("local[2]").setAppName("titian-sales-demo"))
    sc.setLogLevel("WARN")
    val lc = new LineageContext(sc)

    // ---- Run the job with lineage capture on -------------------------------------
    lc.setCaptureLineage(true)

    val lines = lc.textFile(input, 2)
    val sales = lines.map { line =>
      val parts = line.split(",")
      (parts(0), parts(1).toInt)
    }
    val totals = sales.reduceByKey(_ + _)

    val output = totals.collectWithId()

    lc.setCaptureLineage(false)

    println("\n=== Totals per category ===")
    output.foreach { case ((category, total), _) => println(f"$category%-12s $total%7d") }

    // ---- Backward trace: who produced the absurd total? --------------------------
    val (suspect, suspectId) = output.maxBy(_._1._2)
    println(s"\nSuspicious result: $suspect — tracing backward to its inputs...\n")

    var lineage = totals.getLineage()
    lineage = lineage.filter(_ == suspectId)
    lineage = lineage.goBack() // post-shuffle -> pre-shuffle
    lineage = lineage.goBack() // pre-shuffle  -> input file
    val culpritLines = lineage.show().collect()

    println("=== Input lines behind that result ===")
    culpritLines.foreach(println)
    val corrupt = culpritLines.maxBy(_.split(",")(1).toInt)
    println(s"\n--> corrupt record: '$corrupt'")

    // ---- Forward trace: what else did those lines touch? -------------------------
    println("\n=== Forward trace from those lines (records they influenced) ===")
    lineage = lineage.goNext() // input file -> pre-shuffle contributions
    lineage = lineage.goNext() // pre-shuffle -> final aggregates
    lineage.show().collect().foreach(println)

    sc.stop()
  }
}
