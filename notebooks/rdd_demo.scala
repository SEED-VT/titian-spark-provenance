// Titian RDD-API provenance demo, run by notebooks/titian_rdd.ipynb via spark-shell.
// Captures lineage for a reduceByKey job, traces the corrupt aggregate backward to
// its exact source lines, and verifies the forward trace.
//
// The logic lives inside a method: spark-shell wraps top-level vals in a REPL object,
// and lambdas defined there capture the whole wrapper (including the non-serializable
// LineageContext). Method-local lambdas capture only what they use.

import org.apache.spark.SparkContext
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._

object RddDemo {
  def run(sc: SparkContext): Unit = {
    val root = sys.env.getOrElse("TITIAN_HOME", "..")

    val lc = new LineageContext(sc)
    lc.setCaptureLineage(true)

    val totals = lc.textFile(s"$root/examples/data/sales.txt", 2)
      .map { line =>
        val c = line.indexOf(',')
        (line.substring(0, c), line.substring(c + 1).toInt)
      }
      .reduceByKey(_ + _)
    val output = totals.collectWithId()
    lc.setCaptureLineage(false)

    println("TOTALS: " + output.map(_._1).mkString(", "))
    val suspect = output.maxBy(_._1._2)
    require(suspect._1 == ("electronics", 101109), s"unexpected totals: ${suspect._1}")
    val suspectId: Long = suspect._2

    // backward: aggregate -> pre-shuffle -> input file
    var lin = totals.getLineage()
    lin = lin.filter(_ == suspectId)
    lin = lin.goBack().goBack()
    val witnesses = lin.show().collect()
    witnesses.foreach(w => println("witness: " + w))
    require(witnesses.exists(_.toString.contains("99999")), "corrupt record not found")

    // forward again: those lines influence exactly one aggregate
    val up = lin.goNext().goNext()
    require(up.collect().length == 1, "forward trace should reach one aggregate")
    println("FORWARD TRACE OK")

    println("RDD-DEMO-ALL-OK")
  }
}

RddDemo.run(sc)
