package edu.vt.bigdebug.examples

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._

/**
 * Titian demo with TWO data sources and a join, on stock Spark 4.
 *
 * Joins orders with customers, flags the implausible joined record, and traces it
 * backward through the join to the culprit lines in BOTH input files.
 *
 * Run with: bin/sbt 'examples/run-main edu.vt.bigdebug.examples.OrderCustomerJoin'
 */
object OrderCustomerJoin {

  def main(args: Array[String]): Unit = {
    val ordersPath = if (args.length > 0) args(0) else "data/orders.txt"
    val customersPath = if (args.length > 1) args(1) else "data/customers.txt"

    val sc = new SparkContext(
      new SparkConf().setMaster("local[2]").setAppName("titian-join-demo"))
    sc.setLogLevel("WARN")
    val lc = new LineageContext(sc)

    // ---- Run the join with lineage capture on ------------------------------------
    lc.setCaptureLineage(true)

    // Source 1: orders.txt — "orderId,customerId,amount", keyed by customerId
    val orders = lc.textFile(ordersPath, 2).map { line =>
      val Array(orderId, customerId, amount) = line.split(",")
      (customerId, (orderId, amount.toInt))
    }

    // Source 2: customers.txt — "customerId,name", keyed by customerId
    val customers = lc.textFile(customersPath, 2).map { line =>
      val Array(customerId, name) = line.split(",")
      (customerId, name)
    }

    val joined = orders.join(customers)

    val output = joined.collectWithId()

    lc.setCaptureLineage(false)

    println("\n=== Joined records: (customer, ((orderId, amount), name)) ===")
    output.foreach { case (record, _) => println(record) }

    // ---- Backward trace: find the absurd order and walk back through the join ----
    val (suspect, suspectId) = output.maxBy(_._1._2._1._2)
    println(s"\nSuspicious joined record: $suspect — tracing backward...\n")

    // A join has two parents; goBack(path = i) follows the i-th input of the join.
    def traceBranch(path: Int): Array[String] = {
      var lineage = joined.getLineage()
      lineage = lineage.filter(_ == suspectId)
      lineage = lineage.goBack()     // result tap   -> post-cogroup tap
      lineage = lineage.goBack(path) // post-cogroup -> pre-cogroup tap of input #path
      lineage = lineage.goBack()     // pre-cogroup  -> that input file
      lineage.show().collect()
    }

    println("=== Culprit line in orders.txt (join input 0) ===")
    traceBranch(0).foreach(println)

    println("\n=== Culprit line in customers.txt (join input 1) ===")
    traceBranch(1).foreach(println)

    sc.stop()
  }
}
