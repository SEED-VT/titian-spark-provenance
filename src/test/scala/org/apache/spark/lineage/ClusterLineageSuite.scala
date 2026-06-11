/*
 * Runs lineage capture and tracing on `local-cluster` mode: a real standalone master
 * plus separate executor JVM processes. Unlike local[n], this exercises closure and
 * lineage-data serialization across JVMs, executor classpath distribution, remote
 * BlockManager fetches of materialized tap blocks, and the locality-aware trace joins —
 * the things that differ between a laptop run and a real cluster.
 *
 * Also asserts the structure of Titian's distributed selective join (VLDB '16 §4):
 * the bulk lineage tables are joined via zipPartitions (narrow — never reshuffled);
 * only the small trace cursor is routed, by the partition id packed in each record id
 * (LocalityAwarePartitioner).
 */
package org.apache.spark.lineage

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.rdd.RDD
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ClusterLineageSuite extends AnyFunSuite with LocalLineageContext with Matchers {

  private val salesFile = new java.io.File("src/test/resources/sales.txt").getAbsolutePath
  private val ordersFile = new java.io.File("src/test/resources/orders.txt").getAbsolutePath
  private val customersFile =
    new java.io.File("src/test/resources/customers.txt").getAbsolutePath

  private val electronicsLines = Set(
    "electronics,420", "electronics,310", "electronics,99999", "electronics,380")

  private val moduleOpens = Seq(
    "java.base/java.lang", "java.base/java.lang.invoke", "java.base/java.lang.reflect",
    "java.base/java.io", "java.base/java.net", "java.base/java.nio",
    "java.base/java.util", "java.base/java.util.concurrent",
    "java.base/java.util.concurrent.atomic", "java.base/jdk.internal.ref",
    "java.base/sun.nio.ch", "java.base/sun.nio.cs", "java.base/sun.security.action",
    "java.base/sun.util.calendar"
  ).map(p => s"--add-opens=$p=ALL-UNNAMED").mkString(" ")

  private def newClusterContext(name: String): LineageContext = {
    val conf = new SparkConf()
      .setMaster("local-cluster[2,1,1024]")
      .setAppName(name)
      // Executors are separate JVMs: hand them the test classpath and module opens.
      .set("spark.executor.extraClassPath", System.getProperty("java.class.path"))
      .set("spark.executor.extraJavaOptions", moduleOpens)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    new LineageContext(sc)
  }

  /** All transitive ancestor RDDs of `rdd` in its (trace-time) DAG. */
  private def ancestors(rdd: RDD[_]): Seq[RDD[_]] = {
    val seen = scala.collection.mutable.LinkedHashSet[RDD[_]]()
    def visit(r: RDD[_]): Unit = r.dependencies.map(_.rdd).foreach { p =>
      if (seen.add(p)) visit(p)
    }
    visit(rdd)
    seen.toSeq
  }

  test("AggregateByKey on local-cluster — capture and bidirectional trace") {
    lc = newClusterContext("clusterAggregateTest")
    lc.setCaptureLineage(true)

    val pairs = lc.textFile(salesFile, 4).map { line =>
      val Array(category, amount) = line.split(",")
      (category, amount.toInt)
    }
    val totals = pairs.aggregateByKey(0)(_ + _, _ + _)
    val output = totals.collectWithId()
    lc.setCaptureLineage(false)

    output.map(_._1).toSet should equal (Set(
      ("electronics", 101109), ("furniture", 865), ("groceries", 355), ("toys", 280)))

    val suspectId = output.find(_._1._1 == "electronics").get._2

    var lin = totals.getLineage()
    lin = lin.filter(_ == suspectId)
    lin = lin.goBack()

    // Distributed selective join, structurally: the lineage table is consumed through
    // zipPartitions (narrow), with only the cursor routed by LocalityAwarePartitioner.
    val traceAncestors = ancestors(lin)
    assert(traceAncestors.exists(_.getClass.getName.contains("ZippedPartitions")),
      "trace join should zipPartitions the lineage table, not shuffle it")

    lin = lin.goBack()
    lin.show().collect().toSet should equal (electronicsLines)

    lin = lin.goNext()
    lin = lin.goNext()
    val shown = lin.show().collect()
    shown should have size 1
    shown(0) should startWith ("((electronics,101109)")
  }

  test("Join on local-cluster — backward trace reaches both sources") {
    lc = newClusterContext("clusterJoinTest")
    lc.setCaptureLineage(true)

    val orders = lc.textFile(ordersFile, 2).map { line =>
      val Array(orderId, customerId, amount) = line.split(",")
      (customerId, (orderId, amount.toInt))
    }
    val customers = lc.textFile(customersFile, 2).map { line =>
      val Array(customerId, name) = line.split(",")
      (customerId, name)
    }
    val joined = orders.join(customers)
    val output = joined.collectWithId()
    lc.setCaptureLineage(false)

    output.length should equal (12)
    val (suspect, suspectId) = output.maxBy(_._1._2._1._2)
    suspect should equal (("c2", (("o8", 99999), "Bob")))

    def traceBranch(path: Int): Set[String] = {
      var lin = joined.getLineage()
      lin = lin.filter(_ == suspectId)
      lin = lin.goBack()
      lin = lin.goBack(path)
      lin = lin.goBack()
      lin.show().collect().toSet
    }

    traceBranch(0) should equal (
      Set("o2,c2,250", "o5,c2,190", "o8,c2,99999", "o11,c2,205"))
    traceBranch(1) should equal (Set("c2,Bob"))
  }
}
