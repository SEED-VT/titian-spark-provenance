/*
 * Lineage tests for the shuffle operators beyond the original suite: join (cogroup),
 * groupByKey, aggregateByKey, and combineByKey. Assertions are content-based against
 * the fixture files (see LineageSuite header for why constants from the fork's tests
 * are not used).
 *
 * Fixtures:
 *  - sales.txt:     "category,amount" — electronics contains a planted 99999 outlier
 *  - orders.txt:    "orderId,customerId,amount" — o8/c2 carries the 99999 outlier
 *  - customers.txt: "customerId,name"
 */
package org.apache.spark.lineage

import org.apache.spark.SparkContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.rdd.Lineage
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class LineageOpsSuite extends AnyFunSuite with LocalLineageContext with Matchers {

  private val salesFile = "src/test/resources/sales.txt"
  private val ordersFile = "src/test/resources/orders.txt"
  private val customersFile = "src/test/resources/customers.txt"

  private val electronicsLines = Array(
    "electronics,420", "electronics,310", "electronics,99999", "electronics,380")

  test("Join — backward trace reaches both sources") {
    lc = new LineageContext(new SparkContext("local[2]", "joinBackwardTest"))
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

    // 12 orders, each with exactly one matching customer
    output.length should equal (12)
    val (suspect, suspectId) = output.maxBy(_._1._2._1._2)
    suspect should equal (("c2", (("o8", 99999), "Bob")))

    // goBack(path = i) follows join input i
    def traceBranch(path: Int): Set[String] = {
      var lin = joined.getLineage()
      lin = lin.filter(_ == suspectId)
      lin = lin.goBack()     // result tap   -> post-cogroup tap
      lin = lin.goBack(path) // post-cogroup -> pre-cogroup tap of input #path
      lin = lin.goBack()     // pre-cogroup  -> input file
      lin.show().collect().toSet
    }

    // Input 0 (orders): all c2 order lines — key-hash granularity across the shuffle —
    // including the corrupt one
    traceBranch(0) should equal (Set("o2,c2,250", "o5,c2,190", "o8,c2,99999", "o11,c2,205"))

    // Input 1 (customers): the single c2 line
    traceBranch(1) should equal (Set("c2,Bob"))
  }

  test("GroupByKey — backward and forward trace") {
    lc = new LineageContext(new SparkContext("local[2]", "groupByKeyTest"))
    lc.setCaptureLineage(true)

    val pairs = lc.textFile(salesFile, 2).map { line =>
      val Array(category, amount) = line.split(",")
      (category, amount.toInt)
    }
    val grouped = pairs.groupByKey()
    val output = grouped.collectWithId()
    lc.setCaptureLineage(false)

    output.length should equal (4) // electronics, furniture, groceries, toys
    val electronics = output.find(_._1._1 == "electronics").get
    electronics._1._2.toSet should equal (Set(420, 310, 99999, 380))

    // Backward: the group traces to exactly the electronics input lines
    var lin = grouped.getLineage()
    lin = lin.filter(_ == electronics._2)
    lin = lin.goBack()
    lin = lin.goBack()
    lin.show().collect().toSet should equal (electronicsLines.toSet)

    // Forward: those lines flow back to the electronics group (and only it)
    lin = lin.goNext()
    lin = lin.goNext()
    val shown = lin.show().collect()
    shown should have size 1
    shown(0) should startWith ("((electronics,")
    Seq("420", "310", "99999", "380").foreach(v => shown(0) should include (v))
  }

  test("AggregateByKey — backward and forward trace") {
    lc = new LineageContext(new SparkContext("local[2]", "aggregateByKeyTest"))
    lc.setCaptureLineage(true)

    val pairs = lc.textFile(salesFile, 2).map { line =>
      val Array(category, amount) = line.split(",")
      (category, amount.toInt)
    }
    val totals = pairs.aggregateByKey(0)(_ + _, _ + _)
    val output = totals.collectWithId()
    lc.setCaptureLineage(false)

    output.map(_._1).toSet should equal (Set(
      ("electronics", 101109), ("furniture", 865), ("groceries", 355), ("toys", 280)))

    val suspectId = output.find(_._1._1 == "electronics").get._2

    // Backward: the absurd total comes from exactly the electronics lines
    var lin = totals.getLineage()
    lin = lin.filter(_ == suspectId)
    lin = lin.goBack()
    lin = lin.goBack()
    lin.show().collect().toSet should equal (electronicsLines.toSet)

    // Forward: those lines influenced only the electronics total
    lin = lin.goNext()
    lin = lin.goNext()
    val shown = lin.show().collect()
    shown should have size 1
    shown(0) should startWith ("((electronics,101109)")
  }

  test("Distinct — control-flow provenance returns all duplicate witnesses") {
    lc = new LineageContext(new SparkContext("local[2]", "distinctTest"))
    lc.setCaptureLineage(true)

    // fruits.txt: 8 lines, "apple" x3 (split across both partitions), "banana" x2,
    // "cherry" x2, "date" x1
    val lines = lc.textFile("src/test/resources/fruits.txt", 2)
    val unique = lines.distinct()
    val output = unique.collectWithId()
    lc.setCaptureLineage(false)

    output.map(_._1).toSet should equal (Set("apple", "banana", "cherry", "date"))

    // A distinct output's existence depends on ALL its duplicates (control-flow
    // dependency): pre-shuffle capture records every input record before the combiner
    // collapses them, so the backward trace must surface every witness, not just the
    // surviving representative.
    val appleId = output.find(_._1 == "apple").get._2
    var lin = unique.getLineage()
    lin = lin.filter(_ == appleId)
    lin = lin.goBack() // final tap    -> post-shuffle
    lin = lin.goBack() // post-shuffle -> pre-shuffle
    lin = lin.goBack() // pre-shuffle  -> input file
    // Record-level: three distinct witness records (one per source line). show()
    // deduplicates identical display strings per partition (fork behavior, ShowRDD
    // collect), so assert on dump — the raw (byteOffset, line) pairs.
    lin.collect() should have length 3
    val witnesses = lin.show().asInstanceOf[org.apache.spark.lineage.rdd.ShowRDD[Long]]
      .dump.collect()
    witnesses should have length 3
    witnesses.map(_._1).distinct should have length 3 // three different source offsets
    witnesses.map(_._2).toSet should equal (Set("apple"))

    val dateId = output.find(_._1 == "date").get._2
    var lin2 = unique.getLineage()
    lin2 = lin2.filter(_ == dateId)
    lin2 = lin2.goBack()
    lin2 = lin2.goBack()
    lin2 = lin2.goBack()
    lin2.show().collect() should equal (Array("date"))
  }

  test("CombineByKey — backward trace") {
    lc = new LineageContext(new SparkContext("local[2]", "combineByKeyTest"))
    lc.setCaptureLineage(true)

    val pairs = lc.textFile(salesFile, 2).map { line =>
      val Array(category, amount) = line.split(",")
      (category, amount.toInt)
    }
    // (count, sum) per category — classic average-building combiner. The 3-arg
    // combineByKey is the stock signature (typed RDD); it dispatches through the
    // overridden combineByKeyWithClassTag, so the result is a Lineage at runtime.
    val stats = pairs.combineByKey(
      (v: Int) => (1, v),
      (acc: (Int, Int), v: Int) => (acc._1 + 1, acc._2 + v),
      (a: (Int, Int), b: (Int, Int)) => (a._1 + b._1, a._2 + b._2))
      .asInstanceOf[Lineage[(String, (Int, Int))]]
    val output = stats.collectWithId()
    lc.setCaptureLineage(false)

    output.map(_._1).toSet should equal (Set(
      ("electronics", (4, 101109)), ("furniture", (4, 865)),
      ("groceries", (4, 355)), ("toys", (4, 280))))

    val suspectId = output.find(_._1._1 == "electronics").get._2

    var lin = stats.getLineage()
    lin = lin.filter(_ == suspectId)
    lin = lin.goBack()
    lin = lin.goBack()
    lin.show().collect().toSet should equal (electronicsLines.toSet)
  }
}
