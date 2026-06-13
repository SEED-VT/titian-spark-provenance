/*
 * ORDER BY ... LIMIT (TakeOrderedAndProject) and UNION ALL coverage — the two
 * extensions that unlock the TPC-DS query shapes (nearly every TPC-DS query ends in
 * ORDER BY ... LIMIT 100, and many union fact-table subqueries).
 *
 * TakeOrderedAndProject is captured as a keyed boundary (pre tap on the sort
 * expressions below, post tap on the same columns in the output above): key-hash
 * granularity, exact when sort keys are unique. UNION ALL needs no tap — each task
 * runs one child's partition, so ids thread through; the trace graph routes
 * goBack(branch) by stage-partition range and re-scans translate partitions back.
 */
package org.apache.spark.sql.lineage

import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SQLLineageLimitUnionSuite extends AnyFunSuite with BeforeAndAfterEach
  with Matchers {

  @transient private var spark: SparkSession = _

  private val salesDir = "src/test/resources/sales_parts"

  override def afterEach(): Unit = {
    if (spark != null) { spark.stop(); spark = null }
    System.clearProperty("spark.driver.port")
  }

  private def newSession(adaptive: Boolean = true): SparkSession = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("titian-sql-limit-union-test")
      .config("spark.sql.extensions", classOf[TitianSQLExtension].getName)
      .config("spark.sql.adaptive.enabled", adaptive.toString)
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.titian.sql.capture", "true")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.read.schema("category STRING, amount INT").csv(salesDir)
      .createOrReplaceTempView("sales")
    spark
  }

  // sales totals: electronics 101109, furniture 865, groceries 355, toys 280

  test("ORDER BY ... LIMIT over an aggregate — captured and traced to sources") {
    val s = newSession()
    val df = s.sql(
      """SELECT category, SUM(amount) AS total FROM sales
        |GROUP BY category ORDER BY total DESC LIMIT 2""".stripMargin)
    val output = TitianSQL.collectWithLineage(df)
    assert(df.queryExecution.executedPlan.toString.contains("TakeOrderedAndProject"),
      s"expected TakeOrderedAndProject:\n${df.queryExecution.executedPlan}")
    output.map(_._1).toSeq should equal (
      Seq(Row("electronics", 101109), Row("furniture", 865)))

    // backward: through the order-by-limit boundary, then the aggregate's shuffle
    var cursor = TitianSQL.trace(df, Seq(output.head._2))
    cursor = cursor.goBack() // limit level -> aggregate level
    cursor = cursor.goBack() // aggregate level -> scan
    cursor.atScan should be (true)
    val witnesses = cursor.show()
    witnesses should have length 4
    witnesses.map(_.getInt(1)).sum should equal (101109)

    // forward retrace ends at exactly the one surviving result row
    cursor.goNext().goNext().ids should have length 1
  }

  test("ORDER BY on a column pruned from the output — carried through and stripped") {
    val s = newSession()
    // amount is the sort key but not selected: the tap pair carries it through the
    // limit's projection and a Project above strips it — schema unchanged
    val df = s.sql("SELECT category FROM sales ORDER BY amount LIMIT 3")
    val output = TitianSQL.collectWithLineage(df)
    output.map(_._1) should equal (Array(Row("toys"), Row("toys"), Row("groceries")))

    // smallest amounts: 55, 60, 70 — sort keys are unique, so the trace is exact
    val cursor = TitianSQL.trace(df, Seq(output.head._2)).goBack()
    cursor.atScan should be (true)
    cursor.show().toSeq should equal (Seq(Row("toys", 55)))
  }

  test("UNION ALL at the result — branches route to the correct scans") {
    val s = newSession()
    val df = s.sql(
      """SELECT category, amount FROM sales WHERE amount > 300
        |UNION ALL
        |SELECT category, amount FROM sales WHERE amount < 70""".stripMargin)
    val output = TitianSQL.collectWithLineage(df)
    output should have length 6 // 420,310,99999,380 | 60,55

    val big = output.find(_._1.getInt(1) == 99999).get
    val viaFirst = TitianSQL.trace(df, Seq(big._2)).goBack(0)
    viaFirst.show().toSeq should equal (Seq(Row("electronics", 99999)))
    // the row belongs to branch 0: branch 1 holds none of its ids
    TitianSQL.trace(df, Seq(big._2)).goBack(1).ids shouldBe empty

    val small = output.find(_._1.getInt(1) == 55).get
    val viaSecond = TitianSQL.trace(df, Seq(small._2)).goBack(1)
    viaSecond.show().toSeq should equal (Seq(Row("toys", 55)))
    TitianSQL.trace(df, Seq(small._2)).goBack(0).ids shouldBe empty
  }

  test("UNION ALL under an aggregate — partition offsets translate on re-scan") {
    val s = newSession()
    val df = s.sql(
      """SELECT category, SUM(amount) AS total FROM (
        |  SELECT category, amount FROM sales
        |  UNION ALL
        |  SELECT category, amount FROM sales
        |) GROUP BY category""".stripMargin)
    val output = TitianSQL.collectWithLineage(df)
    val electronics = output.find(_._1.getString(0) == "electronics").get
    electronics._1.getLong(1) should equal (2L * 101109)

    // aggregate level -> union level; both branches resolve the same 4 source rows,
    // branch 1 only via the partition-offset translation (its rows recorded with
    // stage partitions shifted past branch 0's)
    val union = TitianSQL.trace(df, Seq(electronics._2)).goBack()
    val first = union.goBack(0)
    val second = union.goBack(1)
    first.show() should have length 4
    second.show() should have length 4
    first.show().map(_.getInt(1)).sorted should equal (
      second.show().map(_.getInt(1)).sorted)
    first.show().map(_.getInt(1)).sum should equal (101109)

    // forward from a union branch retraces to the single aggregate row
    first.goNext().goNext().ids should have length 1
  }

  test("GROUP BY over a union of identically-grouped aggregates — fused pair tapped") {
    val s = newSession()
    // the inner aggregates already partition by category, so Spark fuses the outer
    // aggregate (partial+final, no shuffle) — the TPC-DS q33/q56/q60/q66 shape that
    // silently produced stale ids before the fused-pair taps
    val df = s.sql(
      """SELECT category, SUM(t) AS total FROM (
        |  SELECT category, SUM(amount) AS t FROM sales GROUP BY category
        |  UNION ALL
        |  SELECT category, SUM(amount) AS t FROM sales GROUP BY category
        |) GROUP BY category ORDER BY total DESC LIMIT 2""".stripMargin)
    val output = TitianSQL.collectWithLineage(df)
    output.map(_._1).toSeq should equal (
      Seq(Row("electronics", 2L * 101109), Row("furniture", 2L * 865)))

    // limit -> outer aggregate; the fused aggregate's branches are its per-union-
    // child pre taps, so branch i walks into union child i down to the exact
    // electronics source rows. Before the fused-pair taps this silently walked
    // into the wrong group entirely.
    val agg = TitianSQL.trace(df, Seq(output.head._2)).goBack()
    Seq(0, 1).foreach { branch =>
      var cursor = agg.goBack(branch)
      while (!cursor.atScan) { cursor = cursor.goBack() }
      val witnesses = cursor.show()
      witnesses should have length 4
      witnesses.map(_.getInt(1)).sum should equal (101109)
      witnesses.foreach(_.getString(0) should equal ("electronics"))
    }
  }

  test("UNION ALL + aggregate + ORDER BY LIMIT — the TPC-DS result shape end-to-end") {
    val s = newSession()
    val df = s.sql(
      """SELECT category, SUM(amount) AS total FROM (
        |  SELECT category, amount FROM sales
        |  UNION ALL
        |  SELECT category, amount FROM sales
        |) GROUP BY category ORDER BY total DESC LIMIT 2""".stripMargin)
    val output = TitianSQL.collectWithLineage(df)
    output.map(_._1).toSeq should equal (
      Seq(Row("electronics", 2L * 101109), Row("furniture", 2L * 865)))

    var cursor = TitianSQL.trace(df, Seq(output.head._2))
    cursor = cursor.goBack() // limit -> aggregate
    cursor = cursor.goBack() // aggregate -> union
    val witnesses = cursor.goBack(1).show() // second union branch
    witnesses should have length 4
    witnesses.map(_.getInt(1)).sum should equal (101109)

    // full forward retrace: scan -> union -> aggregate -> limit level
    cursor.goBack(1).goNext().goNext().goNext().ids should have length 1
  }
}
