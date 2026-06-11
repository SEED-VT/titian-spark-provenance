/*
 * Phase 2/3 tests: aggregates and joins across exchanges, multi-hop queries, AQE
 * on/off, codegen on/off — complex SQL traced record-level back to source files.
 *
 * Fixtures: sales_parts/ (two CSV files -> two scan partitions; "category,amount"
 * with a planted electronics 99999 outlier), orders_csv/, customers_csv/.
 */
package org.apache.spark.sql.lineage

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SQLLineageOpsSuite extends AnyFunSuite with BeforeAndAfterEach with Matchers {

  @transient private var spark: SparkSession = _

  private val salesDir = "src/test/resources/sales_parts"
  private val ordersDir = "src/test/resources/orders_csv"
  private val customersDir = "src/test/resources/customers_csv"

  private val electronicsRows =
    Set(Row("electronics", 420), Row("electronics", 310),
        Row("electronics", 99999), Row("electronics", 380))

  override def afterEach(): Unit = {
    if (spark != null) { spark.stop(); spark = null }
    System.clearProperty("spark.driver.port")
  }

  private def newSession(adaptive: Boolean, codegen: Boolean = true): SparkSession = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("titian-sql-ops-test")
      .config("spark.sql.extensions", classOf[TitianSQLExtension].getName)
      .config("spark.sql.adaptive.enabled", adaptive.toString)
      .config("spark.sql.adaptive.optimizeSkewedJoin.enabled", "false")
      .config("spark.sql.adaptive.skewJoin.enabled", "false")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.sql.codegen.wholeStage", codegen.toString)
      .config("spark.titian.sql.capture", "true")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }

  private def sales(s: SparkSession): DataFrame =
    s.read.schema("category STRING, amount INT").csv(salesDir)

  private def orders(s: SparkSession): DataFrame =
    s.read.schema("oid STRING, cid STRING, amount INT").csv(ordersDir)

  private def customers(s: SparkSession): DataFrame =
    s.read.schema("cid STRING, name STRING").csv(customersDir)

  private def aggregateRoundTrip(s: SparkSession): Unit = {
    sales(s).createOrReplaceTempView("sales")
    val df = s.sql("SELECT category, SUM(amount) AS total FROM sales GROUP BY category")
    val output = TitianSQL.collectWithLineage(df)
    output.map(_._1).toSet should equal (Set(
      Row("electronics", 101109L), Row("furniture", 865L),
      Row("groceries", 355L), Row("toys", 280L)))

    val electronicsId = output.find(_._1.getString(0) == "electronics").get._2
    var cursor = TitianSQL.trace(df, Seq(electronicsId))
    val topIds = cursor.ids
    cursor = cursor.goBack()
    cursor.atScan should be (true)
    // electronics rows live in BOTH input files (split across scan partitions)
    cursor.show().toSet should equal (electronicsRows)
    cursor.ids should have length 4

    // forward retraces to exactly the electronics aggregate
    val back = cursor.goNext()
    back.ids.toSet should equal (topIds.toSet)
  }

  test("groupBy aggregate over two scan partitions — backward and forward (AQE on)") {
    aggregateRoundTrip(newSession(adaptive = true))
  }

  test("groupBy aggregate over two scan partitions — backward and forward (AQE off)") {
    aggregateRoundTrip(newSession(adaptive = false))
  }

  test("groupBy aggregate — interpreted path (codegen off, AQE on)") {
    aggregateRoundTrip(newSession(adaptive = true, codegen = false))
  }

  test("multi-aggregate with HAVING") {
    val s = newSession(adaptive = true)
    sales(s).createOrReplaceTempView("sales")
    val df = s.sql(
      """SELECT category, SUM(amount) AS total, COUNT(*) AS cnt, MAX(amount) AS mx
        |FROM sales GROUP BY category HAVING SUM(amount) > 300""".stripMargin)
    val output = TitianSQL.collectWithLineage(df)
    // toys (280) filtered out by HAVING
    output.map(_._1).toSet should equal (Set(
      Row("electronics", 101109L, 4L, 99999),
      Row("furniture", 865L, 4L, 250),
      Row("groceries", 355L, 4L, 110)))

    val groceriesId = output.find(_._1.getString(0) == "groceries").get._2
    val cursor = TitianSQL.trace(df, Seq(groceriesId)).goBack()
    cursor.show().toSet should equal (Set(
      Row("groceries", 80), Row("groceries", 95), Row("groceries", 110),
      Row("groceries", 70)))
  }

  test("global aggregate traces to every input row") {
    val s = newSession(adaptive = true)
    sales(s).createOrReplaceTempView("sales")
    val df = s.sql("SELECT SUM(amount) AS total, COUNT(*) AS cnt FROM sales")
    val output = TitianSQL.collectWithLineage(df)
    output.map(_._1) should equal (Array(Row(102609L, 16L)))

    val cursor = TitianSQL.trace(df, Seq(output(0)._2)).goBack()
    cursor.ids should have length 16
    cursor.show() should have length 16
  }

  test("DISTINCT — control-flow witnesses across the exchange") {
    val s = newSession(adaptive = true)
    sales(s).createOrReplaceTempView("sales")
    val df = s.sql("SELECT DISTINCT category FROM sales")
    val output = TitianSQL.collectWithLineage(df)
    output.map(_._1.getString(0)).toSet should equal (
      Set("electronics", "furniture", "groceries", "toys"))

    // A distinct output depends on ALL duplicate witnesses. The scan is
    // column-pruned to `category`, so the four witness records display identically —
    // but they are four distinct records (distinct ids / source rows).
    val electronicsId = output.find(_._1.getString(0) == "electronics").get._2
    val cursor = TitianSQL.trace(df, Seq(electronicsId)).goBack()
    cursor.ids should have length 4
    val witnesses = cursor.show()
    witnesses should have length 4
    witnesses.toSet should equal (Set(Row("electronics")))
  }

  test("filter + projection + expression pipeline under an aggregate") {
    val s = newSession(adaptive = true)
    sales(s).createOrReplaceTempView("sales")
    val df = s.sql(
      """SELECT UPPER(category) AS cat, SUM(amount + 1) AS total
        |FROM sales WHERE amount > 100 GROUP BY UPPER(category)""".stripMargin)
    val output = TitianSQL.collectWithLineage(df)
    // rows with amount > 100: electronics 420/310/99999/380,
    // furniture 250/190/205/220, groceries 110 (SUM(amount + 1) adds 1 per row)
    output.map(_._1).toSet should equal (Set(
      Row("ELECTRONICS", 101113L), Row("FURNITURE", 869L), Row("GROCERIES", 111L)))

    val furnitureId = output.find(_._1.getString(0) == "FURNITURE").get._2
    val cursor = TitianSQL.trace(df, Seq(furnitureId)).goBack()
    cursor.show().toSet should equal (Set(
      Row("furniture", 250), Row("furniture", 190), Row("furniture", 205),
      Row("furniture", 220)))
  }

  private def joinBothBranches(s: SparkSession): Unit = {
    orders(s).createOrReplaceTempView("orders")
    customers(s).createOrReplaceTempView("customers")
    val df = s.sql(
      """SELECT o.oid, o.amount, c.name
        |FROM orders o JOIN customers c ON o.cid = c.cid""".stripMargin)
    val output = TitianSQL.collectWithLineage(df)
    output should have length 12
    val suspect = output.maxBy(_._1.getInt(1))
    suspect._1 should equal (Row("o8", 99999, "Bob"))

    // branch 0 = orders side: all c2 order rows (key-hash granularity)
    val ordersSide = TitianSQL.trace(df, Seq(suspect._2)).goBack(0)
    ordersSide.atScan should be (true)
    ordersSide.show().toSet should equal (Set(
      Row("o2", "c2", 250), Row("o5", "c2", 190),
      Row("o8", "c2", 99999), Row("o11", "c2", 205)))

    // branch 1 = customers side
    val customersSide = TitianSQL.trace(df, Seq(suspect._2)).goBack(1)
    customersSide.show().toSet should equal (Set(Row("c2", "Bob")))
  }

  test("inner join (SMJ) — backward into both sources (AQE on)") {
    joinBothBranches(newSession(adaptive = true))
  }

  test("inner join (SMJ) — backward into both sources (AQE off)") {
    joinBothBranches(newSession(adaptive = false))
  }

  test("join then aggregate — two-hop trace into both sources") {
    val s = newSession(adaptive = true)
    orders(s).createOrReplaceTempView("orders")
    customers(s).createOrReplaceTempView("customers")
    val df = s.sql(
      """SELECT c.name, SUM(o.amount) AS total
        |FROM orders o JOIN customers c ON o.cid = c.cid
        |GROUP BY c.name""".stripMargin)
    val output = TitianSQL.collectWithLineage(df)
    output.map(_._1).toSet should equal (Set(
      Row("Alice", 900L), Row("Bob", 100644L), Row("Carol", 630L)))

    val bobId = output.find(_._1.getString(0) == "Bob").get._2

    // hop 1: aggregate level -> joined-row level; hop 2: join -> orders source
    var cursor = TitianSQL.trace(df, Seq(bobId)).goBack() // through the agg exchange
    cursor.atScan should be (false) // now at the join's keyed level
    val ordersSide = cursor.goBack(0)
    // orders scan is column-pruned to (cid, amount) — oid is never read by this query
    ordersSide.show().toSet should equal (Set(
      Row("c2", 250), Row("c2", 190), Row("c2", 99999), Row("c2", 205)))

    // hop 2 alternative branch: join -> customers source
    val customersSide = cursor.goBack(1)
    customersSide.show().toSet should equal (Set(Row("c2", "Bob")))

    // forward all the way back up: orders rows -> join level -> aggregate level
    val up = ordersSide.goNext().goNext()
    up.ids.toSet should equal (TitianSQL.trace(df, Seq(bobId)).ids.toSet)
  }

  test("non-equi joins (BroadcastNestedLoopJoin) fail loudly") {
    val s = newSession(adaptive = true)
    sales(s).createOrReplaceTempView("sales")
    val df = s.sql(
      "SELECT * FROM sales a JOIN sales b ON a.amount > b.amount")
    val e = intercept[Exception] { df.collect() }
    def causes(t: Throwable): Seq[Throwable] =
      if (t == null) Nil else t +: causes(t.getCause)
    assert(causes(e).exists(_.isInstanceOf[TitianUnsupportedOperatorException]),
      s"expected TitianUnsupportedOperatorException, got: $e")
  }
}
