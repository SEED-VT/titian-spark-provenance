/*
 * Phase 4 tests: broadcast joins (exact probe provenance), outer joins, explode
 * (Generate), window functions (peer-set granularity), count-distinct chains, and
 * loud failure for the still-unsupported rollup grouping-id shape.
 *
 * Fixtures: sales_parts/, customers_csv/, orders2_csv/ (orders plus an unmatched
 * o13,c9,42 row).
 */
package org.apache.spark.sql.lineage

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SQLLineagePhase4Suite extends AnyFunSuite with BeforeAndAfterEach with Matchers {

  @transient private var spark: SparkSession = _

  private val salesDir = "src/test/resources/sales_parts"
  private val orders2Dir = "src/test/resources/orders2_csv"
  private val customersDir = "src/test/resources/customers_csv"

  override def afterEach(): Unit = {
    if (spark != null) { spark.stop(); spark = null }
    System.clearProperty("spark.driver.port")
  }

  private def newSession(broadcast: Boolean, adaptive: Boolean = true): SparkSession = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("titian-sql-phase4-test")
      .config("spark.sql.extensions", classOf[TitianSQLExtension].getName)
      .config("spark.sql.adaptive.enabled", adaptive.toString)
      .config("spark.sql.adaptive.optimizeSkewedJoin.enabled", "false")
      .config("spark.sql.adaptive.skewJoin.enabled", "false")
      .config("spark.sql.autoBroadcastJoinThreshold", if (broadcast) "10MB" else "-1")
      .config("spark.titian.sql.capture", "true")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }

  private def registerViews(s: SparkSession): Unit = {
    s.read.schema("category STRING, amount INT").csv(salesDir)
      .createOrReplaceTempView("sales")
    s.read.schema("oid STRING, cid STRING, amount INT").csv(orders2Dir)
      .createOrReplaceTempView("orders")
    s.read.schema("cid STRING, name STRING").csv(customersDir)
      .createOrReplaceTempView("customers")
  }

  test("broadcast hash join — exact probe-row provenance, both branches") {
    val s = newSession(broadcast = true)
    registerViews(s)
    val df = s.sql(
      """SELECT o.oid, o.amount, c.name
        |FROM orders o JOIN customers c ON o.cid = c.cid""".stripMargin)
    // sanity: the small customers side must actually broadcast
    df.collect()
    assert(df.queryExecution.executedPlan.toString.contains("BroadcastHashJoin"),
      s"expected a broadcast join:\n${df.queryExecution.executedPlan}")

    val output = TitianSQL.collectWithLineage(df)
    output should have length 12
    val suspect = output.maxBy(_._1.getInt(1))
    suspect._1 should equal (Row("o8", 99999, "Bob"))

    // probe side (orders = logical left): EXACT single-row provenance — not the
    // whole c2 key group
    val ordersSide = TitianSQL.trace(df, Seq(suspect._2)).goBack(0)
    ordersSide.atScan should be (true)
    ordersSide.show().toSeq should equal (Seq(Row("o8", "c2", 99999)))

    // build side (customers): key-hash granularity through the broadcast
    val customersSide = TitianSQL.trace(df, Seq(suspect._2)).goBack(1)
    customersSide.show().toSet should equal (Set(Row("c2", "Bob")))
  }

  test("broadcast LEFT OUTER join — unmatched probe row traces to itself, empty build") {
    val s = newSession(broadcast = true)
    registerViews(s)
    val df = s.sql(
      """SELECT o.oid, o.amount, c.name
        |FROM orders o LEFT JOIN customers c ON o.cid = c.cid""".stripMargin)
    val output = TitianSQL.collectWithLineage(df)
    output should have length 13
    val orphan = output.find(_._1.getString(0) == "o13").get
    orphan._1 should equal (Row("o13", 42, null))

    val probe = TitianSQL.trace(df, Seq(orphan._2)).goBack(0)
    probe.show().toSeq should equal (Seq(Row("o13", "c9", 42)))

    val build = TitianSQL.trace(df, Seq(orphan._2)).goBack(1)
    build.ids shouldBe empty
  }

  test("sort-merge LEFT OUTER join — unmatched row, both branches") {
    val s = newSession(broadcast = false)
    registerViews(s)
    val df = s.sql(
      """SELECT o.oid, o.amount, c.name
        |FROM orders o LEFT JOIN customers c ON o.cid = c.cid""".stripMargin)
    val output = TitianSQL.collectWithLineage(df)
    output should have length 13
    val orphan = output.find(_._1.getString(0) == "o13").get

    val ordersSide = TitianSQL.trace(df, Seq(orphan._2)).goBack(0)
    ordersSide.show().toSet should equal (Set(Row("o13", "c9", 42)))

    val customersSide = TitianSQL.trace(df, Seq(orphan._2)).goBack(1)
    customersSide.ids shouldBe empty
  }

  test("explode (Generate) under an aggregate — threads exactly") {
    val s = newSession(broadcast = true)
    registerViews(s)
    val df = s.sql(
      """SELECT w, COUNT(*) AS cnt FROM (
        |  SELECT explode(array(category, category)) AS w FROM sales
        |) GROUP BY w""".stripMargin)
    val output = TitianSQL.collectWithLineage(df)
    output.map(_._1).toSet should equal (Set(
      Row("electronics", 8L), Row("furniture", 8L),
      Row("groceries", 8L), Row("toys", 8L)))

    val electronicsId = output.find(_._1.getString(0) == "electronics").get._2
    val cursor = TitianSQL.trace(df, Seq(electronicsId)).goBack()
    // 2 exploded copies per row collapse back to the 4 source records
    cursor.ids should have length 4
    cursor.show().toSet should equal (Set(Row("electronics")))
  }

  test("window function — peer-set (window partition) granularity") {
    val s = newSession(broadcast = true)
    registerViews(s)
    val df = s.sql(
      """SELECT category, amount,
        |       RANK() OVER (PARTITION BY category ORDER BY amount) AS r
        |FROM sales""".stripMargin)
    val output = TitianSQL.collectWithLineage(df)
    output should have length 16
    val top = output.find(r => r._1.getInt(1) == 99999).get
    top._1 should equal (Row("electronics", 99999, 4))

    // backward: all peers of the window partition (Titian key-granularity)
    val cursor = TitianSQL.trace(df, Seq(top._2)).goBack()
    cursor.atScan should be (true)
    cursor.show().toSet should equal (Set(
      Row("electronics", 420), Row("electronics", 310),
      Row("electronics", 99999), Row("electronics", 380)))
  }

  test("COUNT(DISTINCT) two-level aggregate chain") {
    val s = newSession(broadcast = true)
    registerViews(s)
    val df = s.sql("SELECT COUNT(DISTINCT category) AS c FROM sales")
    val output = TitianSQL.collectWithLineage(df)
    output.map(_._1) should equal (Array(Row(4L)))

    // result -> global count level -> per-category distinct level -> scan
    var cursor = TitianSQL.trace(df, Seq(output(0)._2)).goBack()
    cursor.atScan should be (false)
    cursor = cursor.goBack()
    cursor.atScan should be (true)
    cursor.ids should have length 16
  }

  test("ROLLUP — per-category and grand-total rows trace correctly") {
    val s = newSession(broadcast = true)
    registerViews(s)
    val df = s.sql(
      "SELECT category, SUM(amount) AS total FROM sales GROUP BY ROLLUP(category)")
    val output = TitianSQL.collectWithLineage(df)
    output should have length 5 // 4 categories + grand total

    // grand total (category NULL) depends on every input row
    val totalId = output.find(_._1.isNullAt(0)).get._2
    val totalCursor = TitianSQL.trace(df, Seq(totalId)).goBack()
    totalCursor.ids should have length 16

    // a category row traces to exactly its category's inputs
    val electronicsId = output.find(r =>
      !r._1.isNullAt(0) && r._1.getString(0) == "electronics").get._2
    val catCursor = TitianSQL.trace(df, Seq(electronicsId)).goBack()
    catCursor.ids should have length 4
  }

  test("Parquet (columnar) scan — taps fused at the ColumnarToRow boundary") {
    val s = newSession(broadcast = true)
    registerViews(s)
    val pqDir = "/tmp/titian-test-sales-parquet"
    s.conf.set("spark.titian.sql.capture", "false")
    s.sql("SELECT * FROM sales").write.mode("overwrite").parquet(pqDir)
    s.read.parquet(pqDir).createOrReplaceTempView("sales_pq")
    s.conf.set("spark.titian.sql.capture", "true")

    val df = s.sql(
      "SELECT category, SUM(amount) AS total FROM sales_pq GROUP BY category")
    val output = TitianSQL.collectWithLineage(df)
    output.map(_._1).toSet should equal (Set(
      Row("electronics", 101109L), Row("furniture", 865L),
      Row("groceries", 355L), Row("toys", 280L)))

    val electronicsId = output.find(_._1.getString(0) == "electronics").get._2
    val cursor = TitianSQL.trace(df, Seq(electronicsId)).goBack()
    cursor.atScan should be (true)
    cursor.show().toSet should equal (Set(
      Row("electronics", 420), Row("electronics", 310),
      Row("electronics", 99999), Row("electronics", 380)))
  }

  test("cached DataFrame — cache is a source boundary, witnesses are cached rows") {
    val s = newSession(broadcast = true)
    registerViews(s)
    s.conf.set("spark.titian.sql.capture", "false")
    val cached = s.sql("SELECT * FROM sales").cache()
    cached.count() // materialize the cache without capture
    cached.createOrReplaceTempView("sales_cached")
    s.conf.set("spark.titian.sql.capture", "true")

    val df = s.sql(
      "SELECT category, SUM(amount) AS total FROM sales_cached GROUP BY category")
    val output = TitianSQL.collectWithLineage(df)
    output.map(_._1).toSet should equal (Set(
      Row("electronics", 101109L), Row("furniture", 865L),
      Row("groceries", 355L), Row("toys", 280L)))

    val electronicsId = output.find(_._1.getString(0) == "electronics").get._2
    val cursor = TitianSQL.trace(df, Seq(electronicsId)).goBack()
    cursor.atScan should be (true)
    cursor.show().toSet should equal (Set(
      Row("electronics", 420), Row("electronics", 310),
      Row("electronics", 99999), Row("electronics", 380)))
  }

  test("df.show() / LIMIT queries skip capture instead of failing") {
    val s = newSession(broadcast = true)
    registerViews(s)
    // show() plans CollectLimit; must run untapped with capture on
    s.sql("SELECT category, SUM(amount) FROM sales GROUP BY category").show(5)
    val limited = s.sql("SELECT * FROM sales LIMIT 3")
    limited.collect() should have length 3
    assert(!limited.queryExecution.executedPlan.exists(_.isInstanceOf[TitianTapExec]))
  }

  test("releaseLineage drops all capture blocks for a query") {
    val s = newSession(broadcast = true)
    registerViews(s)
    val df = s.sql(
      "SELECT category, SUM(amount) AS total FROM sales GROUP BY category")
    val output = TitianSQL.collectWithLineage(df)
    val electronicsId = output.find(_._1.getString(0) == "electronics").get._2
    // trace works before release
    TitianSQL.trace(df, Seq(electronicsId)).goBack().ids should have length 4

    TitianSQL.releaseLineage(df)

    // after release the capture blocks are gone: the same trace finds nothing
    TitianSQL.trace(df, Seq(electronicsId)).ids shouldBe empty
  }

  test("full-row show — DISTINCT witnesses resolved to complete source records") {
    val s = newSession(broadcast = true)
    registerViews(s)
    val df = s.sql("SELECT DISTINCT category FROM sales")
    val output = TitianSQL.collectWithLineage(df)
    val electronicsId = output.find(_._1.getString(0) == "electronics").get._2
    val cursor = TitianSQL.trace(df, Seq(electronicsId)).goBack()

    // pruned view: only the category column the query read
    cursor.show().toSet should equal (Set(Row("electronics")))
    // full view: the complete source rows, amounts included
    cursor.show(full = true).toSet should equal (Set(
      Row("electronics", 420), Row("electronics", 310),
      Row("electronics", 99999), Row("electronics", 380)))
  }
}
