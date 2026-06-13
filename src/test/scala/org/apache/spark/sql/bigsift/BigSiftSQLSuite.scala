/*
 * SQL BigSift: isolate the minimal fault-inducing base-table rows for a faulty query
 * output, by combining Titian's SQL provenance with delta debugging. One corrupt sales
 * row (a large negative amount) drives a category total negative; BigSift must isolate
 * exactly that row.
 */
package org.apache.spark.sql.bigsift

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.lineage.TitianSQLExtension
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class BigSiftSQLSuite extends AnyFunSuite with BeforeAndAfterEach with Matchers {

  @transient private var spark: SparkSession = _
  private val salesCsv = "src/test/resources/bigsift_sales.csv"

  override def afterEach(): Unit = {
    if (spark != null) { spark.stop(); spark = null }
    System.clearProperty("spark.driver.port")
  }

  private def newSession(): SparkSession = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("bigsift-sql-test")
      .config("spark.sql.extensions", classOf[TitianSQLExtension].getName)
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.titian.sql.capture", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.read.schema("category STRING, amount INT").csv(salesCsv)
      .createOrReplaceTempView("sales")
    spark
  }

  test("isolates the corrupt sales row behind a negative category total") {
    val s = newSession()
    val result = BigSiftSQL.debug(s, "sales",
      "SELECT category, SUM(amount) AS total FROM sales GROUP BY category",
      (r: Row) => r.getLong(1) < 0)

    // a faulty output existed (electronics total is negative)
    result.faultyOutputs.map(_.getString(0)) should contain ("electronics")
    // provenance narrowed to the electronics rows (3), not all 6
    result.provenanceSize should be < 6
    result.provenanceSize should be >= 3
    // delta debugging isolated exactly the corrupt row
    result.faultInducingRows should equal (Seq(Row("electronics", -9999999)))
  }

  test("predefined minimum oracle isolates the same corrupt row") {
    val s = newSession()
    // "explain the minimum output total" — threshold derived from the original output
    val rows = {
      s.conf.set("spark.titian.sql.capture", "true")
      val df = s.sql("SELECT category, SUM(amount) AS total FROM sales GROUP BY category")
      val out = org.apache.spark.sql.lineage.TitianSQL.collectWithLineage(df).map(_._1)
      s.conf.set("spark.titian.sql.capture", "false")
      out
    }
    val minTotal = rows.map(_.getLong(1)).min
    val result = BigSiftSQL.debug(s, "sales",
      "SELECT category, SUM(amount) AS total FROM sales GROUP BY category",
      (r: Row) => r.getLong(1) <= minTotal)

    result.faultInducingRows should equal (Seq(Row("electronics", -9999999)))
  }

  test("clean data yields no fault-inducing rows") {
    val s = newSession()
    val result = BigSiftSQL.debug(s, "sales",
      "SELECT category, SUM(amount) AS total FROM sales WHERE amount > 0 GROUP BY category",
      (r: Row) => r.getLong(1) < 0)
    result.faultInducingRows shouldBe empty
    result.faultyOutputs shouldBe empty
  }
}
