/*
 * Phase 0/1 tests for Titian-SQL: extension injection, codegen-fused single-stage
 * capture, backward trace, and show() via deterministic re-scan.
 */
package org.apache.spark.sql.lineage

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.functions.col
import org.apache.spark.util.PackIntIntoLong
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SQLLineageSuite extends AnyFunSuite with BeforeAndAfterEach with Matchers {

  @transient private var spark: SparkSession = _

  private val fruitsFile = "src/test/resources/fruits.txt"

  override def beforeEach(): Unit = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("titian-sql-test")
      .config("spark.sql.extensions", classOf[TitianSQLExtension].getName)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterEach(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
    System.clearProperty("spark.driver.port")
  }

  test("capture off — extension is a strict no-op") {
    spark.conf.set("spark.titian.sql.capture", "false")
    val df = spark.read.text(fruitsFile).filter(col("value").contains("a"))
    df.collect().length should equal (6)
    assert(!df.queryExecution.executedPlan.exists(_.isInstanceOf[TitianTapExec]),
      "no tap operators may appear when capture is off")
  }

  test("unsupported operators fail loudly") {
    spark.conf.set("spark.titian.sql.capture", "true")
    // global orderBy plans a RangePartitioning exchange — outside the supported set
    val df = spark.read.text(fruitsFile).orderBy(col("value"))
    val e = intercept[Exception] { df.collect() }
    def causes(t: Throwable): Seq[Throwable] =
      if (t == null) Nil else t +: causes(t.getCause)
    assert(causes(e).exists(_.isInstanceOf[TitianUnsupportedOperatorException]),
      s"expected TitianUnsupportedOperatorException, got: $e")
  }

  test("single-stage capture: taps are fused into whole-stage codegen") {
    spark.conf.set("spark.titian.sql.capture", "true")
    val df = spark.read.text(fruitsFile).filter(col("value").contains("a"))
    df.collect()

    val plan = df.queryExecution.executedPlan
    val fusedStage = plan.collectFirst {
      case w: WholeStageCodegenExec
        if w.exists(_.isInstanceOf[TapScanExec]) && w.exists(_.isInstanceOf[TapResultExec]) => w
    }
    assert(fusedStage.isDefined,
      s"taps must be inside WholeStageCodegen (no fallback); plan:\n$plan")
  }

  test("single-stage backward trace and show") {
    spark.conf.set("spark.titian.sql.capture", "true")
    // fruits.txt: apple,banana,apple,cherry | banana,apple,cherry,date (2 splits).
    // Filter keeps rows containing "a": drops the two cherry rows, so downstream
    // indices shift against scan indices — the trace must bridge that.
    val df = spark.read.text(fruitsFile).filter(col("value").contains("a"))
    val output = TitianSQL.collectWithLineage(df)

    output.map(_._1.getString(0)).sorted should equal (
      Array("apple", "apple", "apple", "banana", "banana", "date"))

    // Trace "date": the filter drops cherry rows before it, so its scan row index is
    // strictly greater than its post-filter output index — proves the id threading
    // crosses the filter correctly. (Split layout is the file source's business: this
    // small file typically lands in a single partition, unlike RDD textFile(_, 2).)
    val dateId = output.find(_._1.getString(0) == "date").get._2
    val dateInputs = TitianSQL.backward(df, Seq(dateId))
    dateInputs should have length 1
    assert(PackIntIntoLong.getRight(dateInputs(0)) > PackIntIntoLong.getRight(dateId),
      "scan row index must exceed post-filter output index for 'date'")

    TitianSQL.showInputs(df, dateInputs).map(_.getString(0)) should equal (Array("date"))

    // Trace every output back and re-show: must reproduce exactly the filtered rows
    val allInputs = TitianSQL.backward(df, output.map(_._2))
    allInputs should have length 6
    TitianSQL.showInputs(df, allInputs).map(_.getString(0)).sorted should equal (
      Array("apple", "apple", "apple", "banana", "banana", "date"))
  }

  test("interpreted path: capture works with whole-stage codegen disabled") {
    spark.conf.set("spark.titian.sql.capture", "true")
    spark.conf.set("spark.sql.codegen.wholeStage", "false")
    val df = spark.read.text(fruitsFile).filter(col("value").contains("an"))
    val output = TitianSQL.collectWithLineage(df)
    output.map(_._1.getString(0)).sorted should equal (Array("banana", "banana"))

    val inputs = TitianSQL.backward(df, output.map(_._2))
    TitianSQL.showInputs(df, inputs).map(_.getString(0)).sorted should equal (
      Array("banana", "banana"))
  }
}
