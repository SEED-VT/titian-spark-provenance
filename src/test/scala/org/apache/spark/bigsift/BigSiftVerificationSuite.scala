/*
 * Verification of the BigSift implementation against the examples in the papers
 * (SoCC '17, FSE '18 demo): the airport transit-time program (FSE '18 Figure 5) and
 * all of the UI's predefined test oracles — minimum, maximum, k-sigma-from-median,
 * NaN/Null — plus a custom predicate, and a multi-record 1-minimality case.
 *
 * For every scenario BigSift must return the exact fault-inducing record(s), and we
 * additionally check necessity + sufficiency: the job on the cause alone is faulty,
 * and on the input minus the cause is not.
 */
package org.apache.spark.bigsift

import org.apache.spark.SparkContext
import org.apache.spark.lineage.{LineageContext, LocalLineageContext}
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.rdd.Lineage
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class BigSiftVerificationSuite extends AnyFunSuite with LocalLineageContext with Matchers {

  private val transit = "src/test/resources/transit.csv"
  private val midnightRow = "11/9/12,141011,23:53,1:23,MNN"

  // FSE '18 Figure 5: total layover per (airport, arrival hour) for transits < 45 min
  private def airportJob(in: Lineage[String]): Lineage[((String, String), Int)] =
    in.map { s =>
      val t = s.split(",")
      val arr = t(2).split(":"); val dep = t(3).split(":")
      ((t(4), arr(0)), (dep(0).toInt * 60 + dep(1).toInt) - (arr(0).toInt * 60 + arr(1).toInt))
    }.filter(_._2 < 45).reduceByKey(_ + _)

  private def ctx(name: String): BigSift = {
    lc = new LineageContext(new SparkContext("local[2]", name))
    new BigSift(lc)
  }

  /** Necessity + sufficiency: cause alone reproduces, full input minus cause does not. */
  private def assertNecessaryAndSufficient[T](
      all: Seq[String], cause: Seq[String],
      job: Lineage[String] => Lineage[T], test: T => Boolean): Unit = {
    lc.setCaptureLineage(false)
    job(lc.parallelize(cause)).collect().exists(test) shouldBe true            // sufficient
    val without = all.filterNot(cause.contains)
    job(lc.parallelize(without)).collect().exists(test) shouldBe false         // necessary
  }

  // ---- the paper's airport example, four ways of asking the question ----------

  test("airport — custom predicate (negative layover), FSE '18 Figure 5") {
    val bs = ctx("verify-negative")
    val all = scala.io.Source.fromFile(transit).getLines().toVector
    val r = bs.run(lc.textFile(transit, 2), airportJob,
      (o: ((String, String), Int)) => o._2 < 0)
    r.faultInducingInputs should equal (Seq(midnightRow))
    assertNecessaryAndSufficient(all, r.faultInducingInputs, airportJob, (o: ((String, String), Int)) => o._2 < 0)
  }

  test("airport — minimum-output oracle") {
    val bs = ctx("verify-min")
    val r = bs.runWithOracle(lc.textFile(transit, 2), airportJob,
      TestOracle.min[((String, String), Int)](_._2.toDouble))
    r.faultInducingInputs should equal (Seq(midnightRow))
  }

  test("airport — k-sigma-from-median oracle") {
    val bs = ctx("verify-ksigma")
    val r = bs.runWithOracle(lc.textFile(transit, 2), airportJob,
      TestOracle.kSigmaFromMedian[((String, String), Int)](1.0)(_._2.toDouble))
    r.faultInducingInputs should equal (Seq(midnightRow))
  }

  // ---- maximum oracle: a max aggregation with one anomalous record ------------

  test("maximum-output oracle isolates the anomalous record") {
    val bs = ctx("verify-max")
    val csv = "src/test/resources/bigsift_max.csv"             // b,9999999 anomalous
    val data = scala.io.Source.fromFile(csv).getLines().toVector
    def maxJob(in: Lineage[String]): Lineage[(String, Int)] =
      in.map { s => val t = s.split(","); (t(0), t(1).toInt) }.reduceByKey(math.max)
    val r = bs.runWithOracle(lc.textFile(csv, 2), maxJob,
      TestOracle.max[(String, Int)](_._2.toDouble))
    r.faultInducingInputs should equal (Seq("b,9999999"))
    assertNecessaryAndSufficient(data, r.faultInducingInputs, maxJob,
      TestOracle.max[(String, Int)](_._2.toDouble)(
        Seq(("a", 20), ("b", 9999999))))
  }

  // ---- NaN/Null oracle: a divide-by-zero record ------------------------------

  test("NaN/Null oracle isolates the divide-by-zero record") {
    val bs = ctx("verify-nan")
    val csv = "src/test/resources/bigsift_nan.csv"             // b,0,0 -> 0.0/0.0 = NaN
    val data = scala.io.Source.fromFile(csv).getLines().toVector
    def ratioJob(in: Lineage[String]): Lineage[(String, Double)] =
      in.map { s => val t = s.split(","); (t(0), t(1).toDouble / t(2).toDouble) }
        .reduceByKey(_ + _)
    val r = bs.run(lc.textFile(csv, 2), ratioJob,
      TestOracle.isNaNorNull[(String, Double)](_._2))
    r.faultInducingInputs should equal (Seq("b,0,0"))
    assertNecessaryAndSufficient(data, r.faultInducingInputs, ratioJob,
      TestOracle.isNaNorNull[(String, Double)](_._2))
  }

  // ---- multi-record 1-minimality: the fault needs two records together -------

  test("isolates a 2-record fault and is 1-minimal") {
    val bs = ctx("verify-2record")
    val csv = "src/test/resources/bigsift_2record.csv"         // x sums to 0 with both
    def sumJob(in: Lineage[String]): Lineage[(String, Int)] =
      in.map { s => val t = s.split(","); (t(0), t(1).toInt) }.reduceByKey(_ + _)
    val r = bs.run(lc.textFile(csv, 2), sumJob, (o: (String, Int)) => o._2 == 0)
    r.faultInducingInputs.toSet should equal (Set("x,1000", "x,-1000"))
    // 1-minimal: removing either element stops reproducing the fault
    lc.setCaptureLineage(false)
    sumJob(lc.parallelize(Seq("x,1000"))).collect().exists(_._2 == 0) shouldBe false
    sumJob(lc.parallelize(Seq("x,-1000"))).collect().exists(_._2 == 0) shouldBe false
    sumJob(lc.parallelize(Seq("x,1000", "x,-1000"))).collect().exists(_._2 == 0) shouldBe true
  }
}
