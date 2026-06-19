/*
 * Unit tests for the delta-debugging core — pure, no Spark.
 */
package org.apache.spark.bigsift

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DeltaDebugSuite extends AnyFunSuite with Matchers {

  test("isolates a single failure-inducing element (1-minimal)") {
    // failure = the subset contains 7; ddmin must return exactly Seq(7)
    val items = (1 to 64).toVector
    val result = DeltaDebug.ddmin(items)(subset => subset.contains(7))
    result should equal (Seq(7))
  }

  test("isolates a minimal combination (failure needs both 3 and 5)") {
    val items = (1 to 32).toVector
    val result = DeltaDebug.ddmin(items)(s => s.contains(3) && s.contains(5))
    result.toSet should equal (Set(3, 5))
  }

  test("the BigSift sum scenario — negative outlier among positives") {
    // mimics ((MNN,23), negative): one huge-negative record makes the sum < 0
    val records = Seq(10, 20, 30, -1000000, 15, 25)
    // failing(subset) = sum over subset is still negative (fault reproduced)
    val cause = DeltaDebug.ddmin(records)(subset => subset.sum < 0)
    cause should equal (Seq(-1000000))
  }

  test("memoization: each distinct subset is tested at most once") {
    val items = (1 to 40).toVector
    val tested = scala.collection.mutable.Set[Seq[Int]]()
    var calls = 0
    DeltaDebug.ddmin(items)(subset => {
      calls += 1
      tested.add(subset) shouldBe true   // never a repeat
      subset.contains(11)
    })
    calls should equal (tested.size)
  }

  test("no fault in the candidate set — returned unchanged, not emptied") {
    val items = Seq(1, 2, 3)
    DeltaDebug.ddmin(items)(_ => false) should equal (items)
  }

  test("progress callback is monotonically non-increasing") {
    val sizes = scala.collection.mutable.ArrayBuffer[Int]()
    DeltaDebug.ddmin((1 to 100).toVector, (s: Seq[Int]) => sizes += s.size)(_.contains(42))
    sizes.toSeq should equal (sizes.sorted.reverse.toSeq)
  }

  test("TestOracle.min fixes the threshold from the original output") {
    val outputs = Seq(("a", 5.0), ("b", -3.0), ("c", 8.0))
    val faulty = TestOracle.min[(String, Double)](_._2)(outputs)
    outputs.filter(faulty).map(_._1) should equal (Seq("b"))
  }

  test("TestOracle.kSigmaFromMedian flags the outlier") {
    val outputs = (1 to 20).map(i => ("k" + i, i.toDouble)) :+ ("bad", 100000.0)
    val faulty = TestOracle.kSigmaFromMedian[(String, Double)](3.0)(_._2)(outputs)
    outputs.filter(faulty).map(_._1) should contain ("bad")
    outputs.filter(faulty).map(_._1) should not contain "k10"
  }

  test("TestOracle.isNaNorNull flags NaN and null values") {
    val faulty = TestOracle.isNaNorNull[Any](identity)
    faulty(Double.NaN) shouldBe true
    faulty(null) shouldBe true
    faulty(3.0) shouldBe false
  }
}
