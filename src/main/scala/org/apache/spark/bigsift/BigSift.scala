/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.bigsift

import scala.reflect.ClassTag

import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.rdd.{Lineage, LineageRDD}

/**
 * Outcome of a BigSift debugging session.
 *
 * @param faultInducingInputs the 1-minimal set of input records that reproduce the
 *                            test failure (the answer)
 * @param faultyOutputs       the output records flagged faulty by the test oracle
 * @param provenanceSize      number of input records in the backward-trace candidate
 *                            set (before delta debugging) — BigSift narrows this to
 *                            `faultInducingInputs`
 */
case class BigSiftResult[T](
    faultInducingInputs: Seq[String],
    faultyOutputs: Seq[T],
    provenanceSize: Int)

/**
 * BigSift (SoCC '17, FSE '18 demo) — automated isolation of the minimal fault-inducing
 * input records of a Spark job, by combining Titian's data provenance with delta
 * debugging.
 *
 * Given an input dataset, a job (`Lineage[String] => Lineage[T]`), and a test oracle
 * (`T => Boolean`, true = faulty), BigSift:
 *   1. runs the job once with lineage capture and finds the faulty output records;
 *   2. backward-traces them to their provenance — the candidate fault-inducing inputs
 *      (orders of magnitude smaller than the full input);
 *   3. delta-debugs that candidate set to a 1-minimal subset that still reproduces the
 *      failure, re-running the job on subsets (capture off) and re-applying the test,
 *      with bitmap-memoized results ([[DeltaDebug]]).
 *
 * {{{
 * val lc = new LineageContext(sc)
 * val bs = new BigSift(lc)
 * val result = bs.run(
 *   lc.textFile("transit.csv"),
 *   in => in.map(parseToKV).filter(_._2 < 45).reduceByKey(_ + _),
 *   (out: ((String, String), Int)) => out._2 < 0)   // faulty = negative total
 * result.faultInducingInputs.foreach(println)        // the culprit line(s)
 * }}}
 *
 * Predefined oracles (minimum, maximum, k-sigma-from-median, NaN/Null) are in
 * [[TestOracle]]; use [[runWithOracle]] to derive the test from the original output.
 */
class BigSift(lc: LineageContext) extends Serializable {

  /**
   * Debug with an explicit per-record test oracle.
   *
   * @param input the input dataset (e.g. `lc.textFile(path)`)
   * @param job   the Spark job as a transformation of the input
   * @param test  predicate on output records — true marks a record faulty
   * @param onProgress optional callback streaming the current candidate-set size
   */
  def run[T: ClassTag](
      input: Lineage[String],
      job: Lineage[String] => Lineage[T],
      test: T => Boolean,
      onProgress: Seq[String] => Unit = (_: Seq[String]) => ()): BigSiftResult[T] = {

    // 1. capture run: execute the job once with lineage on, collect outputs + ids
    lc.setCaptureLineage(true)
    val out = job(input)
    val withIds = out.collectWithId()
    lc.setCaptureLineage(false)

    val faulty = withIds.filter { case (v, _) => test(v) }
    if (faulty.isEmpty) return BigSiftResult(Seq.empty, Seq.empty, 0)
    val faultyIds: Set[Long] = faulty.map(_._2).toSet

    // a candidate set "reproduces the fault" iff re-running the job on just those
    // records still yields a faulty output (capture off). This is both the delta-
    // debugging test and the soundness check on the provenance trace.
    def reproduces(subset: Seq[String]): Boolean =
      subset.nonEmpty &&
        (try job(lc.parallelize(subset)).collect().exists(test)
         catch { case _: Throwable => false })

    // 2. backward provenance — the candidate fault-inducing inputs. If the trace
    //    resolves to source records that reproduce the fault, delta debugging runs
    //    over that (much smaller) set; otherwise we fall back to the full input so the
    //    result is always correct, just less pre-shrunk.
    val provenance = provenanceLines(out, faultyIds)
    val candidates: Seq[String] =
      if (reproduces(provenance)) provenance else input.collect().toVector.distinct

    // 3. delta-debug the candidate set down to the 1-minimal fault-inducing records.
    val cause = DeltaDebug.ddmin(candidates, onProgress)(reproduces)

    BigSiftResult(cause, faulty.map(_._1).toVector, candidates.size)
  }

  /**
   * Backward-trace the faulty output ids to their source records.
   *
   * The number of backward stages is job-specific and the trace must stop exactly on
   * the input tap before resolving (one step further throws). So we first probe how
   * many `goBack`s the trace allows, then re-trace to that depth and `show()`. Each
   * probe starts from a fresh `getLineage()`, which resets the cursor state — so a
   * throw while probing never corrupts the final resolving trace.
   */
  private def provenanceLines[T](out: Lineage[T], faultyIds: Set[Long]): Seq[String] = {
    def traced: LineageRDD =
      out.getLineage().filter((id: Any) => faultyIds.contains(id.asInstanceOf[Long]))

    // Probe every backward depth from a fresh trace, independently (a goBack past the
    // input tap throws, but earlier/later depths are still attempted — the first deep
    // walk also primes Titian's lineage caches, which a cold single walk lacks). The
    // deepest depth that resolves lands on the input tap and yields the raw source
    // records; shallower depths show intermediate rows, so the last successful show
    // wins. (goBack mutates the shared cursor eagerly while the lineage RDDs stay
    // lazy, so a fresh trace + a materializing show() per depth are both required.)
    var best = Vector.empty[String]
    var depth = 0
    while (depth <= 32) {
      try {
        var c = traced
        var i = 0
        while (i < depth) { c = c.goBack(); i += 1 }
        best = c.show().collect().toVector
      } catch { case _: Throwable => }
      depth += 1
    }
    best.map(_.toString).distinct
  }

  /**
   * Debug with a distribution-based oracle ([[TestOracle]]): the oracle inspects the
   * original output once to produce a fixed faulty predicate (e.g. a k-sigma band),
   * which is then used for capture and every delta-debugging re-run.
   */
  def runWithOracle[T: ClassTag](
      input: Lineage[String],
      job: Lineage[String] => Lineage[T],
      oracle: Seq[T] => (T => Boolean),
      onProgress: Seq[String] => Unit = (_: Seq[String]) => ()): BigSiftResult[T] = {
    lc.setCaptureLineage(true)
    val out = job(input)
    val withIds = out.collectWithId()
    lc.setCaptureLineage(false)
    val test = oracle(withIds.map(_._1).toSeq)

    val faulty = withIds.filter { case (v, _) => test(v) }
    if (faulty.isEmpty) return BigSiftResult(Seq.empty, Seq.empty, 0)
    val faultyIds: Set[Long] = faulty.map(_._2).toSet

    def reproduces(subset: Seq[String]): Boolean =
      subset.nonEmpty &&
        (try job(lc.parallelize(subset)).collect().exists(test)
         catch { case _: Throwable => false })

    val candidates: Seq[String] = {
      val first = provenanceLines(out, faultyIds)
      if (reproduces(first)) first
      else {
        val second = provenanceLines(out, faultyIds)
        if (reproduces(second)) second else input.collect().toVector.distinct
      }
    }

    val cause = DeltaDebug.ddmin(candidates, onProgress)(reproduces)
    BigSiftResult(cause, faulty.map(_._1).toVector, candidates.size)
  }

  /** Alias matching the original FSE '18 API name. */
  def runWithBigSift[T: ClassTag](
      input: Lineage[String],
      job: Lineage[String] => Lineage[T],
      test: T => Boolean): BigSiftResult[T] = run(input, job, test)
}
