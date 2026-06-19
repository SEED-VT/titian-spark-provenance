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

import scala.collection.immutable.BitSet
import scala.collection.mutable

/**
 * Delta Debugging (Zeller & Hildebrandt, *Simplifying and Isolating Failure-Inducing
 * Input*, TSE 2002) — the minimization core of BigSift.
 *
 * `ddmin` finds a 1-minimal subset of `items` that still reproduces a failure: a subset
 * for which `failing` returns true, but from which removing any single element makes it
 * stop failing. BigSift drives it with `failing(subset)` = "re-running the Spark job on
 * only these input records still produces a faulty output (per the test oracle)".
 *
 * Test results are memoized by the bitmap of selected item indices (the paper's
 * bitmap-based memoization), so a subset is never re-tested — the expensive part is the
 * job re-execution inside `failing`.
 */
object DeltaDebug {

  /** A test verdict cache keyed by the selected-index bitmap. */
  private type Memo = mutable.HashMap[BitSet, Boolean]

  /**
   * @param items    the candidate failure-inducing elements (e.g. provenance records)
   * @param failing  true iff the given subset still reproduces the failure
   * @param onProgress optional callback after each accepted reduction, with the current
   *                   candidate subset (BigSift's UI streams this — its size is the
   *                   number of still-localized records, its head a sample)
   * @return a 1-minimal failing subset, in the original element order
   */
  def ddmin[A](items: Seq[A], onProgress: Seq[A] => Unit = (_: Seq[A]) => ())(
      failing: Seq[A] => Boolean): Seq[A] = {
    val all = items.toVector
    val memo = new Memo
    var tests = 0

    def testFails(indices: BitSet): Boolean = memo.getOrElseUpdate(indices, {
      tests += 1
      failing(indices.toVector.map(all))
    })

    def report(set: BitSet): Unit = onProgress(set.toVector.map(all))

    // The whole candidate set must reproduce the failure, or there is nothing to
    // minimize — return it unchanged rather than silently emptying it.
    val full = BitSet(all.indices: _*)
    if (all.isEmpty || !testFails(full)) return items
    report(full)

    var cFail = full
    var n = 2
    var progress = true
    while (cFail.size >= 2 && progress) {
      progress = false
      val partitions = split(cFail, n)

      // reduce to a single subset that still fails
      partitions.find(testFails) match {
        case Some(sub) =>
          cFail = sub; n = 2; progress = true
          report(cFail)
        case None =>
          // reduce to a complement that still fails
          partitions.iterator.map(p => cFail.diff(p)).find(c => c.nonEmpty && testFails(c)) match {
            case Some(comp) =>
              cFail = comp; n = math.max(n - 1, 2); progress = true
              report(cFail)
            case None =>
              if (n < cFail.size) { n = math.min(n * 2, cFail.size); progress = true }
          }
      }
    }
    cFail.toVector.map(all)
  }

  /** Split a bitmap into `n` near-equal contiguous chunks (by position within it). */
  private def split(set: BitSet, n: Int): Seq[BitSet] = {
    val elems = set.toVector
    val size = elems.size
    val chunk = math.ceil(size.toDouble / n).toInt.max(1)
    elems.grouped(chunk).map(g => BitSet(g: _*)).toVector
  }
}
