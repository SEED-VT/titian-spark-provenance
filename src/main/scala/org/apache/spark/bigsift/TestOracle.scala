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

/**
 * Pre-defined test oracles for common big-data anomalies (the BigSift UI's built-in
 * test functions: minimum, maximum, k-sigma-from-median, NaN/Null).
 *
 * A distribution-based oracle (`min`, `max`, `kSigmaFromMedian`) is a function of the
 * *original* output: it inspects the full result once and returns a fixed predicate —
 * e.g. a numeric threshold — that is then applied unchanged while delta debugging
 * re-runs the job on subsets. Fixing the predicate up front is what keeps the failing
 * test stable across re-executions (cf. the paper's failure-monotonicity discussion).
 */
object TestOracle {

  /** Faulty = the record(s) achieving the minimum of `value`. */
  def min[T](value: T => Double): Seq[T] => (T => Boolean) = outputs => {
    val m = outputs.iterator.map(value).reduceOption(_ min _).getOrElse(Double.NaN)
    (t: T) => value(t) <= m
  }

  /** Faulty = the record(s) achieving the maximum of `value`. */
  def max[T](value: T => Double): Seq[T] => (T => Boolean) = outputs => {
    val m = outputs.iterator.map(value).reduceOption(_ max _).getOrElse(Double.NaN)
    (t: T) => value(t) >= m
  }

  /**
   * Faulty = records whose `value` is more than `k` standard deviations from the
   * median (the UI's "k-sigma range of median" oracle). The band `[median - kσ,
   * median + kσ]` is computed once from the original output and then fixed.
   */
  def kSigmaFromMedian[T](k: Double)(value: T => Double): Seq[T] => (T => Boolean) =
    outputs => {
      val xs = outputs.map(value).toArray
      if (xs.isEmpty) (_: T) => false
      else {
        val median = percentile(xs, 0.5)
        val mean = xs.sum / xs.length
        val variance = xs.iterator.map(x => (x - mean) * (x - mean)).sum / xs.length
        val sigma = math.sqrt(variance)
        val (lo, hi) = (median - k * sigma, median + k * sigma)
        (t: T) => { val v = value(t); v < lo || v > hi }
      }
    }

  /** Faulty = records whose `value` is NaN or null (bad parses, divide-by-zero, …). */
  def isNaNorNull[T](value: T => Any): T => Boolean = (t: T) =>
    value(t) match {
      case null => true
      case d: Double => d.isNaN
      case f: Float => f.isNaN
      case _ => false
    }

  private def percentile(xs: Array[Double], p: Double): Double = {
    val sorted = xs.sorted
    val idx = math.min((p * sorted.length).toInt, sorted.length - 1)
    sorted(idx)
  }
}
