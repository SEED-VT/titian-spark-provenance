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

package org.apache.spark.lineage

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.lineage.util.LongIntByteBuffer
import org.apache.spark.util.collection._
import org.apache.spark.{Aggregator, TaskContext}

/**
 * Lineage-aware aggregator. When `isLineage` is on, the incoming records carry a packed
 * lineage id alongside each value (`(K, (V, Long))`); the aggregator records
 * `(lineageId, murmur3(key))` into the task's capture buffer while combining, exactly as
 * in the fork.
 *
 * Differences from the fork, mechanism only (semantics preserved):
 *  - The fork overrode a 3-arg `combineCombinersByKey(iter, context, isCache)` it had
 *    added to Spark's Aggregator. Stock Spark has the 2-arg form; the `isCache` bit now
 *    travels via [[LineageTaskState.isShuffleCache]].
 *  - The fork used its `ExternalAppendOnlyMap.insert(key, update)` addition. We strip
 *    the lineage component with a recording iterator and use the stock `insertAll`,
 *    which applies the same create/merge functions record by record.
 *  - The fork's restored non-spilling path (plain AppendOnlyMap when
 *    `spark.shuffle.spill=false`) is dropped: Spark removed that option in 1.6+ and
 *    always uses the external map; without memory pressure it behaves identically.
 *
 * @param createCombiner function to create the initial value of the aggregation.
 * @param mergeValue function to merge a new value into the aggregation result.
 * @param mergeCombiners function to merge outputs from multiple mergeValue function.
 */
@DeveloperApi
class LAggregator[K, V, C] (
    val userCreateCombiner: V => C,
    val userMergeValue: (C, V) => C,
    val userMergeCombiners: (C, C) => C,
    val isLineage: Boolean = false)
  extends Aggregator[K, V, C](
    // The functions handed to super are what the stock shuffle write path (map-side
    // combine inside ExternalSorter) applies. In lineage mode the records on the wire
    // are (K, (V, packedId)) — the wrappers propagate the tag through the combine,
    // collapsing it to (sourcePartition, 0) exactly as the fork's writePartitionedFile
    // did for combined values.
    if (isLineage) LAggregator.wrapCreate(userCreateCombiner) else userCreateCombiner,
    if (isLineage) LAggregator.wrapMergeValue(userMergeValue) else userMergeValue,
    if (isLineage) LAggregator.wrapMergeCombiners(userMergeCombiners) else userMergeCombiners) {


  override def combineValuesByKey(
      iter: Iterator[_ <: Product2[K, V]],
      context: TaskContext): Iterator[(K, C)] = {
    val combiners = new ExternalAppendOnlyMap[K, V, C](userCreateCombiner, userMergeValue, userMergeCombiners)

    if (!isLineage) {
      combiners.insertAll(iter)
    } else {
      val state = LineageTaskState.get(context)
      val buffer = new LongIntByteBuffer(state.getFromBufferPool())
      val tappedIter = iter.asInstanceOf[Iterator[Product2[K, Product2[V, Long]]]]
      val recordingIter = tappedIter.map { pair =>
        buffer.put(pair._2._2, LineageHashing.hashKey(pair._1))
        (pair._1, pair._2._1)
      }
      combiners.insertAll(recordingIter)
      state.currentBuffer = buffer
    }

    updateMetrics(context, combiners)
    combiners.iterator
  }

  override def combineCombinersByKey(
      iter: Iterator[_ <: Product2[K, C]],
      context: TaskContext): Iterator[(K, C)] = {
    val combiners = new ExternalAppendOnlyMap[K, C, C](identity, userMergeCombiners, userMergeCombiners)

    if (!isLineage) {
      combiners.insertAll(iter)
    } else {
      val state = LineageTaskState.get(context)
      val tappedIter = iter.asInstanceOf[Iterator[Product2[K, Product2[C, Long]]]]
      if (state.isShuffleCache) {
        // Reading already-captured shuffle data back as a cache: strip lineage, no capture
        combiners.insertAll(tappedIter.map(pair => (pair._1, pair._2._1)))
      } else {
        val buffer = new LongIntByteBuffer(state.getFromBufferPool())
        val recordingIter = tappedIter.map { pair =>
          buffer.put(pair._2._2, LineageHashing.hashKey(pair._1))
          (pair._1, pair._2._1)
        }
        combiners.insertAll(recordingIter)
        state.currentBuffer = buffer
      }
    }

    updateMetrics(context, combiners)
    combiners.iterator
  }

  def setLineage(lineage: Boolean): Aggregator[K, V, C] = {
    // The map-side wrappers are fixed at construction, so flipping the flag means
    // building a fresh instance from the original user functions.
    new LAggregator(userCreateCombiner, userMergeValue, userMergeCombiners, lineage)
      .asInstanceOf[Aggregator[K, V, C]]
  }

  private def updateMetrics(context: TaskContext, map: ExternalAppendOnlyMap[_, _, _]): Unit = {
    Option(context).foreach { c =>
      c.taskMetrics().incMemoryBytesSpilled(map.memoryBytesSpilled)
      c.taskMetrics().incDiskBytesSpilled(map.diskBytesSpilled)
      c.taskMetrics().incPeakExecutionMemory(map.peakMemoryUsedBytes)
    }
  }
}

object LAggregator {

  import org.apache.spark.util.PackIntIntoLong

  /**
   * Mutable carrier for a combined value plus its lineage tag. A combiner lives in
   * exactly one map slot, so merges can update `value` in place instead of allocating
   * a fresh Tuple2 per record. Implements Product2 because everything downstream
   * (reduce-side aggregation, trace machinery) reads tagged combiners through
   * `Product2[C, Long]`.
   */
  private final class TaggedCombiner(var value: Any, val tag: Long)
    extends Product2[Any, Long] with Serializable {
    override def _1: Any = value
    override def _2: Long = tag
    override def canEqual(that: Any): Boolean = that.isInstanceOf[TaggedCombiner]
  }

  // The casts are deliberate type-punning: in lineage mode the runtime objects flowing
  // through an RDD[(K, V)]-typed shuffle are (K, (V, Long)), as in the fork.
  // The boxedCombiner ablation restores the fork's Tuple2-per-merge behavior; both
  // shapes are read downstream through Product2, so they are interchangeable.

  private def wrapCreate[V, C](f: V => C): V => C = {
    if (TitianAblation.boxedCombiner) { (tagged: Any) =>
      val pair = tagged.asInstanceOf[Product2[V, Long]]
      (f(pair._1), PackIntIntoLong(PackIntIntoLong.getLeft(pair._2), 0))
    } else { (tagged: Any) =>
      val pair = tagged.asInstanceOf[Product2[V, Long]]
      new TaggedCombiner(f(pair._1), PackIntIntoLong(PackIntIntoLong.getLeft(pair._2), 0))
    }
  }.asInstanceOf[V => C]

  private def wrapMergeValue[V, C](f: (C, V) => C): (C, V) => C = {
    if (TitianAblation.boxedCombiner) { (combined: Any, tagged: Any) =>
      val c = combined.asInstanceOf[Product2[C, Long]]
      val pair = tagged.asInstanceOf[Product2[V, Long]]
      (f(c._1, pair._1), c._2)
    } else { (combined: Any, tagged: Any) =>
      val c = combined.asInstanceOf[TaggedCombiner]
      val pair = tagged.asInstanceOf[Product2[V, Long]]
      c.value = f(c.value.asInstanceOf[C], pair._1)
      c
    }
  }.asInstanceOf[(C, V) => C]

  private def wrapMergeCombiners[C](f: (C, C) => C): (C, C) => C = {
    if (TitianAblation.boxedCombiner) { (left: Any, right: Any) =>
      val a = left.asInstanceOf[Product2[C, Long]]
      val b = right.asInstanceOf[Product2[C, Long]]
      (f(a._1, b._1), a._2)
    } else { (left: Any, right: Any) =>
      val a = left.asInstanceOf[TaggedCombiner]
      val b = right.asInstanceOf[TaggedCombiner]
      a.value = f(a.value.asInstanceOf[C], b.value.asInstanceOf[C])
      a
    }
  }.asInstanceOf[(C, C) => C]
}
