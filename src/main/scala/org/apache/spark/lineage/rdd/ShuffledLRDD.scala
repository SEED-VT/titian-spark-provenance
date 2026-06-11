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

package org.apache.spark.lineage.rdd

import org.apache.spark._
import org.apache.spark.lineage.{LAggregator, LBlockStoreShuffleReader, LineageContext, LineageTaskState}
import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.BaseShuffleHandle

import scala.reflect._

/**
 * The resulting RDD from a shuffle (e.g. repartitioning of data).
 *
 * Stock `ShuffledRDD` keeps its serializer/ordering/aggregator/mapSideCombine fields
 * private (the fork made them protected); we shadow the values in our overriding setters
 * — the stock setters still run, so the ShuffleDependency this RDD creates is identical.
 *
 * @param previous the parent RDD.
 * @param part the partitioner used to partition the RDD
 * @tparam K the key class.
 * @tparam V the value class.
 * @tparam C the combiner class.
 */
class ShuffledLRDD[K: ClassTag, V: ClassTag, C: ClassTag](
    @transient var previous: Lineage[_ <: Product2[K, V]],
    part: Partitioner
  ) extends ShuffledRDD[K, V, C](previous, part) with Lineage[(K, C)] {
  self =>

  private var lSerializer: Option[Serializer] = None

  private var lKeyOrdering: Option[Ordering[K]] = None

  private var lAggregator: Option[Aggregator[K, V, C]] = None

  private var lMapSideCombine: Boolean = false

  override def ttag = classTag[(K, C)]

  override def lineageContext: LineageContext = previous.lineageContext

  override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    // Fork: SortShuffleManager.getReader(..., Some(isLineageActive)) returned the
    // lineage-aware reader; the library constructs it directly.
    new LBlockStoreShuffleReader(
      dep.shuffleHandle.asInstanceOf[BaseShuffleHandle[K, _, C]],
      split.index, split.index + 1, context, isLineageActive)
      .read(isPreShuffleCache, id)
      .asInstanceOf[Iterator[(K, C)]]
  }

  /** Set a serializer for this RDD's shuffle, or null to use the default (spark.serializer) */
  override def setSerializer(serializer: Serializer): ShuffledLRDD[K, V, C] = {
    lSerializer = Option(serializer)
    super.setSerializer(serializer)
    this
  }

  /** Set aggregator for RDD's shuffle. */
  override def setAggregator(aggregator: Aggregator[K, V, C]): ShuffledLRDD[K, V, C] = {
    lAggregator = Option(aggregator)
    super.setAggregator(aggregator)
    this
  }

  /** Set mapSideCombine flag for RDD's shuffle. */
  override def setMapSideCombine(mapSideCombine: Boolean): ShuffledLRDD[K, V, C] = {
    lMapSideCombine = mapSideCombine
    super.setMapSideCombine(mapSideCombine)
    this
  }

  /** Set key ordering for RDD's shuffle. */
  override def setKeyOrdering(keyOrdering: Ordering[K]): ShuffledLRDD[K, V, C] = {
    lKeyOrdering = Option(keyOrdering)
    super.setKeyOrdering(keyOrdering)
    this
  }

  override def tapRight(): TapLRDD[(K, C)] = {
    val tap = new TapPostShuffleLRDD[(K, C)](
      lineageContext, Seq(new OneToOneDependency[(K, C)](this))
    )
    setTap(tap)
    setCaptureLineage(true)
    tap.setCached(this)
  }

  override def tapLeft(): TapLRDD[(K, C)] = {
    var newDeps = Seq.empty[Dependency[_]]
    for(dep <- dependencies) {
      newDeps = newDeps :+ new OneToOneDependency(dep.rdd)
    }
    new TapPreShuffleLRDD[(K, C)](lineageContext, newDeps)
      .setCached(this).combinerEnabled(lMapSideCombine)
  }

  override def getAggregate(
      tappedIter: Iterator[Nothing],
      context: TaskContext): Iterator[Product2[_, _]] = {
    if (tappedIter.isEmpty) {
      return tappedIter
    } else {
      val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
      val result: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
        if (dep.mapSideCombine) {
          // Fork: combineCombinersByKey(tappedIter, context, isCache = true); the flag
          // now travels through the task state (see LAggregator).
          val state = LineageTaskState.get(context)
          state.isShuffleCache = true
          try {
            dep.aggregator.get.combineCombinersByKey(tappedIter, context)
          } finally {
            state.isShuffleCache = false
          }
        } else {
          dep.aggregator.get.combineValuesByKey(tappedIter, context)
        }
      } else if (dep.aggregator.isEmpty && dep.mapSideCombine) {
        throw new IllegalStateException("Aggregator is empty for map-side combine")
      } else {
        // Convert the Product2s to pairs since this is what downstream RDDs currently expect
        tappedIter.asInstanceOf[Iterator[Product2[K, C]]].map(pair => (pair._1, pair._2))
      }
      result
    }
  }

  override def replay(rdd: Lineage[_]) = new ShuffledLRDD[K, V, C](rdd.asInstanceOf[Lineage[(K, V)]], part)
    .setSerializer(lSerializer.getOrElse(null))
    .setAggregator(lAggregator.get.asInstanceOf[LAggregator[K, V, C]].setLineage(false))
    .asInstanceOf[ShuffledLRDD[K, V, C]]
    .setMapSideCombine(lMapSideCombine)
}
