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

import org.apache.spark._
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.lineage.util.LongIntByteBuffer
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleReader}
import org.apache.spark.storage.{BlockManager, ShuffleBlockFetcherIterator}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

/**
 * Lineage-aware shuffle reader used by `ShuffledLRDD.compute`.
 *
 * Port of the fork's reader: the fetch plumbing mirrors Spark 4.1.2's
 * `BlockStoreShuffleReader.read()` (see `reference/spark-4.1.2-snippets/`; this is the
 * one version-coupled class — TOUCHPOINT_INVENTORY.md C1/C2). The lineage-specific
 * semantics are unchanged from the fork:
 *
 *  - `read(isCache = Some(true))`  — pre-shuffle cache read: return raw fetched records
 *    with no aggregation (the data was already combined when written).
 *  - `read(isCache = Some(false))` — post-shuffle cache read: empty partitions
 *    short-circuit; otherwise aggregate (the `LAggregator` capture path applies).
 *  - `read(None)`                  — normal capture-time read: aggregate; capture
 *    happens inside `LAggregator` when its lineage flag is on.
 *
 * Known gap vs. the fork (deliberate, tracked): the `keyOrdering` sort path uses the
 * stock ExternalSorter, so per-record lineage ids are not captured for sort-only
 * shuffles (fork: `ExternalSorter.insertAll(records, Some(true), context)`).
 * sortByKey-lineage will be restored with the wrapper-ShuffleManager work.
 */
private[spark] class LBlockStoreShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    var lineage: Boolean = false,
    serializerManager: SerializerManager = SparkEnv.get.serializerManager,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
  extends ShuffleReader[K, C] with Logging {

  private val dep = handle.dependency

  override def read(): Iterator[Product2[K, C]] = read(None)

  /** Read the combined key-values for this reduce task */
  def read(isCache: Option[Boolean], shuffId: Int = 0): Iterator[Product2[K, C]] = {
    val readMetrics = context.taskMetrics().createTempShuffleReadMetrics()
    val blocksByAddress = mapOutputTracker.getMapSizesByExecutorId(
      handle.shuffleId, 0, Int.MaxValue, startPartition, endPartition)

    val wrappedStreams = new ShuffleBlockFetcherIterator(
      context,
      blockManager.blockStoreClient,
      blockManager,
      mapOutputTracker,
      blocksByAddress,
      serializerManager.wrapStream,
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      SparkEnv.get.conf.get(config.REDUCER_MAX_SIZE_IN_FLIGHT) * 1024 * 1024,
      SparkEnv.get.conf.get(config.REDUCER_MAX_REQS_IN_FLIGHT),
      SparkEnv.get.conf.get(config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS),
      SparkEnv.get.conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM),
      SparkEnv.get.conf.get(config.SHUFFLE_MAX_ATTEMPTS_ON_NETTY_OOM),
      SparkEnv.get.conf.get(config.SHUFFLE_DETECT_CORRUPT),
      SparkEnv.get.conf.get(config.SHUFFLE_DETECT_CORRUPT_MEMORY),
      SparkEnv.get.conf.get(config.SHUFFLE_CHECKSUM_ENABLED),
      SparkEnv.get.conf.get(config.SHUFFLE_CHECKSUM_ALGORITHM),
      readMetrics,
      doBatchFetch = false).toCompletionIterator

    val serializerInstance = dep.serializer.newInstance()

    // Create a key/value iterator for each stream
    val recordIter = wrappedStreams.flatMap { case (blockId, wrappedStream) =>
      // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
      // NextIterator. The NextIterator makes sure that close() is called on the
      // underlying InputStream when all records have been read.
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
    }

    // Update the context task metrics for each record read.
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    if (isCache.isDefined) {
      if (isCache.get) {
        return interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
      } else {
        if (interruptibleIter.isEmpty) {
          return Iterator.empty
        }
        lineage = true
      }
    }

    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        // We are reading values that are already combined
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else if (dep.aggregator.isEmpty && dep.mapSideCombine) {
      throw new IllegalStateException("Aggregator is empty for map-side combine")
    } else {
      // Convert the Product2s to pairs since this is what downstream RDDs currently expect
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]].map(pair => (pair._1, pair._2))
    }

    // Sort the output if there is a sort ordering defined.
    dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // Fork: ExternalSorter.insertAll(records, Some(true), context) stripped the
        // lineage tag from each record into the task capture buffer before sorting.
        // Same association, done on the iterator before the stock sorter consumes it.
        val toSort: Iterator[Product2[K, C]] = if (lineage) {
          val state = LineageTaskState.get(context)
          val buffer = new LongIntByteBuffer(state.getFromBufferPool())
          state.currentBuffer = buffer
          aggregatedIter.asInstanceOf[Iterator[Product2[K, Product2[C, Long]]]].map { pair =>
            buffer.put(pair._2._2, LineageHashing.hashKey(pair._1))
            (pair._1, pair._2._1)
          }
        } else {
          aggregatedIter
        }
        val sorter =
          new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = dep.serializer)
        sorter.insertAllAndUpdateMetrics(toSort.asInstanceOf[Iterator[(K, C)]])
      case None =>
        aggregatedIter
    }
  }
}
