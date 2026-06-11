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

package org.apache.spark.sql.lineage

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.lineage.LineageTaskState
import org.apache.spark.lineage.util.LongIntByteBuffer
import org.apache.spark.storage.{RDDBlockId, StorageLevel}
import org.apache.spark.util.PackIntIntoLong

/**
 * Per-task runtime objects invoked by the tap operators — one instance per task,
 * created in the generated class's init (codegen) or in mapPartitions (interpreted).
 * Plain classes with simple methods so generated Java can call them directly.
 */

/** Scan-side tap: assigns the per-task input row id (TapHadoopLRDD's role). */
class ScanTapRuntime {
  private val state = LineageTaskState.get(TaskContext.get())
  private var nextId: Int = -1

  /** Called once per input row, before downstream operators consume it. */
  def tap(): Unit = {
    nextId += 1
    state.currentInputId = nextId
  }
}

/**
 * Pre-exchange tap: records (keyHash -> bitmap of currentInputIds) on the map side of
 * a shuffle — TapPreShuffleLRDD's role. Placed below the partial aggregate when one
 * exists, otherwise directly below the exchange; either way it sees pre-combine rows.
 */
class PreExchangeTapRuntime(tapId: Int) {
  private val ctx = TaskContext.get()
  private val state = LineageTaskState.get(ctx)
  private val split = ctx.partitionId()
  private val map = new org.apache.spark.lineage.Int2RoaringBitMapOpenHashMap(16384)

  ctx.addTaskCompletionListener[Unit] { _ =>
    val rows: Array[Any] =
      map.keySet().toIntArray.map(k => (k, map.get(k).toArray): Any)
    SparkEnv.get.blockManager.putIterator[Any](
      RDDBlockId(tapId, split), rows.iterator, StorageLevel.MEMORY_AND_DISK,
      tellMaster = true)
  }

  /** Called once per pre-combine row with the murmur hash of its partition/group key. */
  def tap(keyHash: Int): Unit = map.put(keyHash, state.currentInputId)
}

/**
 * Post-keyed tap: above a final aggregate or a join on the reduce side — records
 * (packed(partition, outIdx) -> keyHash) and threads outIdx downstream as the
 * currentInputId (TapPostShuffleLRDD / TapPostCoGroupLRDD's role).
 */
class PostKeyedTapRuntime(tapId: Int) {
  private val ctx = TaskContext.get()
  private val state = LineageTaskState.get(ctx)
  private val split = ctx.partitionId()
  private val buffer = new LongIntByteBuffer(LineageTaskState.getFromBufferPool())
  private var outIdx: Int = -1

  ctx.addTaskCompletionListener[Unit] { _ =>
    val rows: Array[Any] = buffer.iterator.toArray.map(r => (r._1, r._2.toLong))
    try {
      SparkEnv.get.blockManager.putIterator[Any](
        RDDBlockId(tapId, split), rows.iterator, StorageLevel.MEMORY_AND_DISK,
        tellMaster = true)
    } finally {
      buffer.clear()
      LineageTaskState.addToBufferPool(buffer.getData)
    }
  }

  /** Called once per combined output row with the murmur hash of its key. */
  def tap(keyHash: Int): Unit = {
    outIdx += 1
    buffer.put(PackIntIntoLong(split, outIdx), keyHash)
    state.currentInputId = outIdx
  }
}

/**
 * Broadcast-join tap: above a BroadcastHashJoin on the probe (streamed) side. Records
 * (packed(partition, outIdx) -> (keyHash, probeInputId)) — the key hash locates the
 * build side's broadcast lineage; the probe id is EXACT row provenance, since the
 * probe row is the pipeline's current row when the join emits.
 */
class PostJoinTapRuntime(tapId: Int) {
  private val ctx = TaskContext.get()
  private val state = LineageTaskState.get(ctx)
  private val split = ctx.partitionId()
  private val rows = new scala.collection.mutable.ArrayBuffer[Any]()
  private var outIdx: Int = -1

  ctx.addTaskCompletionListener[Unit] { _ =>
    SparkEnv.get.blockManager.putIterator[Any](
      RDDBlockId(tapId, split), rows.iterator, StorageLevel.MEMORY_AND_DISK,
      tellMaster = true)
  }

  def tap(keyHash: Int): Unit = {
    outIdx += 1
    rows += ((PackIntIntoLong(split, outIdx), (keyHash, state.currentInputId)))
    state.currentInputId = outIdx
  }
}

/**
 * Below a buffered 1:1 operator (Python UDF eval): records the per-row input-id
 * sequence so the matching ReseqTap above can replay it positionally.
 */
class SeqTapRuntime(pairId: Int) {
  private val state = LineageTaskState.get(TaskContext.get())
  private val buf = new it.unimi.dsi.fastutil.ints.IntArrayList()
  state.seqBuffers.put(pairId, buf)

  def tap(): Unit = buf.add(state.currentInputId)
}

/** Above the buffered operator: restores currentInputId row by row. */
class ReseqTapRuntime(pairId: Int) {
  private val state = LineageTaskState.get(TaskContext.get())
  private var i: Int = -1

  def tap(): Unit = {
    i += 1
    state.currentInputId = state.seqBuffers.get(pairId).getInt(i)
  }
}

/**
 * Result-side tap: records (packed(partition, outputIdx) -> currentInputId) per output
 * row and materializes the buffer to the BlockManager at task end (TapLRDD's role,
 * with LineageManager's materialization inlined into a completion listener).
 */
class ResultTapRuntime(tapId: Int) {
  private val ctx = TaskContext.get()
  private val state = LineageTaskState.get(ctx)
  private val split = ctx.partitionId()
  private val buffer = new LongIntByteBuffer(LineageTaskState.getFromBufferPool())
  private var outIdx: Int = -1

  ctx.addTaskCompletionListener[Unit] { _ =>
    val rows: Array[Any] = buffer.iterator.toArray.map(r => (r._1, r._2.toLong))
    try {
      SparkEnv.get.blockManager.putIterator[Any](
        RDDBlockId(tapId, split), rows.iterator, StorageLevel.MEMORY_AND_DISK,
        tellMaster = true)
    } finally {
      buffer.clear()
      LineageTaskState.addToBufferPool(buffer.getData)
    }
  }

  /** Called once per output row. */
  def tap(): Unit = {
    outIdx += 1
    buffer.put(PackIntIntoLong(split, outIdx), state.currentInputId)
  }
}
