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
import org.apache.spark.storage.{RDDBlockId, StorageLevel}
import org.apache.spark.util.PackIntIntoLong

/**
 * Per-task runtime objects invoked by the tap operators — one instance per task,
 * created in the generated class's init (codegen) or in mapPartitions (interpreted).
 * Plain classes with simple methods so generated Java can call them directly.
 */

/**
 * Compact lineage block payloads — one object of primitive arrays per
 * (tapId, partition) block, instead of one boxed tuple per captured row. A row's
 * packed output id is never stored: the partition is constant per block and the
 * local index is the row's array position.
 */
private[lineage] sealed trait TapBlock extends Serializable { def split: Int }

/**
 * Post-keyed or result tap block: row i has packed id (split, i); values(i) is the
 * key hash (post-keyed) or the upstream input id (result).
 */
private[lineage] case class KeyedBlock(split: Int, values: Array[Int]) extends TapBlock

/** Broadcast-join tap block: pairs(i) packs (keyHash, probeInputId) for row i. */
private[lineage] case class JoinBlock(split: Int, pairs: Array[Long]) extends TapBlock

/**
 * Pre-exchange tap block: hashes sorted ascending for binary search;
 * idLists(j) = input ids that produced key hash hashes(j).
 */
private[lineage] case class PreExchangeBlock(
    split: Int, hashes: Array[Int], idLists: Array[Array[Int]]) extends TapBlock

private[lineage] object TapBlock {
  /**
   * Where lineage blocks live, `spark.titian.lineage.storageLevel` (a cluster conf —
   * set it at submit time). Compact primitive-array blocks make the serialized-memory
   * default cheap to read back.
   */
  def storageLevel: StorageLevel = StorageLevel.fromString(
    SparkEnv.get.conf.get("spark.titian.lineage.storageLevel", "MEMORY_AND_DISK_SER"))

  def store(tapId: Int, split: Int, block: TapBlock): Unit =
    SparkEnv.get.blockManager.putSingle[Any](
      RDDBlockId(tapId, split), block, storageLevel, tellMaster = true)
}

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
    val hashes = map.keySet().toIntArray
    java.util.Arrays.sort(hashes)
    TapBlock.store(tapId, split, PreExchangeBlock(split, hashes, hashes.map(map.get(_).toArray)))
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
  private val hashes = new it.unimi.dsi.fastutil.ints.IntArrayList()

  ctx.addTaskCompletionListener[Unit] { _ =>
    TapBlock.store(tapId, split, KeyedBlock(split, hashes.toIntArray))
  }

  /** Called once per combined output row with the murmur hash of its key. */
  def tap(keyHash: Int): Unit = {
    state.currentInputId = hashes.size()
    hashes.add(keyHash)
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
  // Capture stays primitive: one packed (keyHash, probeId) long per row. The row's
  // packed output id needs no storage — split is constant and outIdx is its position.
  private val pairs = new it.unimi.dsi.fastutil.longs.LongArrayList()

  ctx.addTaskCompletionListener[Unit] { _ =>
    TapBlock.store(tapId, split, JoinBlock(split, pairs.toLongArray))
  }

  def tap(keyHash: Int): Unit = {
    val outIdx = pairs.size()
    pairs.add(PackIntIntoLong(keyHash, state.currentInputId))
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
  private val inputIds = new it.unimi.dsi.fastutil.ints.IntArrayList()

  ctx.addTaskCompletionListener[Unit] { _ =>
    TapBlock.store(tapId, split, KeyedBlock(split, inputIds.toIntArray))
  }

  /** Called once per output row. */
  def tap(): Unit = inputIds.add(state.currentInputId)
}
