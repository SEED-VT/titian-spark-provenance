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

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, ThreadPoolExecutor}

import org.apache.spark.TaskContext
import org.apache.spark.lineage.util.ByteBuffer
import org.apache.spark.util.ThreadUtils

/**
 * Per-task lineage capture state.
 *
 * The Spark 2.1 fork stored this state directly on [[org.apache.spark.TaskContextImpl]]
 * (fields `currentInputId`, `currentBuffer`, buffer pools, thread pool injected from
 * `Executor`). As an attach-on library we keep the identical state in a side table keyed
 * by task attempt id, created on first access and removed by a task-completion listener.
 * Semantics are unchanged: one state object per running task, visible to every tap that
 * the task pipeline passes through.
 */
class LineageTaskState {

  /** Used to pipeline records through taps inside the same stage. */
  var currentInputId: Int = -1

  /** Used to pipeline records through taps inside the same stage. */
  var currentBuffer: ByteBuffer[Long, Int] = null

  /** Whether the current shuffle read is serving cached shuffle data (trace time). */
  var isShuffleCache: Boolean = false

  /**
   * Sequencing buffers for order-preserving-but-buffered operators (Python UDF
   * eval): the tap below records the per-row input-id sequence under its pair id;
   * the tap above replays it positionally.
   */
  val seqBuffers = new java.util.HashMap[Int, it.unimi.dsi.fastutil.ints.IntArrayList]()

  /**
   * Per-join saved probe ids. A fan-out join's matches are traversed depth-first:
   * the first match's downstream taps overwrite `currentInputId` before the second
   * match emerges, so the join's post tap must read the probe id its mark tap saved
   * when the streamed row ENTERED the join, keyed by the join's pair id.
   */
  val probeMarks = new it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap()

  def getFromBufferPool(): Array[Byte] = LineageTaskState.getFromBufferPool()

  def getFromBufferPoolLarge(): Array[Byte] = LineageTaskState.getFromBufferPoolLarge()

  def addToBufferPool(data: Array[Byte]): Unit = LineageTaskState.addToBufferPool(data)

  def addToBufferPoolLarge(data: Array[Byte]): Unit = LineageTaskState.addToBufferPoolLarge(data)
}

object LineageTaskState {

  private val states = new ConcurrentHashMap[Long, LineageTaskState]()

  /**
   * Executor-wide pools of reusable capture buffers (the fork pre-filled these in
   * `Executor`; filling lazily allocates the same buffers on first demand).
   */
  private val bufferPool = new ConcurrentLinkedQueue[Array[Byte]]()
  private val bufferPoolLarge = new ConcurrentLinkedQueue[Array[Byte]]()

  /** Replaces the executor task-launch pool the fork reused for async materialization. */
  lazy val threadPool: ThreadPoolExecutor =
    ThreadUtils.newDaemonCachedThreadPool("titian-lineage-materialization")

  /** Get (or create) the lineage state for the task owning `context`. */
  def get(context: TaskContext): LineageTaskState = {
    val id = context.taskAttemptId()
    var state = states.get(id)
    if (state == null) {
      state = new LineageTaskState
      val prev = states.putIfAbsent(id, state)
      if (prev != null) {
        state = prev
      } else {
        context.addTaskCompletionListener[Unit](_ => states.remove(id))
      }
    }
    state
  }

  def getFromBufferPool(): Array[Byte] = {
    val buffer = bufferPool.poll()
    if (buffer == null) new Array[Byte](64 * 1024 * 128) else buffer
  }

  def getFromBufferPoolLarge(): Array[Byte] = {
    val buffer = bufferPoolLarge.poll()
    if (buffer == null) new Array[Byte](64 * 1024 * 1024) else buffer
  }

  def addToBufferPool(data: Array[Byte]): Unit = bufferPool.add(data)

  def addToBufferPoolLarge(data: Array[Byte]): Unit = bufferPoolLarge.add(data)
}
