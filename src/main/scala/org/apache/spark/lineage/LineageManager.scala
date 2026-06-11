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

import scala.collection.mutable.HashSet

import org.apache.spark._
import org.apache.spark.lineage.rdd.TapLRDD
import org.apache.spark.storage._

/**
 * Materializes tap buffers to the BlockManager when a task finishes.
 *
 * In the fork this was driven by `finally` blocks added to `ShuffleMapTask` and
 * `ResultTask`. As a library, `initMaterialization` (called from `TapLRDD.compute`)
 * registers a task-completion listener instead — the sanctioned Spark 4 hook with the
 * same timing. Multiple taps in one task each register; `materialize` drains only the
 * entries for that (partition, stage), so duplicate listener invocations are no-ops.
 */
object LineageManager {

  private val underMaterialization = new HashSet[(TapLRDD[_], Int, Int, StorageLevel)]

  def initMaterialization[T](
      rdd: TapLRDD[T],
      partition: Partition,
      context: TaskContext,
      level: StorageLevel = StorageLevel.MEMORY_AND_DISK): Unit = {
    underMaterialization.synchronized {
      underMaterialization += ((rdd, partition.index, context.stageId, level))
    }
    context.addTaskCompletionListener[Unit] { ctx =>
      materialize(partition.index, ctx)
    }
  }

  private def materialize(split: Int, context: TaskContext): Unit = {
    val toMaterialize = underMaterialization.synchronized {
      val rdds = underMaterialization.filter(r => (r._2 == split) && (r._3 == context.stageId))
      rdds.foreach(underMaterialization.remove(_))
      rdds
    }

    val blockManager = SparkEnv.get.blockManager
    toMaterialize.foreach { case (tap, _, _, level) =>
      val task = new Runnable() {
        override def run(): Unit = {
          val key = RDDBlockId(tap.id, split)
          val arr = tap.materializeBuffer
          try {
            blockManager.putIterator[Any](key, arr.iterator, level, tellMaster = true)
          } catch {
            case e: Exception => println(e)
          } finally {
            tap.releaseBuffer()
          }
        }
      }
      LineageTaskState.threadPool.execute(task)
    }
  }
}
