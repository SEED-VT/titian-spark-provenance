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

import org.apache.spark.Partitioner
import org.apache.spark.util.Utils

/**
 * Partitions user records by the same murmur3 key hash that lineage capture stores
 * (see [[LineageHashing]]).
 *
 * The fork achieved this by replacing Spark's default `HashPartitioner` globally
 * (TOUCHPOINT_INVENTORY.md C3). The library instead swaps this partitioner in only for
 * lineage-captured shuffles, so that at trace time a record's stored hash identifies
 * the reduce partition that processed it — the invariant the trace-side joins
 * ([[HashAwarePartitioner]], `getCachedData` locality) rely on. User jobs that run
 * without capture keep stock partitioning.
 */
class LineageHashPartitioner(partitions: Int) extends Partitioner {

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => Utils.nonNegativeMod(LineageHashing.hashKey(key), numPartitions)
  }

  override def equals(other: Any): Boolean = other match {
    case h: LineageHashPartitioner => h.numPartitions == numPartitions
    case _ => false
  }

  override def hashCode: Int = numPartitions
}
