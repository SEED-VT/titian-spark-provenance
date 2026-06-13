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

import org.apache.spark.SparkEnv

/**
 * Performance-tuning flags. Every Titian capture/trace optimization is ON by default;
 * `spark.titian.ablation` lets you switch any one back to its pre-optimization
 * implementation — for the ablation study (measuring each one's contribution) or as a
 * fallback. All legacy paths are verified to produce byte-identical lineage to the
 * optimized ones (see docs/ablation.md / scripts/verify_ablation.sh).
 *
 * `spark.titian.ablation` is a comma-separated list of:
 *  - `legacyHash`       — P1a: murmur over key.toString bytes (RDD side)
 *  - `boxedCombiner`    — P1b: Tuple2 per combine instead of mutable TaggedCombiner
 *  - `boxedJoinBuffer`  — P2: boxed tuple ArrayBuffer in the broadcast-join tap
 *  - `boxedBlocks`      — P3: one boxed tuple per row in lineage blocks
 *  - `driverTrace`      — P4: collect whole blocks to the driver and filter there;
 *                          no block-locality preferred locations
 * (`spark.titian.lineage.storageLevel` already ablates P3's serialized default.)
 *
 * A cluster conf: set at submit/session-build time, constant for the JVM's current
 * SparkEnv. The lookup is two volatile reads after the first call per SparkEnv.
 */
private[spark] object TitianAblation {

  @volatile private var cachedFor: SparkEnv = _
  @volatile private var cached: Set[String] = Set.empty

  private def flags: Set[String] = {
    val env = SparkEnv.get
    if (env ne cachedFor) {
      cached =
        if (env == null) Set.empty
        else env.conf.get("spark.titian.ablation", "")
          .split(",").map(_.trim).filter(_.nonEmpty).toSet
      cachedFor = env
    }
    cached
  }

  def legacyHash: Boolean = flags.contains("legacyHash")
  def boxedCombiner: Boolean = flags.contains("boxedCombiner")
  def boxedJoinBuffer: Boolean = flags.contains("boxedJoinBuffer")
  def boxedBlocks: Boolean = flags.contains("boxedBlocks")
  def driverTrace: Boolean = flags.contains("driverTrace")
}
