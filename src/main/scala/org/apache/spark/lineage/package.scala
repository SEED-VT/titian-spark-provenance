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

package org.apache.spark

/**
 * RDD-level data provenance — the original Titian capture engine, ported to attach to
 * stock Spark 4 (no forked Spark sources).
 *
 * ==Usage==
 * Wrap a `SparkContext` in a [[org.apache.spark.lineage.LineageContext]], turn capture
 * on, run a job through the lineage-aware RDD operators, then trace:
 * {{{
 * val lc = new LineageContext(sc)
 * lc.setCaptureLineage(true)
 * val counts = lc.textFile(path).map(parse).reduceByKey(_ + _)
 * val out = counts.collectWithId()
 * lc.setCaptureLineage(false)
 *
 * // backward: an aggregate -> its source records
 * counts.getLineage().filter(_ == suspectId).goBack().goBack().show().collect()
 * }}}
 *
 * ==Mechanism==
 * Capture inserts tap RDDs at stage boundaries by splicing the job's dependency DAG
 * ([[org.apache.spark.lineage.LineageContext.tapJob]]). Each tap records compact
 * `(inputId, outputId)` associations keyed by a murmur key hash
 * ([[org.apache.spark.lineage.LineageHashing]]); the lineage-aware aggregator
 * ([[org.apache.spark.lineage.LAggregator]]) carries the tag through map-side combine.
 * The distributed selective join of the VLDB paper — partition-id-routed trace cursor
 * plus `zipPartitions` — is preserved so traces never reshuffle the data.
 *
 * ==Optimizations & tuning (`spark.titian.ablation`)==
 * Five capture/trace optimizations are '''on by default'''. Each can be switched off
 * individually (comma-separated in `spark.titian.ablation`) for the ablation study or
 * as a fallback; every legacy path produces byte-identical lineage (verified by the
 * full suite under each flag — see `docs/ablation.md`, `scripts/verify_ablation.sh`). The
 * flag reader is `TitianAblation`.
 *
 *   - '''P1a''' `legacyHash` — key hash via `Murmur3.hashInt(key.hashCode)` instead of
 *     murmuring `key.toString` bytes (kills a per-record String allocation).
 *   - '''P1b''' `boxedCombiner` — a mutable tagged carrier in [[LAggregator]] instead
 *     of a fresh `Tuple2` per map-side merge.
 *   - '''P2''' `boxedJoinBuffer` — primitive `long` buffer in the broadcast-join tap
 *     instead of an `ArrayBuffer` of boxed tuples.
 *   - '''P3''' `boxedBlocks` — compact primitive-array lineage blocks
 *     ([[org.apache.spark.sql.lineage.TapBlock]]) instead of one boxed tuple per row
 *     (≈4× smaller footprint). Block storage level:
 *     `spark.titian.lineage.storageLevel` (default `MEMORY_AND_DISK_SER`).
 *   - '''P4''' `driverTrace` — trace filtering runs executor-side over the blocks
 *     (block-locality scheduling), only matches returning to the driver, instead of
 *     collecting whole lineage tables.
 *
 * The SQL / DataFrame counterpart is [[org.apache.spark.sql.lineage]].
 */
package object lineage
