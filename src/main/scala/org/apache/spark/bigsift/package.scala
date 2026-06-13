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
 * BigSift — automated isolation of the minimal fault-inducing input records of a Spark
 * job (SoCC '17, FSE '18 demo), built on Titian's data provenance plus delta debugging.
 *
 * Given a job, a test oracle (a predicate marking output records faulty), and the
 * input, BigSift runs the job once with provenance capture, backward-traces the faulty
 * outputs to their candidate inputs, and delta-debugs that set down to the 1-minimal
 * fault-inducing records — re-running the job on subsets and re-applying the oracle.
 *
 *   - [[org.apache.spark.bigsift.BigSift]] — the RDD-API entry point (`run`,
 *     `runWithOracle`, `runWithBigSift`).
 *   - [[org.apache.spark.bigsift.DeltaDebug]] — Zeller & Hildebrandt ddmin with bitmap
 *     memoization (the minimization core).
 *   - [[org.apache.spark.bigsift.TestOracle]] — predefined oracles (minimum, maximum,
 *     k-sigma-from-median, NaN/Null).
 *
 * The Spark SQL counterpart is `org.apache.spark.sql.bigsift.BigSiftSQL`; PySpark is
 * `python/bigsift.py`. See the BigSift page of the docs site.
 */
package object bigsift
