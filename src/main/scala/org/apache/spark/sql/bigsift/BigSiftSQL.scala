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

package org.apache.spark.sql.bigsift

import java.io.File

import scala.jdk.CollectionConverters._

import org.apache.spark.bigsift.DeltaDebug
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import org.apache.spark.sql.lineage.TitianSQL

/**
 * Outcome of a SQL BigSift session.
 *
 * @param faultInducingRows the 1-minimal set of base-table rows that reproduce the
 *                          test failure
 * @param faultyOutputs     the output rows flagged faulty by the test
 * @param provenanceSize    number of base-table rows in the backward-trace candidate
 *                          set, before delta debugging
 */
case class BigSiftSQLResult(
    faultInducingRows: Seq[Row],
    faultyOutputs: Seq[Row],
    provenanceSize: Int)

/**
 * BigSift for Spark SQL / DataFrames: isolate the minimal fault-inducing rows of a
 * base table for a faulty query result, by combining Titian's SQL provenance with
 * delta debugging.
 *
 * The query reads a file-backed `baseTable` (the input to minimize) and any number of
 * other tables (kept whole). BigSift:
 *   1. runs the query with capture and finds the faulty output rows;
 *   2. traces them through every branch to their source witnesses, keeping the
 *      `baseTable` rows — the candidate fault-inducing set;
 *   3. delta-debugs that set, re-running the query with the base table restricted to
 *      each subset and re-applying the test, down to a 1-minimal subset.
 *
 * {{{
 * spark.read.parquet(salesPath).createOrReplaceTempView("sales")
 * val result = BigSiftSQL.debug(spark, "sales",
 *   "SELECT category, SUM(amount) AS total FROM sales GROUP BY category",
 *   (r: Row) => r.getLong(1) < 0)                 // faulty = negative total
 * result.faultInducingRows.foreach(println)        // the corrupt sales row(s)
 * }}}
 *
 * Requires the base table to be a file source (Parquet/CSV) so Titian captures its
 * provenance; in-memory `LocalRelation` views are not captured.
 */
object BigSiftSQL {

  def debug(
      spark: SparkSession,
      baseTable: String,
      query: String,
      test: Row => Boolean,
      onProgress: Int => Unit = _ => ()): BigSiftSQLResult = {

    val base = spark.table(baseTable)
    val basePath = filePathOf(spark, baseTable)

    // 1. capture run
    spark.conf.set("spark.titian.sql.capture", "true")
    val df = spark.sql(query)
    val withIds = TitianSQL.collectWithLineage(df)
    val faulty = withIds.filter { case (r, _) => test(r) }
    if (faulty.isEmpty) {
      spark.conf.set("spark.titian.sql.capture", "false")
      return BigSiftSQLResult(Seq.empty, Seq.empty, 0)
    }

    // 2. provenance: witnesses from the base table feeding the faulty outputs
    val sources = TitianSQL.traceAllSources(df, faulty.map(_._2).toSeq)
    TitianSQL.releaseLineage(df)
    spark.conf.set("spark.titian.sql.capture", "false")

    val baseWitnesses = sources.filter { sw =>
      sw.rows.nonEmpty && basePath.exists(p => sw.sourcePath.exists(_.endsWith(p)))
    }
    // candidate base rows: rows of the full table that match a witness (null-safe
    // semi-join per scan, so a pruned scan's columns still select full rows)
    val withRid = base.withColumn("__rid", monotonically_increasing_id()).cache()
    val candidates: Seq[Long] = baseWitnesses.flatMap { sw =>
      val w = spark.createDataFrame(sw.rows.toSeq.distinct.asJava, sw.schema)
        .toDF(sw.schema.fieldNames.map("__w_" + _).toIndexedSeq: _*)
      val cond = sw.schema.fieldNames.map(c => withRid(c) <=> w("__w_" + c)).reduce(_ && _)
      withRid.join(w, cond, "left_semi").select("__rid").collect().map(_.getLong(0)).toSeq
    }.distinct

    // 3. delta-debug: a subset "fails" if re-running the query with the base table
    //    restricted to those rows still yields a faulty output (capture off).
    def reproduces(subset: Seq[Long]): Boolean = subset.nonEmpty && {
      val keep = subset.toSet
      val restricted = withRid.filter(col("__rid").isInCollection(keep)).drop("__rid")
      restricted.createOrReplaceTempView(baseTable)
      try spark.sql(query).collect().exists(test) finally base.createOrReplaceTempView(baseTable)
    }
    val causeRids =
      if (candidates.nonEmpty && reproduces(candidates))
        DeltaDebug.ddmin(candidates, (s: Seq[Long]) => onProgress(s.size))(reproduces)
      else candidates

    val causeRows = withRid.filter(col("__rid").isInCollection(causeRids.toSet))
      .drop("__rid").collect().toSeq
    withRid.unpersist()
    base.createOrReplaceTempView(baseTable)

    BigSiftSQLResult(causeRows, faulty.map(_._1).toSeq, candidates.size)
  }

  /** The base name of a file-backed table's location, for matching witness paths. */
  private def filePathOf(spark: SparkSession, table: String): Option[String] =
    try {
      val plan = spark.table(table).queryExecution.optimizedPlan
      plan.collectFirst {
        case lr: org.apache.spark.sql.execution.datasources.LogicalRelation =>
          lr.relation match {
            case hfr: org.apache.spark.sql.execution.datasources.HadoopFsRelation =>
              new File(hfr.location.rootPaths.head.toString).getName
          }
      }
    } catch { case _: Throwable => None }
}
