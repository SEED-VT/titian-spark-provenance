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

import scala.reflect.ClassTag

import org.apache.spark.{Partition, SparkContext, SparkEnv, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.util.PackIntIntoLong

/**
 * Driver-side trace model for SQL lineage (phases 1-3).
 *
 * The executed plan is parsed into a chain/tree of capture levels:
 *   ResultTap -> [ KeyedSource (post-tap above agg/join)
 *                    -> branches: PreExchange tap -> upstream source (recursive) ]*
 *             -> ScanSource (terminal; resolved by deterministic re-scan)
 *
 * Backward/forward traversal mirrors the RDD LineageRDD API: `goBack(path)` follows
 * join input `path`; `show()` resolves source rows at a scan level.
 */
object TitianSQL {

  def enableCapture(spark: SparkSession): Unit =
    spark.conf.set("spark.titian.sql.capture", "true")
  def disableCapture(spark: SparkSession): Unit =
    spark.conf.set("spark.titian.sql.capture", "false")

  // ---------------------------------------------------------------- plan parsing

  sealed trait SourceInfo
  case class ScanSource(scan: FileSourceScanExec) extends SourceInfo
  /**
   * A keyed capture level (post tap above an aggregate/join/window).
   * `hasProbe`: broadcast joins store (keyHash, exactProbeId) per output row; the
   * probe branch is then a [[DirectBranch]] with exact row provenance.
   */
  case class KeyedSource(tapId: Int, hasProbe: Boolean, branches: Seq[BranchLink])
    extends SourceInfo

  sealed trait BranchLink { def upstream: SourceInfo }
  /** Across a shuffle/broadcast: resolved via the branch's pre-exchange tap. */
  case class HashBranch(preTapId: Int, upstream: SourceInfo) extends BranchLink
  /** Same-pipeline probe side of a broadcast join: ids are exact. */
  case class DirectBranch(upstream: SourceInfo) extends BranchLink

  case class CaptureGraph(spark: SparkSession, resultTapId: Int, source: SourceInfo)

  private def finalPlan(df: DataFrame): SparkPlan = unwrap(df.queryExecution.executedPlan)

  /** Strip AQE wrappers (AdaptiveSparkPlanExec, ResultQueryStageExec are leaf-like). */
  private def unwrap(p: SparkPlan): SparkPlan = p match {
    case a: AdaptiveSparkPlanExec => unwrap(a.executedPlan)
    case other =>
      other.getClass.getSimpleName match {
        case "ResultQueryStageExec" =>
          // Spark 4 wraps the final stage; access its inner plan reflectively to stay
          // compatible if the field moves
          val m = other.getClass.getMethod("plan")
          unwrap(m.invoke(other).asInstanceOf[SparkPlan])
        case _ => other
      }
  }

  def captureGraph(df: DataFrame): CaptureGraph = {
    val plan = finalPlan(df)
    val result = plan.collectFirst { case t: TapResultExec => t }.getOrElse {
      throw new IllegalStateException(
        "No Titian result tap in the executed plan — was the query run with " +
          "spark.titian.sql.capture=true and the TitianSQLExtension registered?")
    }
    CaptureGraph(df.sparkSession, result.tapId, parseSource(result.child))
  }

  private def parseSource(plan: SparkPlan): SourceInfo = plan match {
    case t: TapScanExec =>
      // CollapseCodegenStages may wrap the scan in an InputAdapter
      ScanSource(t.child.collectFirst { case s: FileSourceScanExec => s }.getOrElse(
        throw new IllegalStateException(s"No file scan under scan tap:\n$t")))
    case t: TapPostKeyedExec =>
      KeyedSource(t.tapId, hasProbe = false, directShuffles(t.child).map(parseBranch))
    case t: TapPostBroadcastJoinExec =>
      val bhj = t.child.asInstanceOf[
        org.apache.spark.sql.execution.joins.BroadcastHashJoinExec]
      val buildOnLeft =
        bhj.buildSide == org.apache.spark.sql.catalyst.optimizer.BuildLeft
      val (buildPlan, probePlan) =
        if (buildOnLeft) (bhj.left, bhj.right) else (bhj.right, bhj.left)
      val buildBranch = parseBroadcastBranch(buildPlan)
      val probeBranch = DirectBranch(parseSource(probePlan))
      // branch indices follow logical join inputs: 0 = left, 1 = right
      val branches =
        if (buildOnLeft) Seq(buildBranch, probeBranch) else Seq(probeBranch, buildBranch)
      KeyedSource(t.tapId, hasProbe = true, branches)
    case u: UnaryExecNode => parseSource(u.child)
    case other =>
      throw new IllegalStateException(
        s"Cannot parse capture graph at ${other.getClass.getSimpleName}:\n$other")
  }

  private def parseBroadcastBranch(p: SparkPlan): HashBranch = {
    val exchangeChild = p.collectFirst {
      case b: org.apache.spark.sql.execution.exchange.BroadcastExchangeExec => b.child
    }.orElse {
      // AQE wraps the broadcast in a leaf BroadcastQueryStageExec
      p.collectFirst {
        case s: org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec =>
          s.plan match {
            case b: org.apache.spark.sql.execution.exchange.BroadcastExchangeExec => b.child
            case inner => inner
          }
      }
    }.getOrElse(throw new IllegalStateException(s"No broadcast exchange under:\n$p"))
    val pre = firstPreTap(exchangeChild).getOrElse {
      throw new IllegalStateException(s"No pre tap under broadcast:\n$exchangeChild")
    }
    HashBranch(pre.tapId, parseSource(pre.child))
  }

  /** Shuffle boundaries directly feeding this operator (not nested deeper ones). */
  private def directShuffles(p: SparkPlan): Seq[SparkPlan] = p match {
    case s: ShuffleQueryStageExec => Seq(s)
    case e: ShuffleExchangeExec => Seq(e)
    case other => other.children.flatMap(directShuffles)
  }

  private def parseBranch(shuffle: SparkPlan): HashBranch = {
    val mapSide = shuffle match {
      case s: ShuffleQueryStageExec => s.plan match {
        case e: ShuffleExchangeExec => e.child
        case p => p
      }
      case e: ShuffleExchangeExec => e.child
      case p => p
    }
    val pre = firstPreTap(mapSide).getOrElse {
      throw new IllegalStateException(s"No pre-exchange tap under shuffle:\n$mapSide")
    }
    HashBranch(pre.tapId, parseSource(pre.child))
  }

  private def firstPreTap(p: SparkPlan): Option[TapPreExchangeExec] = p match {
    case t: TapPreExchangeExec => Some(t)
    case _: ShuffleQueryStageExec | _: ShuffleExchangeExec => None
    case other => other.children.flatMap(firstPreTap).headOption
  }

  // ---------------------------------------------------------------- block access

  private def tapPartitions(sc: SparkContext, tapId: Int): Seq[Int] = {
    sc.env.blockManager.master.getMatchingBlockIds({
      case RDDBlockId(id, _) => id == tapId
      case _ => false
    }, askStorageEndpoints = true).collect {
      case RDDBlockId(_, split) => split
    }.distinct.sorted
  }

  private def blocks[T: ClassTag](spark: SparkSession, tapId: Int): Array[T] = {
    val sc = spark.sparkContext
    new TapBlockRDD[T](sc, tapId, tapPartitions(sc, tapId)).collect()
  }

  // ---------------------------------------------------------------- public API

  /** Collect result rows paired with their packed output lineage ids. */
  def collectWithLineage(df: DataFrame): Array[(Row, Long)] = {
    val rows = df.collect() // triggers execution and capture
    val graph = captureGraph(df)
    val associations =
      blocks[(Long, Long)](graph.spark, graph.resultTapId).sortBy(_._1)
    require(rows.length == associations.length,
      s"capture mismatch: ${rows.length} rows vs ${associations.length} lineage records")
    rows.zip(associations.map(_._1))
  }

  /** Start a trace from packed result-row ids (from [[collectWithLineage]]). */
  def trace(df: DataFrame, outputIds: Seq[Long]): TraceCursor = {
    val graph = captureGraph(df)
    val wanted = outputIds.toSet
    val locals = blocks[(Long, Long)](graph.spark, graph.resultTapId)
      .filter(r => wanted.contains(r._1))
      .map(r => PackIntIntoLong(PackIntIntoLong.getLeft(r._1), r._2.toInt))
    new TraceCursor(graph.spark, graph.source, locals, Nil)
  }

  /** Convenience: full backward walk along join input 0 down to the (single) scan. */
  def backward(df: DataFrame, outputIds: Seq[Long]): Array[Long] = {
    var cursor = trace(df, outputIds)
    while (!cursor.atScan) { cursor = cursor.goBack(0) }
    cursor.ids
  }

  /** Convenience for linear (single-scan) plans: resolve input ids to source rows. */
  def showInputs(df: DataFrame, inputIds: Seq[Long]): Array[Row] = {
    val graph = captureGraph(df)
    def findScan(s: SourceInfo): ScanSource = s match {
      case sc: ScanSource => sc
      case KeyedSource(_, _, branches) => findScan(branches.head.upstream)
    }
    showScanRows(graph.spark, findScan(graph.source).scan, inputIds)
  }

  private[lineage] def showScanRows(
      spark: SparkSession,
      scan: FileSourceScanExec,
      inputIds: Seq[Long],
      full: Boolean = false): Array[Row] = {
    // Full-source-row mode: re-execute the same scan (same FilePartitions, same
    // in-split row order) but with the complete data schema instead of the pruned
    // one. Only safe when nothing was pushed into the reader that could change the
    // emitted row set/order — otherwise fall back to the pruned view.
    val effective =
      if (full && scan.dataFilters.isEmpty &&
          scan.relation.partitionSchema.isEmpty &&
          scan.requiredSchema != scan.relation.dataSchema) {
        val fullAttrs = scan.relation.dataSchema.fields.toSeq.map { f =>
          org.apache.spark.sql.catalyst.expressions.AttributeReference(
            f.name, f.dataType, f.nullable)()
        }
        scan.copy(output = fullAttrs, requiredSchema = scan.relation.dataSchema)
      } else {
        scan
      }
    val wanted = inputIds.toSet
    val matched = (if (effective.supportsColumnar) {
      // columnar scans emit ColumnarBatch; flatten in batch order — identical to the
      // row order ColumnarToRow produced at capture time
      effective.executeColumnar().mapPartitionsInternal { batches =>
        val split = TaskContext.getPartitionId()
        var idx = -1
        batches.flatMap { batch =>
          scala.jdk.CollectionConverters.IteratorHasAsScala(batch.rowIterator())
            .asScala.flatMap { row =>
              idx += 1
              if (wanted.contains(PackIntIntoLong(split, idx))) {
                Iterator.single(row.copy())
              } else Iterator.empty
            }
        }
      }
    } else {
      effective.execute().mapPartitionsInternal { iter =>
        val split = TaskContext.getPartitionId()
        var idx = -1
        iter.flatMap { row =>
          idx += 1
          if (wanted.contains(PackIntIntoLong(split, idx))) {
            Iterator.single(row.copy())
          } else Iterator.empty
        }
      }
    }).collect()
    val converter =
      org.apache.spark.sql.catalyst.CatalystTypeConverters
        .createToScalaConverter(effective.schema)
    matched.map(r => converter(r).asInstanceOf[Row])
  }
}

/**
 * A position in the backward/forward walk. `ids` are packed (partition, localIdx)
 * coordinates at the current level; `path` remembers the branches taken so goNext can
 * retrace forward.
 */
class TraceCursor private[lineage] (
    spark: SparkSession,
    source: TitianSQL.SourceInfo,
    val ids: Array[Long],
    path: List[(TitianSQL.SourceInfo, Array[Long], Int)]) {

  import TitianSQL._

  def atScan: Boolean = source.isInstanceOf[ScanSource]

  /** (packedOut, keyHash) pairs at a keyed level, regardless of block shape. */
  private def keyedRows(level: KeyedSource): Array[(Long, Int)] =
    if (level.hasProbe) {
      blocksOf[(Long, (Int, Int))](level.tapId).map(r => (r._1, r._2._1))
    } else {
      blocksOf[(Long, Long)](level.tapId).map(r => (r._1, r._2.toInt))
    }

  /** One step backward; `branch` selects the join input at a multi-input level. */
  def goBack(branch: Int = 0): TraceCursor = source match {
    case ScanSource(_) =>
      throw new UnsupportedOperationException("already at the source level")
    case ks @ KeyedSource(tapId, hasProbe, branches) =>
      val idSet = ids.toSet
      val nextIds = branches(branch) match {
        case DirectBranch(_) =>
          // exact probe-row provenance: probeId lives in the post-join block, same
          // partition as the output row
          blocksOf[(Long, (Int, Int))](tapId)
            .filter(r => idSet.contains(r._1))
            .map(r => PackIntIntoLong(PackIntIntoLong.getLeft(r._1), r._2._2))
            .distinct
        case HashBranch(preTapId, _) =>
          val hashes = keyedRows(ks).filter(r => idSet.contains(r._1)).map(_._2).toSet
          blocksOfPartitioned[(Int, Array[Int])](preTapId)
            .flatMap { case (mapPart, rows) =>
              rows.filter(r => hashes.contains(r._1))
                .flatMap(_._2.map(id => PackIntIntoLong(mapPart, id)))
            }.distinct
      }
      new TraceCursor(spark, branches(branch).upstream, nextIds,
        (source, ids, branch) :: path)
  }

  /** One step forward, retracing the branch taken by the last goBack. */
  def goNext(): TraceCursor = path match {
    case Nil => throw new UnsupportedOperationException("already at the result level")
    case (parent: KeyedSource, _, branch) :: rest =>
      val idSet = ids.toSet
      val parentIds = parent.branches(branch) match {
        case DirectBranch(_) =>
          blocksOf[(Long, (Int, Int))](parent.tapId)
            .filter { r =>
              idSet.contains(PackIntIntoLong(PackIntIntoLong.getLeft(r._1), r._2._2))
            }.map(_._1)
        case HashBranch(preTapId, _) =>
          val hashes = blocksOfPartitioned[(Int, Array[Int])](preTapId)
            .flatMap { case (mapPart, rows) =>
              rows.filter(_._2.exists(id => idSet.contains(PackIntIntoLong(mapPart, id))))
                .map(_._1)
            }.toSet
          keyedRows(parent).filter(r => hashes.contains(r._2)).map(_._1)
      }
      new TraceCursor(spark, parent, parentIds, rest)
    case _ => throw new IllegalStateException("corrupt trace path")
  }

  /** Resolve source rows; only valid at a scan level. */
  def show(): Array[Row] = show(full = false)

  /**
   * Resolve source rows; with `full = true`, return complete source records (all
   * columns) instead of the query's pruned view, when the scan shape allows it.
   */
  def show(full: Boolean): Array[Row] = source match {
    case ScanSource(scan) => TitianSQL.showScanRows(spark, scan, ids.toSeq, full)
    case _ => throw new UnsupportedOperationException(
      "show() resolves source rows — goBack to a scan level first")
  }

  private def blocksOf[T: ClassTag](tapId: Int): Array[T] = {
    val sc = spark.sparkContext
    new TapBlockRDD[T](sc, tapId, tapPartitionsOf(tapId)).collect()
  }

  private def blocksOfPartitioned[T: ClassTag](tapId: Int): Array[(Int, Array[T])] = {
    val sc = spark.sparkContext
    val parts = tapPartitionsOf(tapId)
    parts.map { p =>
      p -> new TapBlockRDD[T](sc, tapId, Seq(p)).collect()
    }.toArray
  }

  private def tapPartitionsOf(tapId: Int): Seq[Int] = {
    spark.sparkContext.env.blockManager.master.getMatchingBlockIds({
      case RDDBlockId(id, _) => id == tapId
      case _ => false
    }, askStorageEndpoints = true).collect {
      case RDDBlockId(_, split) => split
    }.distinct.sorted
  }
}

/** Reads materialized tap blocks back as an RDD (one partition per captured block). */
private[lineage] class TapBlockRDD[T: ClassTag](
    sc: SparkContext, tapId: Int, parts: Seq[Int])
  extends RDD[T](sc, Nil) {

  override def getPartitions: Array[Partition] =
    parts.zipWithIndex.map { case (block, i) =>
      new TapBlockPartition(i, block)
    }.toArray

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val block = split.asInstanceOf[TapBlockPartition].block
    SparkEnv.get.blockManager.get[Any](RDDBlockId(tapId, block)) match {
      case Some(result) => result.data.asInstanceOf[Iterator[T]]
      case None => Iterator.empty
    }
  }
}

private[lineage] class TapBlockPartition(val index: Int, val block: Int)
  extends Partition
