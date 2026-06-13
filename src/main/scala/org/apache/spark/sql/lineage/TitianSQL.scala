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
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.storage.{BlockId, RDDBlockId}
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
 *
 * @groupname capture Capture
 * @groupdesc capture Toggle lineage capture for a session.
 * @groupprio capture 0
 * @groupname api Trace API
 * @groupdesc api Collect results with lineage ids and walk them back to source rows.
 * @groupprio api 1
 * @groupname lifecycle Footprint & lifecycle
 * @groupdesc lifecycle Inspect and release a query's materialized lineage blocks.
 * @groupprio lifecycle 2
 */
object TitianSQL {

  /** Turn lineage capture on for `spark` (equivalent to setting
   * `spark.titian.sql.capture=true`).
   * @group capture */
  def enableCapture(spark: SparkSession): Unit =
    spark.conf.set("spark.titian.sql.capture", "true")
  /** Turn lineage capture off for `spark`.
   * @group capture */
  def disableCapture(spark: SparkSession): Unit =
    spark.conf.set("spark.titian.sql.capture", "false")

  // ---------------------------------------------------------------- plan parsing

  sealed trait SourceInfo
  /**
   * Terminal level: a file scan or a cached relation (InMemoryTableScan).
   * `partOffset`: when the scan runs inside a union's stage, recorded partition ids
   * are stage-wide (scan partition + offset of this child within the union); the
   * re-scan translates back.
   */
  case class ScanSource(scan: SparkPlan, partOffset: Int = 0) extends SourceInfo
  /**
   * A keyed capture level (post tap above an aggregate/join/window/order-by-limit).
   * `hasProbe`: broadcast joins store (keyHash, exactProbeId) per output row; the
   * probe branch is then a [[DirectBranch]] with exact row provenance.
   */
  case class KeyedSource(tapId: Int, hasProbe: Boolean, branches: Seq[BranchLink])
    extends SourceInfo
  /**
   * UNION ALL: 1:1 concatenation — no tap of its own. Each output row keeps the id its
   * child pipeline assigned; child i owns the stage partitions
   * [starts(i), starts(i) + counts(i)). goBack(i) filters ids to that range.
   */
  case class UnionSource(children: Seq[SourceInfo], starts: Seq[Int], counts: Seq[Int])
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

  private def parseSource(plan: SparkPlan, partOffset: Int = 0): SourceInfo = plan match {
    case t: TapScanExec =>
      // CollapseCodegenStages may wrap the leaf in an InputAdapter/ColumnarToRow
      ScanSource(t.child.collectFirst {
        case s: FileSourceScanExec => s: SparkPlan
        case m: org.apache.spark.sql.execution.columnar.InMemoryTableScanExec => m: SparkPlan
        case c: org.apache.spark.sql.execution.adaptive.TableCacheQueryStageExec => c: SparkPlan
      }.getOrElse(
        throw new IllegalStateException(s"No source leaf under scan tap:\n$t")), partOffset)
    case t: TapPostKeyedExec => topBelow(t.child) match {
      // ORDER BY ... LIMIT: a single branch through the tap pair around it. Its
      // internal shuffle is invisible in the tree, so directShuffles cannot find it.
      case Some(top) =>
        val pre = firstPreTap(top.child).getOrElse(
          throw new IllegalStateException(s"No pre tap under order-by-limit:\n$top"))
        KeyedSource(t.tapId, hasProbe = false,
          Seq(HashBranch(pre.tapId, parseSource(pre.child))))
      case None =>
        // Same-pipeline pre taps (fused aggregate / same-pipeline window) are keyed
        // on exactly this level's keys — they take precedence over deeper shuffles,
        // whose partitioning keys may be a subset (clustered-distribution reuse)
        // and hash differently. A fused aggregate over an aligned union carries one
        // pre tap per union child: each becomes a branch in child order.
        val pres = directPreTaps(t.child)
        val branches =
          if (pres.nonEmpty) {
            pres.map(pre => HashBranch(pre.tapId, parseSource(pre.child, partOffset)))
          } else {
            directShuffles(t.child).map(parseBranch)
          }
        KeyedSource(t.tapId, hasProbe = false, branches)
    }
    case t: TapPostBroadcastJoinExec =>
      val bhj = t.child.asInstanceOf[
        org.apache.spark.sql.execution.joins.BroadcastHashJoinExec]
      val buildOnLeft =
        bhj.buildSide == org.apache.spark.sql.catalyst.optimizer.BuildLeft
      val (buildPlan, probePlan) =
        if (buildOnLeft) (bhj.left, bhj.right) else (bhj.right, bhj.left)
      val buildBranch = parseBroadcastBranch(buildPlan)
      // probe side shares this pipeline's tasks: the union offset (if any) carries over
      val probeBranch = DirectBranch(parseSource(probePlan, partOffset))
      // branch indices follow logical join inputs: 0 = left, 1 = right
      val branches =
        if (buildOnLeft) Seq(buildBranch, probeBranch) else Seq(probeBranch, buildBranch)
      KeyedSource(t.tapId, hasProbe = true, branches)
    case e: org.apache.spark.sql.execution.EmptyRelationExec =>
      // AQE replaced an empty subtree: a zero-row source; no ids can reach it and a
      // re-scan trivially yields nothing
      ScanSource(e, partOffset)
    case u: org.apache.spark.sql.execution.UnionExec =>
      // no tap: rows keep their child-pipeline ids; child i's rows live in stage
      // partitions offset by the partition counts of the children before it
      val counts = u.children.map(_.execute().getNumPartitions)
      val starts = counts.scanLeft(partOffset)(_ + _).init
      UnionSource(
        u.children.zip(starts).map { case (c, o) => parseSource(c, o) }, starts, counts)
    case u: UnaryExecNode => parseSource(u.child, partOffset)
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

  /**
   * The TakeOrderedAndProject a post tap was inserted directly above, looking through
   * the InputAdapter that CollapseCodegenStages places at the codegen boundary.
   */
  private def topBelow(p: SparkPlan): Option[
      org.apache.spark.sql.execution.TakeOrderedAndProjectExec] = p match {
    case top: org.apache.spark.sql.execution.TakeOrderedAndProjectExec => Some(top)
    case ia: org.apache.spark.sql.execution.InputAdapter => topBelow(ia.child)
    case _ => None
  }

  /**
   * Same-pipeline pre taps directly below a keyed level — stopping at shuffle
   * boundaries and at other keyed taps (which belong to deeper levels). In child
   * order, so a fused aggregate's per-union-child taps index its branches.
   */
  private def directPreTaps(p: SparkPlan): Seq[TapPreExchangeExec] = p match {
    case t: TapPreExchangeExec => Seq(t)
    case _: ShuffleQueryStageExec | _: ShuffleExchangeExec => Nil
    case _: TapPostKeyedExec | _: TapPostBroadcastJoinExec => Nil
    case other => other.children.flatMap(directPreTaps)
  }

  private def firstPreTap(p: SparkPlan): Option[TapPreExchangeExec] = p match {
    case t: TapPreExchangeExec => Some(t)
    case _: ShuffleQueryStageExec | _: ShuffleExchangeExec => None
    case other => other.children.flatMap(firstPreTap).headOption
  }

  // ---------------------------------------------------------------- block access

  /**
   * Lineage blocks of a tap as an RDD — one compact [[TapBlock]] per partition, read
   * where it lives (preferred locations from the block managers). Trace steps flatMap
   * filtering closures over this, so only matching ids travel to the driver.
   * The driverTrace ablation collects whole blocks to the driver instead and drops
   * the block-locality hints (the pre-P4 behavior).
   */
  private[lineage] def tapBlocks[T <: TapBlock: ClassTag](
      spark: SparkSession, tapId: Int): RDD[T] = {
    val sc = spark.sparkContext
    val master = sc.env.blockManager.master
    val parts = master.getMatchingBlockIds({
      case RDDBlockId(id, _) => id == tapId
      case _ => false
    }, askStorageEndpoints = true).collect {
      case RDDBlockId(_, split) => split
    }.distinct.sorted
    val locations =
      if (org.apache.spark.lineage.TitianAblation.driverTrace) Nil
      else master
        .getLocations(parts.map(p => RDDBlockId(tapId, p): BlockId).toArray)
        .map(_.map(bm => ExecutorCacheTaskLocation(bm.host, bm.executorId).toString))
    new TapBlockRDD[T](sc, tapId, parts, locations)
  }

  /**
   * Run a filtering function over a tap's blocks. Optimized: executor-side flatMap,
   * only matches return. driverTrace ablation: collect whole blocks, filter on the
   * driver.
   */
  private[lineage] def mapBlocks[R: ClassTag](spark: SparkSession, tapId: Int)(
      f: TapBlock => Iterator[R]): Array[R] = {
    val rdd = tapBlocks[TapBlock](spark, tapId)
    if (org.apache.spark.lineage.TitianAblation.driverTrace) {
      rdd.collect().iterator.flatMap(f).toArray
    } else {
      rdd.flatMap(f).collect()
    }
  }

  /** All (packedOutputId, value) pairs of a result tap, sorted by output id. */
  private def resultAssociations(spark: SparkSession, tapId: Int): Array[(Long, Int)] =
    mapBlocks(spark, tapId) { b =>
      val (split, values) = TapBlock.keyedValues(b)
      values.indices.iterator.map(i => (PackIntIntoLong(split, i), values(i)))
    }.sortBy(_._1)

  // ---------------------------------------------------------------- public API

  /** Collect result rows paired with their packed output lineage ids.
   * @group api */
  def collectWithLineage(df: DataFrame): Array[(Row, Long)] = {
    val rows = df.collect() // triggers execution and capture
    // a query AQE collapsed to an empty relation carries no taps — and no rows
    if (rows.isEmpty && !finalPlan(df).exists(_.isInstanceOf[TapResultExec])) {
      return Array.empty
    }
    val graph = captureGraph(df)
    val associations = resultAssociations(graph.spark, graph.resultTapId)
    require(rows.length == associations.length,
      s"capture mismatch: ${rows.length} rows vs ${associations.length} lineage records")
    rows.zip(associations.map(_._1))
  }

  /** Start a trace from packed result-row ids (from [[collectWithLineage]]).
   * @group api */
  def trace(df: DataFrame, outputIds: Seq[Long]): TraceCursor = {
    val graph = captureGraph(df)
    val wanted = outputIds.toSet
    val locals = mapBlocks(graph.spark, graph.resultTapId) { b =>
      val (split, values) = TapBlock.keyedValues(b)
      values.indices.iterator.collect {
        case i if wanted.contains(PackIntIntoLong(split, i)) =>
          PackIntIntoLong(split, values(i))
      }
    }
    new TraceCursor(graph.spark, graph.source, locals, Nil)
  }

  /** Convenience: full backward walk along join input 0 down to the (single) scan.
   * @group api */
  def backward(df: DataFrame, outputIds: Seq[Long]): Array[Long] = {
    var cursor = trace(df, outputIds)
    while (!cursor.atScan) { cursor = cursor.goBack(0) }
    cursor.ids
  }

  /** Convenience for linear (single-scan) plans: resolve input ids to source rows.
   * @group api */
  def showInputs(df: DataFrame, inputIds: Seq[Long]): Array[Row] = {
    val graph = captureGraph(df)
    def findScan(s: SourceInfo): ScanSource = s match {
      case sc: ScanSource => sc
      case KeyedSource(_, _, branches) => findScan(branches.head.upstream)
      case UnionSource(children, _, _) => findScan(children.head)
    }
    val scan = findScan(graph.source)
    showScanRows(graph.spark, scan.scan, inputIds, full = false, scan.partOffset)
  }

  private[lineage] def showScanRows(
      spark: SparkSession,
      scan: SparkPlan,
      inputIds: Seq[Long],
      full: Boolean = false,
      partOffset: Int = 0): Array[Row] =
    showScanRowsWithSchema(spark, scan, inputIds, full, partOffset)._1

  private[lineage] def showScanRowsWithSchema(
      spark: SparkSession,
      scan: SparkPlan,
      inputIds: Seq[Long],
      full: Boolean,
      partOffset: Int = 0): (Array[Row], org.apache.spark.sql.types.StructType) = {
    // Full-source-row mode: re-execute the same scan (same FilePartitions, same
    // in-split row order) but with the complete data schema instead of the pruned
    // one. Only safe when nothing was pushed into the reader that could change the
    // emitted row set/order — otherwise fall back to the pruned view.
    val effective = scan match {
      case fs: FileSourceScanExec
        if full && fs.dataFilters.isEmpty &&
           fs.relation.partitionSchema.isEmpty &&
           fs.requiredSchema != fs.relation.dataSchema =>
        val fullAttrs = fs.relation.dataSchema.fields.toSeq.map { f =>
          org.apache.spark.sql.catalyst.expressions.AttributeReference(
            f.name, f.dataType, f.nullable)()
        }
        fs.copy(output = fullAttrs, requiredSchema = fs.relation.dataSchema)
      case other => other
    }
    val wanted = inputIds.toSet
    // ids were recorded with stage-wide partition numbers; this re-scan's tasks are
    // scan-local — shift by the scan's offset within its stage (non-zero under unions)
    val offset = partOffset
    val matched = (if (effective.supportsColumnar) {
      // columnar scans emit ColumnarBatch; flatten in batch order — identical to the
      // row order ColumnarToRow produced at capture time
      effective.executeColumnar().mapPartitionsInternal { batches =>
        val split = TaskContext.getPartitionId() + offset
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
        val split = TaskContext.getPartitionId() + offset
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
    (matched.map(r => converter(r).asInstanceOf[Row]), effective.schema)
  }

  // ------------------------------------------------------- lifecycle & py4j API

  private def allTapIds(graph: CaptureGraph): Seq[Int] = {
    def tapIds(s: SourceInfo): Seq[Int] = s match {
      case _: ScanSource => Nil
      case UnionSource(children, _, _) => children.flatMap(tapIds)
      case KeyedSource(id, _, branches) => id +: branches.flatMap {
        case HashBranch(pre, up) => pre +: tapIds(up)
        case DirectBranch(up) => tapIds(up)
      }
    }
    graph.resultTapId +: tapIds(graph.source)
  }

  private def allBlockIds(graph: CaptureGraph): Seq[BlockId] = {
    val master = graph.spark.sparkContext.env.blockManager.master
    allTapIds(graph).flatMap { tapId =>
      master.getMatchingBlockIds({
        case RDDBlockId(id, _) => id == tapId
        case _ => false
      }, askStorageEndpoints = true)
    }
  }

  /** Drop all lineage blocks captured for this query (long sessions accumulate).
   * @group lifecycle */
  def releaseLineage(df: DataFrame): Unit = {
    // a query AQE collapsed to an empty relation carries no result tap (and the
    // reachable capture graph with it); nothing to release
    if (!finalPlan(df).exists(_.isInstanceOf[TapResultExec])) return
    val graph = captureGraph(df)
    val master = graph.spark.sparkContext.env.blockManager.master
    allBlockIds(graph).foreach(master.removeBlock)
  }

  /** (memoryBytes, diskBytes) currently held by this query's lineage blocks.
   * @group lifecycle */
  def lineageSize(df: DataFrame): (Long, Long) = {
    if (!finalPlan(df).exists(_.isInstanceOf[TapResultExec])) return (0L, 0L)
    val graph = captureGraph(df)
    val master = graph.spark.sparkContext.env.blockManager.master
    allBlockIds(graph).foldLeft((0L, 0L)) { case ((mem, disk), bid) =>
      val statuses = master.getBlockStatus(bid, askStorageEndpoints = true).values
      (mem + statuses.map(_.memSize).sum, disk + statuses.map(_.diskSize).sum)
    }
  }

  /** Py4J-friendly: packed lineage ids aligned with `df.collect()` row order.
   * @group api */
  def resultIds(df: DataFrame): Array[Long] = {
    val graph = captureGraph(df)
    resultAssociations(graph.spark, graph.resultTapId).map(_._1)
  }

  /** Py4J-friendly trace entry point (accepts a Java list of numbers).
   * @group api */
  def traceJava(df: DataFrame, outputIds: java.util.List[java.lang.Number]): TraceCursor = {
    import scala.jdk.CollectionConverters._
    trace(df, outputIds.asScala.map(_.longValue()).toSeq)
  }

  /**
   * Witnesses at one source level of a full-tree trace. `sourcePath` is the file
   * source's root location when the level is a file scan (None for cached relations
   * and empty-relation leaves), so callers can map witnesses back to their tables.
   */
  case class SourceWitnesses(
      sourcePath: Option[String],
      rows: Array[Row],
      schema: org.apache.spark.sql.types.StructType)

  /**
   * Trace the given result rows backward through EVERY branch (all join inputs, all
   * union children) and resolve witnesses at each reachable source. One entry per
   * scan level — the complete provenance frontier of the traced rows.
   * @group api
   */
  def traceAllSources(df: DataFrame, outputIds: Seq[Long]): Seq[SourceWitnesses] =
    trace(df, outputIds).showAllSources()
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

  // Every walk step below ships a small id/hash set into a flatMap over the tap's
  // blocks: the filtering runs next to the block (TapBlockRDD preferred locations)
  // and only the matches come back to the driver.

  /** Key hashes of the level's rows whose packed output id is in `idSet`. */
  private def keyHashesOf(level: KeyedSource, idSet: Set[Long]): Set[Int] =
    if (level.hasProbe) {
      TitianSQL.mapBlocks(spark, level.tapId) { b =>
        val (split, pairs) = TapBlock.joinPairs(b)
        pairs.indices.iterator.collect {
          case i if idSet.contains(PackIntIntoLong(split, i)) =>
            PackIntIntoLong.getLeft(pairs(i))
        }
      }.toSet
    } else {
      TitianSQL.mapBlocks(spark, level.tapId) { b =>
        val (split, values) = TapBlock.keyedValues(b)
        values.indices.iterator.collect {
          case i if idSet.contains(PackIntIntoLong(split, i)) => values(i)
        }
      }.toSet
    }

  /** Packed output ids of the level's rows whose key hash is in `hashes`. */
  private def idsForKeyHashes(level: KeyedSource, hashes: Set[Int]): Array[Long] =
    if (level.hasProbe) {
      TitianSQL.mapBlocks(spark, level.tapId) { b =>
        val (split, pairs) = TapBlock.joinPairs(b)
        pairs.indices.iterator.collect {
          case i if hashes.contains(PackIntIntoLong.getLeft(pairs(i))) =>
            PackIntIntoLong(split, i)
        }
      }
    } else {
      TitianSQL.mapBlocks(spark, level.tapId) { b =>
        val (split, values) = TapBlock.keyedValues(b)
        values.indices.iterator.collect {
          case i if hashes.contains(values(i)) => PackIntIntoLong(split, i)
        }
      }
    }

  /** One step backward; `branch` selects the join/union input at a multi-input level. */
  def goBack(branch: Int = 0): TraceCursor = source match {
    case ScanSource(_, _) =>
      throw new UnsupportedOperationException("already at the source level")
    case UnionSource(children, starts, counts) =>
      // 1:1 — ids pass through unchanged; the branch owns a stage-partition range
      val (start, end) = (starts(branch), starts(branch) + counts(branch))
      val nextIds = ids.filter { id =>
        val p = PackIntIntoLong.getLeft(id); p >= start && p < end
      }
      new TraceCursor(spark, children(branch), nextIds, (source, ids, branch) :: path)
    case ks @ KeyedSource(tapId, _, branches) =>
      val idSet = ids.toSet
      val nextIds = branches(branch) match {
        case DirectBranch(_) =>
          // exact probe-row provenance: probeId lives in the post-join block, same
          // partition as the output row
          TitianSQL.mapBlocks(spark, tapId) { b =>
            val (split, pairs) = TapBlock.joinPairs(b)
            pairs.indices.iterator.collect {
              case i if idSet.contains(PackIntIntoLong(split, i)) =>
                PackIntIntoLong(split, PackIntIntoLong.getRight(pairs(i)))
            }
          }.distinct
        case HashBranch(preTapId, _) =>
          val hashes = keyHashesOf(ks, idSet)
          TitianSQL.mapBlocks(spark, preTapId) { b =>
            val (split, blockHashes, idLists) = TapBlock.preEntries(b)
            hashes.iterator.flatMap { h =>
              val j = java.util.Arrays.binarySearch(blockHashes, h)
              if (j >= 0) idLists(j).iterator.map(id => PackIntIntoLong(split, id))
              else Iterator.empty
            }
          }.distinct
      }
      new TraceCursor(spark, branches(branch).upstream, nextIds,
        (source, ids, branch) :: path)
  }

  /** One step forward, retracing the branch taken by the last goBack. */
  def goNext(): TraceCursor = path match {
    case Nil => throw new UnsupportedOperationException("already at the result level")
    case (parent: UnionSource, _, _) :: rest =>
      // 1:1 with shared coordinates: the current ids are already union-level ids
      new TraceCursor(spark, parent, ids, rest)
    case (parent: KeyedSource, _, branch) :: rest =>
      val idSet = ids.toSet
      val parentIds = parent.branches(branch) match {
        case DirectBranch(_) =>
          TitianSQL.mapBlocks(spark, parent.tapId) { b =>
            val (split, pairs) = TapBlock.joinPairs(b)
            pairs.indices.iterator.collect {
              case i if idSet.contains(
                  PackIntIntoLong(split, PackIntIntoLong.getRight(pairs(i)))) =>
                PackIntIntoLong(split, i)
            }
          }
        case HashBranch(preTapId, _) =>
          val hashes =
            TitianSQL.mapBlocks(spark, preTapId) { b =>
              val (split, blockHashes, idLists) = TapBlock.preEntries(b)
              blockHashes.indices.iterator.collect {
                case j if idLists(j)
                  .exists(id => idSet.contains(PackIntIntoLong(split, id))) =>
                  blockHashes(j)
              }
            }.toSet
          idsForKeyHashes(parent, hashes)
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
    case ScanSource(scan, off) =>
      TitianSQL.showScanRows(spark, scan, ids.toSeq, full, off)
    case _ => throw new UnsupportedOperationException(
      "show() resolves source rows — goBack to a scan level first")
  }

  /**
   * Resolve witnesses at every source level reachable from here, following every
   * branch of every join/union below — the full provenance frontier of `ids`.
   */
  def showAllSources(): Seq[TitianSQL.SourceWitnesses] = source match {
    case ScanSource(scan, off) =>
      val (rows, schema) =
        TitianSQL.showScanRowsWithSchema(spark, scan, ids.toSeq, full = false, off)
      val path = scan match {
        case fs: FileSourceScanExec =>
          fs.relation.location.rootPaths.headOption.map(_.toString)
        case _ => None
      }
      Seq(TitianSQL.SourceWitnesses(path, rows, schema))
    case KeyedSource(_, _, branches) =>
      branches.indices.flatMap(i => goBack(i).showAllSources())
    case UnionSource(children, _, _) =>
      children.indices.flatMap(i => goBack(i).showAllSources())
  }

  /** Py4J-friendly: source rows as JSON strings. */
  def showJson(full: Boolean): Array[String] = source match {
    case ScanSource(scan, off) =>
      val (rows, schema) =
        TitianSQL.showScanRowsWithSchema(spark, scan, ids.toSeq, full, off)
      import scala.jdk.CollectionConverters._
      spark.createDataFrame(rows.toSeq.asJava, schema).toJSON.collect()
    case _ => throw new UnsupportedOperationException(
      "show() resolves source rows — goBack to a scan level first")
  }

}

/**
 * Reads materialized tap blocks back as an RDD — one partition per captured block,
 * preferring the executor that holds the block so trace filtering runs in place.
 */
private[lineage] class TapBlockRDD[T: ClassTag](
    sc: SparkContext, tapId: Int, parts: Seq[Int], locations: Seq[Seq[String]])
  extends RDD[T](sc, Nil) {

  override def getPartitions: Array[Partition] =
    parts.zipWithIndex.map { case (block, i) =>
      new TapBlockPartition(i, block)
    }.toArray

  override def getPreferredLocations(split: Partition): Seq[String] =
    if (split.index < locations.length) locations(split.index) else Nil

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
