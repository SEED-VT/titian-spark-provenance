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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.expressions.{Coalesce, Expression}
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, SinglePartition}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, ExpandExec, FileSourceScanExec, FilterExec, GenerateExec, ProjectExec, SortExec, SparkPlan, TakeOrderedAndProjectExec, TitianJoinKeys, UnionExec}
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, BroadcastQueryStageExec, ShuffleQueryStageExec, TableCacheQueryStageExec}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, Exchange, ShuffleExchangeExec}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashedRelationBroadcastMode, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.python.{ArrowEvalPythonExec, BatchEvalPythonExec}
import org.apache.spark.sql.execution.window.WindowExec

/**
 * Entry point: register with `spark.sql.extensions=org.apache.spark.sql.lineage.TitianSQLExtension`
 * and toggle capture per query with `spark.titian.sql.capture=true`.
 *
 * Injection point: a ColumnarRule's pre-transitions hook. It runs inside plan
 * preparations *before* CollapseCodegenStages — so inserted taps get fused into
 * whole-stage codegen — and it runs in both execution modes: without AQE once over the
 * whole plan, with AQE per query stage (map stages arrive rooted at their
 * ShuffleExchangeExec; the final stage arrives rooted at the result operator).
 */
class TitianSQLExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectColumnar(session => TitianColumnarRule(session))
  }
}

case class TitianColumnarRule(session: SparkSession) extends ColumnarRule {
  override def preColumnarTransitions: Rule[SparkPlan] = InsertTitianTaps(session)
}

class TitianUnsupportedOperatorException(plan: SparkPlan)
  extends UnsupportedOperationException(
    s"Titian SQL capture does not support this operator yet (SQL_LINEAGE_PLAN phase " +
      s"coverage): ${plan.getClass.getSimpleName}. Disable spark.titian.sql.capture " +
      s"for this query or restructure it.\nPlan node: ${plan.simpleStringWithNodeId()}")

/**
 * Tap insertion (phases 1-3):
 *  - `TapScanExec` above every row-based file scan;
 *  - `TapPreExchangeExec` on the map side of every shuffle — below the partial
 *    aggregate when one exists (pre-combine rows), keyed by its grouping expressions,
 *    otherwise directly below the exchange keyed by its hash-partitioning expressions;
 *  - `TapPostKeyedExec` above every final aggregate / shuffle join, keyed by grouping
 *    or join keys evaluated on the operator output;
 *  - `TapResultExec` at the root of the final (non-exchange-rooted) stage.
 *
 * Everything outside the supported set fails loudly (SQL_LINEAGE_PLAN caveat 4).
 */
case class InsertTitianTaps(session: SparkSession) extends Rule[SparkPlan] with Logging {

  private def captureEnabled: Boolean =
    session.conf.get("spark.titian.sql.capture", "false").toBoolean

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!captureEnabled) return plan
    // DDL / commands (CREATE VIEW, SHOW, writes-as-commands) are not data queries
    if (isCommand(plan)) return plan
    // Display queries (df.show(), bare LIMIT n) are not provenance targets: positional
    // row selection is control flow Titian does not capture, and show() is ubiquitous
    // in notebooks — skip capture rather than fail. ORDER BY ... LIMIT
    // (TakeOrderedAndProject) IS captured: it selects rows by sort key, which taps key
    // on like any shuffle boundary.
    if (isDisplayOnly(plan)) return plan
    // With AQE the outer preparation pipeline passes the AdaptiveSparkPlanExec wrapper
    // through this rule too; the real injection happens on the per-stage invocations
    // from inside AQE.
    if (plan.exists(_.isInstanceOf[
      org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec])) return plan
    // Idempotency: AQE may re-prepare a subtree that already carries taps
    if (plan.exists(_.isInstanceOf[TitianTapExec])) return plan

    validateSupported(plan)

    var tapped = plan.transformUp {
      // input ids at the sources. For columnar scans (Parquet/ORC) the transition
      // inserter, which runs after this rule, places ColumnarToRowExec between the
      // scan and this row-based tap — ids are assigned at the row boundary.
      case scan: FileSourceScanExec =>
        TapScanExec(scan)

      // a cached DataFrame is a source boundary: trace stops at (and shows) the
      // cached rows; provenance does not cross into the plan that built the cache.
      // Under AQE the cache arrives wrapped in a TableCacheQueryStage leaf.
      case imts: InMemoryTableScanExec =>
        TapScanExec(imts)
      case tc: TableCacheQueryStageExec =>
        TapScanExec(tc)

      // Python UDF eval is 1:1 and order-preserving but batched, which breaks
      // currentInputId threading — record the id sequence below, replay it above
      case py @ (_: BatchEvalPythonExec | _: ArrowEvalPythonExec)
        if !py.children.head.isInstanceOf[TapSeqExec] =>
        val pairId = session.sparkContext.newRddId()
        TapReseqExec(pairId,
          py.withNewChildren(Seq(TapSeqExec(pairId, py.children.head))))

      // map-side capture: below the partial aggregate when the exchange feeds one
      case ex: ShuffleExchangeExec => ex.child match {
        case agg: HashAggregateExec if !agg.child.isInstanceOf[TapPreExchangeExec] =>
          if (!idSafeFromShuffle(agg.child)) {
            throw new TitianUnsupportedOperatorException(ex)
          }
          val pre = TapPreExchangeExec(
            session.sparkContext.newRddId(), visibleGroupKeys(agg.groupingExpressions),
            agg.child)
          ex.withNewChildren(Seq(agg.withNewChildren(Seq(pre))))
        case agg: HashAggregateExec => ex
        case _: TapPreExchangeExec => ex
        case other =>
          val keys = ex.outputPartitioning match {
            case h: HashPartitioning => h.expressions
            case SinglePartition => Seq.empty
            case p => throw new TitianUnsupportedOperatorException(ex)
          }
          if (!idSafeFromShuffle(other)) {
            throw new TitianUnsupportedOperatorException(ex)
          }
          ex.withNewChildren(
            Seq(TapPreExchangeExec(session.sparkContext.newRddId(), keys, other)))
      }

      // build side of a broadcast join: capture (rewritten-key hash -> input ids)
      case bex: BroadcastExchangeExec
        if !bex.child.isInstanceOf[TapPreExchangeExec] => bex.mode match {
        case HashedRelationBroadcastMode(keys, _) =>
          if (!idSafeFromShuffle(bex.child)) {
            throw new TitianUnsupportedOperatorException(bex)
          }
          bex.withNewChildren(
            Seq(TapPreExchangeExec(session.sparkContext.newRddId(), keys, bex.child)))
        case _ => throw new TitianUnsupportedOperatorException(bex)
      }
    }

    // reduce-side capture above final aggregates and shuffle joins
    tapped = tapped.transformUp {
      // Output-side aggregates need a post tap. Two shapes:
      //  - reduce side of a shuffle: the matching pre tap was inserted on the map
      //    side (pass 1);
      //  - a fused pair (final directly over partial, no shuffle — the input was
      //    already partitioned on the grouping keys, e.g. a GROUP BY over a union
      //    of identically-grouped aggregates): the aggregate still breaks 1:1 id
      //    threading, so it gets its own pre tap below the pair. Without this the
      //    next tap up records stale ids — silently wrong lineage.
      case agg: HashAggregateExec
        if (isReduceSide(agg.child) || fusedPartialOf(agg).isDefined) &&
          !agg.isInstanceOf[TapPostKeyedExec] =>
        // The grouping values may surface in the output under an Alias (e.g.
        // GROUP BY upper(c) ... SELECT upper(c) AS x): resolve each grouping attr to
        // the output attribute that carries its value. Grouping attrs pruned from the
        // output (GROUP BY x, SELECT only aggregates) are appended to the aggregate's
        // result expressions so the tap can key on them, and projected away above —
        // the visible schema is unchanged.
        val groupAttrs = visibleGroupKeys(agg.groupingExpressions).map(_.toAttribute)
        val resolved = groupAttrs.map { g =>
          agg.resultExpressions.collectFirst {
            case ar: org.apache.spark.sql.catalyst.expressions.AttributeReference
              if ar.exprId == g.exprId => ar: Expression
            case al @ org.apache.spark.sql.catalyst.expressions.Alias(
              ar: org.apache.spark.sql.catalyst.expressions.AttributeReference, _)
              if ar.exprId == g.exprId => al.toAttribute: Expression
          }
        }
        val keyAttrs = groupAttrs.zip(resolved).map { case (g, r) => r.getOrElse(g) }
        val missing = groupAttrs.zip(resolved).collect { case (g, None) => g }
        val body = fusedPartialOf(agg) match {
          case Some(partial) if !isReduceSide(agg.child) =>
            val keys = visibleGroupKeys(partial.groupingExpressions)
            partial.child match {
              case u: UnionExec =>
                // Fusion implies the union is partitioner-ALIGNED at runtime (child
                // partitions zipped, not concatenated), so positional ids are
                // ambiguous across children. One pre tap per child — keyed on the
                // outer grouping keys rebased onto that child's output — makes the
                // branch the tap identity instead of a partition range.
                val tappedChildren = u.children.map { c =>
                  if (!idSafeFromShuffle(c)) {
                    throw new TitianUnsupportedOperatorException(agg)
                  }
                  val rebased = keys.map(_.transform {
                    case ar: org.apache.spark.sql.catalyst.expressions.AttributeReference =>
                      val j = u.output.indexWhere(_.exprId == ar.exprId)
                      if (j < 0) throw new TitianUnsupportedOperatorException(agg)
                      c.output(j)
                  })
                  TapPreExchangeExec(session.sparkContext.newRddId(), rebased, c)
                }
                agg.withNewChildren(Seq(partial.withNewChildren(
                  Seq(u.withNewChildren(tappedChildren)))))
              case other =>
                if (!idSafeFromShuffle(other)) {
                  throw new TitianUnsupportedOperatorException(agg)
                }
                val pre = TapPreExchangeExec(
                  session.sparkContext.newRddId(), keys, other)
                agg.withNewChildren(Seq(partial.withNewChildren(Seq(pre))))
            }
          case _ => agg
        }
        if (missing.isEmpty) {
          TapPostKeyedExec(session.sparkContext.newRddId(), keyAttrs, body)
        } else {
          val augmented = body.asInstanceOf[HashAggregateExec]
            .copy(resultExpressions = agg.resultExpressions ++ missing)
          ProjectExec(agg.output,
            TapPostKeyedExec(session.sparkContext.newRddId(), keyAttrs, augmented))
        }

      case j: SortMergeJoinExec =>
        TapPostKeyedExec(session.sparkContext.newRddId(),
          shuffleJoinKeys(j.joinType, j.leftKeys, j.rightKeys), j)

      case j: ShuffledHashJoinExec =>
        TapPostKeyedExec(session.sparkContext.newRddId(),
          shuffleJoinKeys(j.joinType, j.leftKeys, j.rightKeys), j)

      case j: BroadcastHashJoinExec =>
        // streamed keys, rewritten the same way as the broadcast mode's build keys,
        // so the output hash matches the build-side pre-broadcast tap
        val buildRight =
          j.buildSide == org.apache.spark.sql.catalyst.optimizer.BuildRight
        val streamedKeys = if (buildRight) j.leftKeys else j.rightKeys
        // probe ids are recorded as exact row provenance: they must not arrive from
        // an untapped shuffle (AQE's SMJ->BHJ conversion can produce this shape)
        val streamedPlan = if (buildRight) j.left else j.right
        if (!idSafeFromShuffle(streamedPlan)) {
          throw new TitianUnsupportedOperatorException(j)
        }
        // mark tap on the streamed side: saves the probe id as the row enters the
        // join, so fan-out matches all record the right probe row
        val tapId = session.sparkContext.newRddId()
        val marked = TapProbeMarkExec(tapId, streamedPlan)
        val joined =
          if (buildRight) j.withNewChildren(Seq(marked, j.right))
          else j.withNewChildren(Seq(j.left, marked))
        TapPostBroadcastJoinExec(tapId, tapId,
          TitianJoinKeys.rewrite(streamedKeys), joined)

      case w: WindowExec if isReduceSide(w.child) =>
        // window-partition (peer set) granularity: key outputs by the partition spec
        TapPostKeyedExec(session.sparkContext.newRddId(), w.partitionSpec, w)

      case w: WindowExec =>
        // same-pipeline window (input already partitioned, no shuffle directly
        // below): like the fused aggregate, it buffers per partition and breaks 1:1
        // threading — give the level its own pre tap
        if (!idSafeFromShuffle(w.child)) {
          throw new TitianUnsupportedOperatorException(w)
        }
        val pre = TapPreExchangeExec(
          session.sparkContext.newRddId(), w.partitionSpec, w.child)
        TapPostKeyedExec(session.sparkContext.newRddId(), w.partitionSpec,
          w.withNewChildren(Seq(pre)))

      // ORDER BY ... LIMIT selects rows by sort key: treat it exactly like a shuffle
      // boundary — pre tap below keyed by the sort expressions, post tap above keyed
      // by the same columns in the output (key-hash granularity; exact when the sort
      // key is unique). Sort keys pruned by the projection (ORDER BY on an unselected
      // column) are carried through the projection for the tap and stripped above.
      // The operator's internal single-partition shuffle stays invisible.
      case top: TakeOrderedAndProjectExec
        if !top.child.isInstanceOf[TapPreExchangeExec] =>
        val sortAttrs = topSortAttrs(top)
        if (!idSafeFromShuffle(top.child)) {
          throw new TitianUnsupportedOperatorException(top)
        }
        val resolved = sortAttrs.map { sa =>
          top.projectList.collectFirst {
            case a: org.apache.spark.sql.catalyst.expressions.AttributeReference
              if a.exprId == sa.exprId => a: Expression
            case al @ org.apache.spark.sql.catalyst.expressions.Alias(
              a: org.apache.spark.sql.catalyst.expressions.AttributeReference, _)
              if a.exprId == sa.exprId => al.toAttribute: Expression
          }
        }
        val outKeys = sortAttrs.zip(resolved).map { case (sa, r) => r.getOrElse(sa) }
        val missing = sortAttrs.zip(resolved).collect { case (sa, None) => sa }
        val pre = TapPreExchangeExec(
          session.sparkContext.newRddId(), sortAttrs, top.child)
        val augmented = top.copy(projectList = top.projectList ++ missing)
          .withNewChildren(Seq(pre))
        val tapped =
          TapPostKeyedExec(session.sparkContext.newRddId(), outKeys, augmented)
        if (missing.isEmpty) tapped else ProjectExec(top.output, tapped)
    }

    // result tap: only on the final (non-exchange-rooted) stage, exactly once
    tapped match {
      case _: Exchange => tapped
      case _ if tapped.isInstanceOf[TapResultExec] => tapped
      case _ if !tapped.exists(_.isInstanceOf[TapScanExec]) &&
                !tapped.exists(_.isInstanceOf[TapPostKeyedExec]) => tapped
      case root =>
        if (!idSafeFromShuffle(root)) {
          throw new TitianUnsupportedOperatorException(root)
        }
        val tapId = session.sparkContext.newRddId()
        logInfo(s"Titian SQL capture: result tap $tapId inserted")
        TapResultExec(tapId, root)
    }
  }

  private def isDisplayOnly(p: SparkPlan): Boolean = p.exists { node =>
    node.isInstanceOf[org.apache.spark.sql.execution.CollectLimitExec] ||
      node.getClass.getSimpleName.startsWith("GlobalLimit") ||
      node.getClass.getSimpleName.startsWith("LocalLimit")
  }

  /**
   * The sort keys of an ORDER BY ... LIMIT as attributes of its child. Inline
   * computed sort expressions (Catalyst normally pre-computes them below) are
   * unsupported.
   */
  private def topSortAttrs(top: TakeOrderedAndProjectExec): Seq[
      org.apache.spark.sql.catalyst.expressions.AttributeReference] =
    top.sortOrder.map(_.child).map {
      case ar: org.apache.spark.sql.catalyst.expressions.AttributeReference => ar
      case _ => throw new TitianUnsupportedOperatorException(top)
    }

  /**
   * Soundness guard: a tap that records `currentInputId` must not consume rows that
   * crossed a shuffle without a keyed operator (whose post tap re-bases ids) in
   * between — the ids would be stale leftovers from unrelated rows. Such shapes
   * (e.g. a bare re-partition of shuffled rows) fail loudly.
   */
  private def idSafeFromShuffle(p: SparkPlan): Boolean = p match {
    case _: ShuffleQueryStageExec | _: ShuffleExchangeExec | _: AQEShuffleReadExec =>
      false
    // a partitioner-aligned union zips child partitions into shared tasks, making
    // positional ids ambiguous across children (the fused-aggregate path handles
    // this with per-child pre taps; everything else must not record above it)
    case u: UnionExec if isAlignedUnion(u) => false
    // keyed operators get a post tap (pass 2) that re-bases ids on their outputs
    case _: HashAggregateExec | _: SortMergeJoinExec | _: ShuffledHashJoinExec |
         _: BroadcastHashJoinExec | _: WindowExec |
         _: TakeOrderedAndProjectExec => true
    case _: TapPostKeyedExec | _: TapPostBroadcastJoinExec => true
    case leaf if leaf.children.isEmpty => true
    case other => other.children.forall(idSafeFromShuffle)
  }

  /**
   * Heuristic for `SparkContext.union` zipping partitions instead of concatenating:
   * it does so when all children expose compatible partitioners — at the plan level,
   * hash-partitioned outputs on every child.
   */
  private def isAlignedUnion(u: UnionExec): Boolean =
    u.children.forall { c =>
      c.outputPartitioning match {
        case _: HashPartitioning => true
        case p => p.getClass.getSimpleName.contains("HashPartitioning")
      }
    }

  private def isCommand(p: SparkPlan): Boolean = p.exists { node =>
    val name = node.getClass.getSimpleName
    name.contains("Command") || name.contains("LocalTableScan")
  }

  /**
   * Grouping keys minus the synthetic grouping id that Expand adds for ROLLUP/CUBE/
   * GROUPING SETS. The id rarely survives into the aggregate output, so both taps key
   * on the visible subset — associations stay consistent at the cost of conflating
   * grouping sets whose visible key values coincide (a coarsening, never a wrong
   * direction).
   */
  private def visibleGroupKeys(groupingExpressions: Seq[
      org.apache.spark.sql.catalyst.expressions.NamedExpression]): Seq[
      org.apache.spark.sql.catalyst.expressions.NamedExpression] =
    groupingExpressions.filterNot(_.name == "spark_grouping_id")

  private val supportedJoinTypes: Set[JoinType] =
    Set(Inner, LeftOuter, RightOuter, FullOuter, LeftSemi, LeftAnti)

  /**
   * Key expressions to hash on a shuffle join's output. For matched rows either side
   * works; for outer rows the present side must win — Coalesce picks it.
   */
  private def shuffleJoinKeys(
      joinType: JoinType,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression]): Seq[Expression] = joinType match {
    case Inner | LeftOuter | LeftSemi | LeftAnti => leftKeys
    case RightOuter => rightKeys
    case FullOuter =>
      leftKeys.zip(rightKeys).map { case (l, r) => Coalesce(Seq(l, r)) }
    case _ => leftKeys
  }

  /**
   * The partial aggregate fused directly under a final one — no shuffle between,
   * because the input was already partitioned on the grouping keys (e.g. GROUP BY
   * over a union of identically-grouped aggregates).
   */
  private def fusedPartialOf(agg: HashAggregateExec): Option[HashAggregateExec] =
    agg.child match {
      case p: HashAggregateExec
        if p.groupingExpressions.map(_.toAttribute.exprId) ==
           agg.groupingExpressions.map(_.toAttribute.exprId) => Some(p)
      case _ => None
    }

  /** The shuffle-read side: final aggregate / join inputs arrive from an exchange. */
  private def isReduceSide(p: SparkPlan): Boolean = p match {
    case _: ShuffleQueryStageExec | _: ShuffleExchangeExec | _: AQEShuffleReadExec => true
    case _: SortExec | _: ProjectExec | _: FilterExec | _: TapPostKeyedExec =>
      p.children.exists(isReduceSide)
    case _ => false
  }

  /**
   * Strict allowlist (phases 1-3). Note: QueryStageExec nodes are leaves whose inner
   * plans were validated when their stage was created.
   */
  private def validateSupported(plan: SparkPlan): Unit = {
    plan.foreach {
      case _: TitianTapExec => // ours
      case _: FileSourceScanExec => // row-based or columnar (ColumnarToRow boundary)
      case _: InMemoryTableScanExec | _: TableCacheQueryStageExec => // cache: a source
      case _: BatchEvalPythonExec | _: ArrowEvalPythonExec => // 1:1 Python UDF eval
      case _: FilterExec | _: ProjectExec => // pass-through
      case _: ExpandExec | _: GenerateExec => // 1->N within the pipeline: the
        // generated rows are consumed depth-first per input row, so currentInputId
        // threading remains exact (rollup/cube/count-distinct, explode)
      case _: HashAggregateExec => // partial or final
      case _: WindowExec => // peer-set granularity via partition-spec keying
      case _: UnionExec => // 1:1 concatenation; each task runs one child's partition,
        // so per-task id threading holds — trace-side partition translation only
      case top: TakeOrderedAndProjectExec =>
        topSortAttrs(top) // throws if a sort key is computed inline
      case _: org.apache.spark.sql.execution.EmptyRelationExec => // AQE's
        // empty-relation propagation: a zero-row leaf, trivially a source
      case ex: ShuffleExchangeExec => ex.outputPartitioning match {
        case _: HashPartitioning | SinglePartition => // supported
        case _ => throw new TitianUnsupportedOperatorException(ex)
      }
      case bex: BroadcastExchangeExec => bex.mode match {
        case _: HashedRelationBroadcastMode => // supported
        case _ => throw new TitianUnsupportedOperatorException(bex)
      }
      case _: ShuffleQueryStageExec | _: AQEShuffleReadExec |
           _: BroadcastQueryStageExec => // stage reads (validated at their creation)
      case s: SortExec =>
        // only as a shuffle-join/window input; a sort between a tap and the result
        // would break currentInputId threading
        if (!isReduceSide(s.child)) throw new TitianUnsupportedOperatorException(s)
      case j: SortMergeJoinExec =>
        if (!supportedJoinTypes.contains(j.joinType)) {
          throw new TitianUnsupportedOperatorException(j)
        }
      case j: ShuffledHashJoinExec =>
        if (!supportedJoinTypes.contains(j.joinType)) {
          throw new TitianUnsupportedOperatorException(j)
        }
      case j: BroadcastHashJoinExec =>
        if (!supportedJoinTypes.contains(j.joinType) || j.joinType == FullOuter) {
          throw new TitianUnsupportedOperatorException(j)
        }
      case other => throw new TitianUnsupportedOperatorException(other)
    }
  }
}
