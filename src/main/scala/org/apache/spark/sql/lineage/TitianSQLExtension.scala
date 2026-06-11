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
import org.apache.spark.sql.execution.{ColumnarRule, ExpandExec, FileSourceScanExec, FilterExec, GenerateExec, ProjectExec, SortExec, SparkPlan, TitianJoinKeys}
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, Exchange, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashedRelationBroadcastMode, ShuffledHashJoinExec, SortMergeJoinExec}
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
    // With AQE the outer preparation pipeline passes the AdaptiveSparkPlanExec wrapper
    // through this rule too; the real injection happens on the per-stage invocations
    // from inside AQE.
    if (plan.exists(_.isInstanceOf[
      org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec])) return plan
    // Idempotency: AQE may re-prepare a subtree that already carries taps
    if (plan.exists(_.isInstanceOf[TitianTapExec])) return plan

    validateSupported(plan)

    var tapped = plan.transformUp {
      // input ids at the sources
      case scan: FileSourceScanExec if !scan.supportsColumnar =>
        TapScanExec(scan)

      // map-side capture: below the partial aggregate when the exchange feeds one
      case ex: ShuffleExchangeExec => ex.child match {
        case agg: HashAggregateExec if !agg.child.isInstanceOf[TapPreExchangeExec] =>
          val pre = TapPreExchangeExec(
            session.sparkContext.newRddId(), agg.groupingExpressions, agg.child)
          ex.withNewChildren(Seq(agg.withNewChildren(Seq(pre))))
        case agg: HashAggregateExec => ex
        case _: TapPreExchangeExec => ex
        case other =>
          val keys = ex.outputPartitioning match {
            case h: HashPartitioning => h.expressions
            case SinglePartition => Seq.empty
            case p => throw new TitianUnsupportedOperatorException(ex)
          }
          ex.withNewChildren(
            Seq(TapPreExchangeExec(session.sparkContext.newRddId(), keys, other)))
      }

      // build side of a broadcast join: capture (rewritten-key hash -> input ids)
      case bex: BroadcastExchangeExec
        if !bex.child.isInstanceOf[TapPreExchangeExec] => bex.mode match {
        case HashedRelationBroadcastMode(keys, _) =>
          bex.withNewChildren(
            Seq(TapPreExchangeExec(session.sparkContext.newRddId(), keys, bex.child)))
        case _ => throw new TitianUnsupportedOperatorException(bex)
      }
    }

    // reduce-side capture above final aggregates and shuffle joins
    tapped = tapped.transformUp {
      case agg: HashAggregateExec
        if isReduceSide(agg.child) && !agg.isInstanceOf[TapPostKeyedExec] =>
        // The grouping values may surface in the output under an Alias (e.g.
        // GROUP BY upper(c) ... SELECT upper(c) AS x): resolve each grouping attr to
        // the output attribute that carries its value.
        val keyAttrs = agg.groupingExpressions.map(_.toAttribute).map { g =>
          agg.resultExpressions.collectFirst {
            case ar: org.apache.spark.sql.catalyst.expressions.AttributeReference
              if ar.exprId == g.exprId => ar
            case al @ org.apache.spark.sql.catalyst.expressions.Alias(
              ar: org.apache.spark.sql.catalyst.expressions.AttributeReference, _)
              if ar.exprId == g.exprId => al.toAttribute
          }.getOrElse(throw new TitianUnsupportedOperatorException(agg))
        }
        TapPostKeyedExec(session.sparkContext.newRddId(), keyAttrs, agg)

      case j: SortMergeJoinExec =>
        TapPostKeyedExec(session.sparkContext.newRddId(),
          shuffleJoinKeys(j.joinType, j.leftKeys, j.rightKeys), j)

      case j: ShuffledHashJoinExec =>
        TapPostKeyedExec(session.sparkContext.newRddId(),
          shuffleJoinKeys(j.joinType, j.leftKeys, j.rightKeys), j)

      case j: BroadcastHashJoinExec =>
        // streamed keys, rewritten the same way as the broadcast mode's build keys,
        // so the output hash matches the build-side pre-broadcast tap
        val streamedKeys =
          if (j.buildSide == org.apache.spark.sql.catalyst.optimizer.BuildRight) {
            j.leftKeys
          } else {
            j.rightKeys
          }
        TapPostBroadcastJoinExec(session.sparkContext.newRddId(),
          TitianJoinKeys.rewrite(streamedKeys), j)

      case w: WindowExec if isReduceSide(w.child) =>
        // window-partition (peer set) granularity: key outputs by the partition spec
        TapPostKeyedExec(session.sparkContext.newRddId(), w.partitionSpec, w)
    }

    // result tap: only on the final (non-exchange-rooted) stage, exactly once
    tapped match {
      case _: Exchange => tapped
      case _ if tapped.isInstanceOf[TapResultExec] => tapped
      case _ if !tapped.exists(_.isInstanceOf[TapScanExec]) &&
                !tapped.exists(_.isInstanceOf[TapPostKeyedExec]) => tapped
      case root =>
        val tapId = session.sparkContext.newRddId()
        logInfo(s"Titian SQL capture: result tap $tapId inserted")
        TapResultExec(tapId, root)
    }
  }

  private def isCommand(p: SparkPlan): Boolean = p.exists { node =>
    val name = node.getClass.getSimpleName
    name.contains("Command") || name.contains("LocalTableScan")
  }

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
      case scan: FileSourceScanExec =>
        if (scan.supportsColumnar) throw new TitianUnsupportedOperatorException(scan)
      case _: FilterExec | _: ProjectExec => // pass-through
      case _: ExpandExec | _: GenerateExec => // 1->N within the pipeline: the
        // generated rows are consumed depth-first per input row, so currentInputId
        // threading remains exact (rollup/cube/count-distinct, explode)
      case _: HashAggregateExec => // partial or final
      case _: WindowExec => // peer-set granularity via partition-spec keying
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
