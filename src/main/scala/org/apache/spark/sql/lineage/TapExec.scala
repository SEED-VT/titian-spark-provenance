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

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{CodegenSupport, SparkPlan, UnaryExecNode}

import org.apache.spark.lineage.LineageTaskState

/**
 * Titian tap operators for SQL plans. Both are strict row pass-throughs that
 * participate in whole-stage codegen: their per-record capture is one method call on a
 * per-task runtime object, fused into the generated loop. Both also implement the
 * interpreted path for stages where codegen bails (SQL_LINEAGE_PLAN caveat 2).
 */
trait TitianTapExec extends UnaryExecNode with CodegenSupport {
  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[org.apache.spark.sql.catalyst.expressions.SortOrder] =
    child.outputOrdering

  override def inputRDDs(): Seq[RDD[InternalRow]] =
    child.asInstanceOf[CodegenSupport].inputRDDs()

  override def doProduce(ctx: CodegenContext): String =
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
}

/**
 * Assigns the per-task input row id as rows leave the scan — `TapHadoopLRDD` for SQL.
 * Inserted directly above each (row-based) leaf scan.
 */
case class TapScanExec(child: SparkPlan) extends TitianTapExec {

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val tap = ctx.addMutableState(
      classOf[ScanTapRuntime].getName, "titianScanTap",
      v => s"$v = new ${classOf[ScanTapRuntime].getName}();")
    s"""
       |$tap.tap();
       |${consume(ctx, input)}
     """.stripMargin
  }

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val tap = new ScanTapRuntime()
      iter.map { r => tap.tap(); r }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): TapScanExec =
    copy(child = newChild)
}

/**
 * Base for taps that hash a key projection per row (pre-exchange / post-keyed).
 * The key hash is the murmur3 `UnsafeRow.hashCode` of the projected key — byte-stable
 * for equal values, no `toString` allocation (cf. LineageHashing on the RDD side).
 */
trait KeyedTapExec extends TitianTapExec {
  def keyExprs: Seq[Expression]
  def tapId: Int
  protected def runtimeClass: Class[_]

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val tap = ctx.addMutableState(
      runtimeClass.getName, "titianKeyedTap",
      v => s"$v = new ${runtimeClass.getName}($tapId);")
    ctx.currentVars = input
    val boundKeys = BindReferences.bindReferences[Expression](keyExprs, child.output)
    val keyEv = GenerateUnsafeProjection.createCode(ctx, boundKeys)
    s"""
       |${keyEv.code}
       |$tap.tap(${keyEv.value}.hashCode());
       |${consume(ctx, input)}
     """.stripMargin
  }

  protected def interpretedTap(tapper: (Any, Int) => Unit): RDD[InternalRow] = {
    val keys = keyExprs
    val childOutput = child.output
    child.execute().mapPartitionsInternal { iter =>
      val proj = UnsafeProjection.create(keys, childOutput)
      val tap = runtimeClass.getConstructor(classOf[Int])
        .newInstance(Integer.valueOf(tapId))
      iter.map { r => tapper(tap, proj(r).hashCode()); r }
    }
  }
}

/** Map-side capture below an exchange: (keyHash -> bitmap of currentInputIds). */
case class TapPreExchangeExec(tapId: Int, keyExprs: Seq[Expression], child: SparkPlan)
  extends KeyedTapExec {

  override protected def runtimeClass: Class[_] = classOf[PreExchangeTapRuntime]

  override protected def doExecute(): RDD[InternalRow] =
    interpretedTap((tap, h) => tap.asInstanceOf[PreExchangeTapRuntime].tap(h))

  override protected def withNewChildInternal(newChild: SparkPlan): TapPreExchangeExec =
    copy(child = newChild)
}

/** Reduce-side capture above a final aggregate or join: (packedOutIdx -> keyHash). */
case class TapPostKeyedExec(tapId: Int, keyExprs: Seq[Expression], child: SparkPlan)
  extends KeyedTapExec {

  override protected def runtimeClass: Class[_] = classOf[PostKeyedTapRuntime]

  override protected def doExecute(): RDD[InternalRow] =
    interpretedTap((tap, h) => tap.asInstanceOf[PostKeyedTapRuntime].tap(h))

  override protected def withNewChildInternal(newChild: SparkPlan): TapPostKeyedExec =
    copy(child = newChild)
}

/**
 * Below the streamed side of a BroadcastHashJoin: saves the probe row's id into the
 * join's mark slot as the row enters the join. Required because a fan-out join's
 * matches are emitted depth-first — `currentInputId` is already overwritten by the
 * first match's downstream taps when the second match emerges.
 */
case class TapProbeMarkExec(pairId: Int, child: SparkPlan) extends TitianTapExec {

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val tap = ctx.addMutableState(
      classOf[ProbeMarkTapRuntime].getName, "titianProbeMarkTap",
      v => s"$v = new ${classOf[ProbeMarkTapRuntime].getName}($pairId);")
    s"""
       |$tap.tap();
       |${consume(ctx, input)}
     """.stripMargin
  }

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val tap = new ProbeMarkTapRuntime(pairId)
      iter.map { r => tap.tap(); r }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): TapProbeMarkExec =
    copy(child = newChild)
}

/**
 * Above a BroadcastHashJoin: (packedOutIdx -> (streamKeyHash, probeInputId)).
 * keyExprs must be the join's *rewritten* streamed keys so the hash matches the build
 * side's pre-broadcast tap (which hashes the broadcast mode's rewritten keys). The
 * probe id is read from the mark slot the join's [[TapProbeMarkExec]] writes
 * (`pairId` pairs them).
 */
case class TapPostBroadcastJoinExec(
    tapId: Int, pairId: Int, keyExprs: Seq[Expression], child: SparkPlan)
  extends KeyedTapExec {

  override protected def runtimeClass: Class[_] = classOf[PostJoinTapRuntime]

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val tap = ctx.addMutableState(
      runtimeClass.getName, "titianKeyedTap",
      v => s"$v = new ${runtimeClass.getName}($tapId, $pairId);")
    ctx.currentVars = input
    val boundKeys = BindReferences.bindReferences[Expression](keyExprs, child.output)
    val keyEv = GenerateUnsafeProjection.createCode(ctx, boundKeys)
    s"""
       |${keyEv.code}
       |$tap.tap(${keyEv.value}.hashCode());
       |${consume(ctx, input)}
     """.stripMargin
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val keys = keyExprs
    val childOutput = child.output
    val (tid, pid) = (tapId, pairId)
    child.execute().mapPartitionsInternal { iter =>
      val proj = UnsafeProjection.create(keys, childOutput)
      val tap = new PostJoinTapRuntime(tid, pid)
      iter.map { r => tap.tap(proj(r).hashCode()); r }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): TapPostBroadcastJoinExec =
    copy(child = newChild)
}

/** Below a Python-UDF eval node: records the input-id sequence (1:1, buffered). */
case class TapSeqExec(pairId: Int, child: SparkPlan) extends TitianTapExec {

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val tap = ctx.addMutableState(
      classOf[SeqTapRuntime].getName, "titianSeqTap",
      v => s"$v = new ${classOf[SeqTapRuntime].getName}($pairId);")
    s"""
       |$tap.tap();
       |${consume(ctx, input)}
     """.stripMargin
  }

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val tap = new SeqTapRuntime(pairId)
      iter.map { r => tap.tap(); r }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): TapSeqExec =
    copy(child = newChild)
}

/** Above the Python-UDF eval node: replays the recorded input-id sequence. */
case class TapReseqExec(pairId: Int, child: SparkPlan) extends TitianTapExec {

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val tap = ctx.addMutableState(
      classOf[ReseqTapRuntime].getName, "titianReseqTap",
      v => s"$v = new ${classOf[ReseqTapRuntime].getName}($pairId);")
    s"""
       |$tap.tap();
       |${consume(ctx, input)}
     """.stripMargin
  }

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val tap = new ReseqTapRuntime(pairId)
      iter.map { r => tap.tap(); r }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): TapReseqExec =
    copy(child = newChild)
}

/**
 * Records (packed(partition, outputIdx) -> currentInputId) for every result row and
 * materializes the association at task end — `TapLRDD` (isLast) for SQL. Inserted at
 * the root of the final stage.
 */
case class TapResultExec(tapId: Int, child: SparkPlan) extends TitianTapExec {

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val tap = ctx.addMutableState(
      classOf[ResultTapRuntime].getName, "titianResultTap",
      v => s"$v = new ${classOf[ResultTapRuntime].getName}($tapId);")
    s"""
       |$tap.tap();
       |${consume(ctx, input)}
     """.stripMargin
  }

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val tap = new ResultTapRuntime(tapId)
      iter.map { r => tap.tap(); r }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): TapResultExec =
    copy(child = newChild)
}
