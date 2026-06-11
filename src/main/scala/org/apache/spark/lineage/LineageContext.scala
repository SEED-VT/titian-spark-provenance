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

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat, JobConf, TextInputFormat}
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.lineage.Direction.Direction
import org.apache.spark.lineage.rdd._
import org.apache.spark.rdd._
import org.apache.spark.util.SerializableConfiguration

import scala.collection.mutable
import scala.collection.mutable.{HashSet, Stack}
import scala.language.implicitConversions
import scala.reflect.ClassTag

object LineageContext {
  type RecordId = (Int, Int)
  val Dummy = 0

  implicit def fromRDDtoLineage(rdd: RDD[_]) = rdd.asInstanceOf[Lineage[_]]

  implicit def fromTapRDDtoLineageRDD(tap: TapLRDD[_]) = new LineageRDD(tap)

  implicit def fromLToLRDD(lineage: Lineage[_]) = new LineageRDD(lineage)

  implicit def fromLToLRDD2(lineage: Lineage[(Int, Any)]) = new LineageRDD(lineage)

  implicit def fromLToLRDD3(lineage: Lineage[(Any, Int)]) = new LineageRDD(lineage)

  implicit def fromLineageToShowRDD(lineage: Lineage[(RecordId, String)]) = new ShowRDD(lineage)

  implicit def lrddToPairLRDDFunctions[K, V](lrdd: Lineage[(K, V)])
      (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null) =
    new PairLRDDFunctions(lrdd)

  implicit def lrddToOrderedLRDDFunctions[K : Ordering : ClassTag, V: ClassTag](
      lrdd: Lineage[(K, V)]) =
    new OrderedLRDDFunctions[K, V, (K, V)](lrdd)
}

import org.apache.spark.lineage.LineageContext._

class LineageContext(@transient val sparkContext: SparkContext) extends Logging {

  /**
   * Read a text file from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI, and return it as an RDD of Strings.
   */
  def textFile(
      path: String,
      minPartitions: Int = sparkContext.defaultMinPartitions): Lineage[String] = {
    val tmpRdd = hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
      minPartitions)
      val map = tmpRdd.map(pair => pair._2.toString).setName(path)
    if(isLineageActive) {
      map.setTap(tmpRdd)
    } else {
      map
    }
  }

  /** Get an RDD for a Hadoop file with an arbitrary InputFormat
    *
    * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
    * record, directly caching the returned RDD will create many references to the same object.
    * If you plan to directly cache Hadoop writable objects, you should first copy them using
    * a `map` function.
    * */
  def hadoopFile[K, V](
      path: String,
      inputFormatClass: Class[_ <: InputFormat[K, V]],
      keyClass: Class[K],
      valueClass: Class[V],
      minPartitions: Int = sparkContext.defaultMinPartitions
      ): Lineage[(K, V)] = {
    // A Hadoop configuration can be about 10 KB, which is pretty big, so broadcast it.
    val confBroadcast = sparkContext.broadcast(
      new SerializableConfiguration(sparkContext.hadoopConfiguration))
    val setInputPathsFunc = (jobConf: JobConf) => FileInputFormat.setInputPaths(jobConf, path)

    val rdd = new HadoopLRDD(
      this,
      confBroadcast,
      Some(setInputPathsFunc),
      inputFormatClass,
      keyClass,
      valueClass,
      minPartitions).setName(path)
    if(isLineageActive) {
      rdd.tapRight()
    } else {
      rdd
    }
  }

  /** Distribute a local Scala collection to form an RDD.
    *
    * @note Parallelize acts lazily. If `seq` is a mutable collection and is
    * altered after the call to parallelize and before the first action on the
    * RDD, the resultant RDD will reflect the modified collection. Pass a copy of
    * the argument to avoid this.
    */
  def parallelize[T: ClassTag](
      seq: Seq[T],
      numSlices: Int = sparkContext.defaultParallelism): Lineage[T] = {
    val rdd = new ParallelCollectionLRDD[T](this, seq, numSlices, Map[Int, Seq[String]]())
    if(isLineageActive) {
      rdd.tapRight()
    } else {
      rdd
    }
  }

  /**
   * Run a job on all partitions in an RDD and return the results in an array.
   */
  def runJobWithId[T, U: ClassTag](rdd: Lineage[T], func: Iterator[(T, Long)] => U): Array[U] = {
    val tappedRdd = tapJobWithId(rdd)
//    sparkContext.runJob(tappedRdd, func, 0 until tappedRdd.partitions.size, false)
    sparkContext.runJob(tappedRdd, func, 0 until tappedRdd.partitions.size)
  }

  /**
   * Run a job on all partitions in an RDD and return the results in an array.
   */
  def runJob[T: ClassTag, U: ClassTag](rdd: Lineage[T], func: Iterator[T] => U): Array[U] = {
    val tappedRdd = if(isLineageActive) tapJob(rdd) else rdd
//    sparkContext.runJob(tappedRdd, func, 0 until tappedRdd.partitions.size, false)
    sparkContext.runJob(tappedRdd, func, 0 until tappedRdd.partitions.size)

  }

  /**
   * Run a job on all partitions in an RDD and return the results in an array.
   */
  def runJob[T, U: ClassTag](rdd: Lineage[T], func: (TaskContext, Iterator[T]) => U): Array[U] = {
    val tappedRdd = if(isLineageActive) tapJob(rdd) else rdd
//    sparkContext.runJob(tappedRdd, func, 0 until rdd.partitions.size, false)
    sparkContext.runJob(tappedRdd, func, 0 until rdd.partitions.size)
  }

  val replayVisited = new mutable.Stack[Lineage[_]]()

  // Program must not be recursive
  def setUpReplay(last: Lineage[_]) = {
    val initial = last

    def visit(rdd: Lineage[_]) {
      replayVisited.push(rdd)
      rdd.setCaptureLineage(false)
      for (dep <- rdd.dependencies) {
          visit(dep.rdd)
      }
      if (rdd.dependencies.isEmpty) {
        replayVisited.pop()
        replayVisited.pop()
        replayVisited.pop()
      }
    }

    visit(initial)
  }

  def replay(rdd: Lineage[_]) = {
    var current: Lineage[_] = rdd
    while(!replayVisited.isEmpty) {
      val prev = replayVisited.pop()
      if(!prev.isInstanceOf[TapLRDD[_]]) {
        current = prev.replay(current)
      }
    }
    current
  }

  def getLineage(rdd: Lineage[_]) = {
    val initialTap: Lineage[_] = rdd.materialize
    val visited = new HashSet[RDD[_]]

    def visit(rdd: RDD[_], parent: RDD[_]) {
      if (!visited(rdd)) {
        //visited += rdd // This creates problems with self-joins
        rdd.setCaptureLineage(isLineageActive)
        var dependencies = List[OneToOneDependency[_]]()
        for (dep <- rdd.dependencies) {
          val newParent: RDD[_] = dep.rdd match {
            case tap: TapLRDD[_] =>
              // Append (fork prepended): keeps goBack(path = i) aligned with the i-th
              // input of a multi-parent operator like cogroup/join.
              dependencies = dependencies :+ new OneToOneDependency(tap.materialize.cache())
              tap
            case _ => parent
          }
          visit(dep.rdd, newParent)
        }
        if (!dependencies.isEmpty) {
          val oldDeps = parent.dependencies.filter(d => d.rdd.isInstanceOf[TapLRDD[_]])
          RDDShims.updateDependencies(parent, oldDeps.toList ::: dependencies)
        }
      }
    }

    visit(initialTap, initialTap)

    currentLineagePosition = Some(rdd.asInstanceOf[Lineage[(RecordId, Any)]])
  }

  private[spark] def tapJob[T](rdd: Lineage[T]): TapLRDD[T] = {
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(rdd: RDD[_]) {
      if (!visited(rdd) && !rdd.isInstanceOf[TapLRDD[_]]) {
        visited += rdd
        // Order matters: for multi-dependency RDDs (cogroup/join) the dependency index
        // determines which slot of the value array each parent fills. The fork used a
        // HashSet here, which scrambles that order nondeterministically.
        val deps = new mutable.LinkedHashSet[Dependency[_]]
        for (dep <- rdd.dependencies) {
          waitingForVisit.push(dep.rdd)
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              shufDep.rdd.setTap(rdd.tapLeft())
              deps += RDDShims.tapShuffleDependency(shufDep, shufDep.rdd.getTap.get)

            case narDep: OneToOneDependency[_] =>
              // Intercept the end of the stage to add a post-shuffle tap
              if(narDep.rdd.dependencies.nonEmpty) {
                if(narDep.rdd.dependencies
                  .count(d => d.isInstanceOf[ShuffleDependency[_, _, _]]) > 0) {
                  val tap = narDep.rdd.tapRight()
                  deps += RDDShims.tapNarrowDependency(narDep, tap)
                }
              }

            case _ =>
          }
        }
        if(deps.nonEmpty) {
          RDDShims.updateDependencies(rdd, deps.toList)
        }
      }
    }
    waitingForVisit.push(rdd)
    while (!waitingForVisit.isEmpty) {
      visit(waitingForVisit.pop())
    }

    rdd.tapRight()
  }

  private def tapJobWithId[T](rdd: Lineage[T]): RDD[(T, Long)] = {
    if(!isLineageActive) {
      rdd.zipWithUniqueId()
    } else {
      val result = tapJob(rdd)
      result.isLast = true
      result.asInstanceOf[RDD[(T, Long)]]
    }
  }

  private var captureLineage: Boolean = false

  private var currentLineagePosition: Option[Lineage[_]] = None

  private var lastLineagePosition: Option[Lineage[_]] = None

  private var lastLineageSeen: Option[Lineage[_]] = None

  def getCurrentLineagePosition = currentLineagePosition

  def getLastLineageSeen = lastLineageSeen

  def setCurrentLineagePosition(initialRDD: Option[Lineage[_]]) = {
    // Cleaning up
    prevLineagePosition.clear()
    lastOperation = None

    if(lastLineagePosition.isDefined && lastLineagePosition.get != initialRDD.get) {
      currentLineagePosition = lastLineagePosition

      // We are starting from the middle, fill the stack with prev positions
      if(currentLineagePosition.get != initialRDD.get) {
        prevLineagePosition.pushAll(search(List(currentLineagePosition.get), initialRDD.get))
      }
    }
    currentLineagePosition = initialRDD
    lastLineageSeen = currentLineagePosition
  }

  def search(path: List[Lineage[_]], initialRDD: Lineage[_]): List[Lineage[_]] = {
    path.map(rdd =>
      if(rdd.id == initialRDD.id) return path
    )
    path.foreach(p => {
      p.dependencies.map(dep => {
        val tmp = search(List(dep.rdd.asInstanceOf[Lineage[_]]), initialRDD)
        if (!tmp.isEmpty) return p :: tmp
      })
    })
    return Nil
  }

  def setLastLineagePosition(finalRDD: Option[Lineage[_]]) = lastLineagePosition = finalRDD

  def getlastOperation = lastOperation

  private[spark] var prevLineagePosition = new Stack[Lineage[_]]()

  private[spark] var lastOperation: Option[Direction] = None

  // Every tap RDD registers itself at construction so its materialized lineage
  // blocks can be dropped later.
  private val capturedTapIds = new mutable.ArrayBuffer[Int]

  private[spark] def registerTap(rddId: Int): Unit = capturedTapIds += rddId

  /**
   * Drop the lineage blocks of every tap captured through this context so far
   * (long sessions accumulate them in the executors' block managers).
   */
  def releaseLineage(): Unit = {
    val ids = capturedTapIds.toSet
    if (ids.nonEmpty) {
      val master = sparkContext.env.blockManager.master
      master.getMatchingBlockIds({
        case org.apache.spark.storage.RDDBlockId(id, _) => ids.contains(id)
        case _ => false
      }, askStorageEndpoints = true).foreach(master.removeBlock)
      capturedTapIds.clear()
    }
  }

  def isLineageActive: Boolean = captureLineage

  def setCaptureLineage(newLineage: Boolean) = {
    if(newLineage == false && captureLineage == true) {
      getLineage(lastLineagePosition.get)
    }
    captureLineage = newLineage
  }

  def getBackward(path: Int = 0) = {
    // CurrentLineagePosition should be always set at this point
    assert(currentLineagePosition.isDefined)

    // Notify if we are at the end of the workflow
    if(currentLineagePosition.get.dependencies.size == 0)
      throw new UnsupportedOperationException("unsopported operation")

    currentLineagePosition.get.dependencies(path).rdd match {
      case _: HadoopRDD[_, _] =>
        throw new UnsupportedOperationException("unsopported operation")
      case _: ParallelCollectionRDD[_] =>
        throw new UnsupportedOperationException("unsopported operation")
      case _ =>
    }

    prevLineagePosition.push(currentLineagePosition.get)
    lastLineageSeen = currentLineagePosition

    currentLineagePosition = Some(currentLineagePosition.get.dependencies(path).rdd)

    if(!lastOperation.isDefined || lastOperation.get == Direction.FORWARD) {
      lastOperation = Some(Direction.BACKWARD)
      None
    } else {
      val result: Lineage[(RecordId, Any)] = getCurrentLineagePosition.get match {
        case postCG: TapPostCoGroupLRDD[(Int, Any) @unchecked] =>
          postCG.map(r => ((Dummy, r._1), r._2))
        case hadoop: TapHadoopLRDD[Long @unchecked, Int @unchecked] =>
          hadoop.map(_.swap).map(r => ((Dummy, r._1), r._2))
        case tap: TapLRDD[_] => tap
      }
      Some(result)
    }
  }

  def getForward(): Lineage[((Int, _), Any)] = {
    if(!lastOperation.isDefined || lastOperation.get == Direction.BACKWARD) {
      lastOperation = Some(Direction.FORWARD)
    }

    if(prevLineagePosition.isEmpty) {
      throw new UnsupportedOperationException("unsopported operation")
    }

    lastLineageSeen = currentLineagePosition
    currentLineagePosition = Some(prevLineagePosition.pop())

    currentLineagePosition.get match {
      case pre: TapPreShuffleLRDD[(Any, Array[Int])@unchecked] =>
        pre.flatMap(r => r._2.map(b => ((Dummy, b), r._1)))
      case post: TapPostShuffleLRDD[(Any, RecordId)@unchecked] =>
        post.map(_.swap)
      case other: TapLRDD[(Any, Long)@unchecked] =>
       other.map(r => ((Dummy, r._2), r._1))
    }
  }
}

object Direction extends Enumeration {
  type Direction = Value
  val FORWARD, BACKWARD = Value
}