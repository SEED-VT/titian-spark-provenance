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

import java.lang.reflect.Field

import org.apache.spark.{Dependency, OneToOneDependency, ShuffleDependency}
import org.apache.spark.rdd.RDD

/**
 * The two DAG-splicing operations the fork added to Spark core (`RDD.updateDependencies`
 * and `Dependency.tapDependency`), reproduced from outside via reflection on the private
 * fields. Both run on the driver, before the job executes, on RDDs the library itself
 * created — semantics are identical to the fork: splice a tap between an RDD and its
 * parent while preserving the already-registered shuffle (same shuffleId/handle).
 */
object RDDShims {

  private def findField(clazz: Class[_], names: Seq[String]): Field = {
    var c: Class[_] = clazz
    while (c != null) {
      val found = c.getDeclaredFields.find(f => names.exists(n => f.getName == n))
      if (found.isDefined) {
        val f = found.get
        f.setAccessible(true)
        return f
      }
      c = c.getSuperclass
    }
    throw new NoSuchFieldException(
      s"none of ${names.mkString(", ")} found on ${clazz.getName} hierarchy")
  }

  private lazy val depsField =
    findField(classOf[RDD[_]], Seq("deps", "org$apache$spark$rdd$RDD$$deps"))

  private lazy val dependenciesField =
    findField(classOf[RDD[_]], Seq("dependencies_", "org$apache$spark$rdd$RDD$$dependencies_"))

  private lazy val shuffleRddField = findField(
    classOf[ShuffleDependency[_, _, _]],
    Seq("_rdd", "org$apache$spark$ShuffleDependency$$_rdd"))

  /** Fork: `RDD.updateDependencies` — replace an RDD's dependency list in place. */
  def updateDependencies(rdd: RDD[_], deps: Seq[Dependency[_]]): Unit = {
    depsField.set(rdd, deps)
    dependenciesField.set(rdd, deps)
  }

  /**
   * Fork: `ShuffleDependency.tapDependency` — repoint the dependency's parent RDD at the
   * tap, keeping the existing shuffleId and registered shuffle handle.
   */
  def tapShuffleDependency[K, V, C](
      dep: ShuffleDependency[K, V, C],
      tap: RDD[_]): ShuffleDependency[K, V, C] = {
    shuffleRddField.set(dep, tap.asInstanceOf[RDD[Product2[K, V]]])
    dep
  }

  /** Fork: `OneToOneDependency.tapDependency` — same splice for a narrow dependency. */
  def tapNarrowDependency[T](dep: OneToOneDependency[T], tap: RDD[_]): OneToOneDependency[T] =
    new OneToOneDependency[T](tap.asInstanceOf[RDD[T]])
}
