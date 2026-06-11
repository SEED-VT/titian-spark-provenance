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

package org.apache.spark.util

/**
 * Packs a pair of ints (typically partition id and record id) into a single Long.
 * Carried over verbatim from the Titian fork (was appended to Spark's Utils.scala;
 * now lives in the library jar).
 */
object PackIntIntoLong {
  private final val RIGHT: Long = 0xFFFFFFFFL

  def apply(left: Int, right: Int): Long = left.toLong << 32 | right & 0xFFFFFFFFL

  def getLeft(value: Long): Int = (value >>> 32).toInt // >>> operator 0-fills from left

  def getRight(value: Long): Int = (value & RIGHT).toInt
}
