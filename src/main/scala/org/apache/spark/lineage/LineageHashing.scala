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

import org.apache.spark.unsafe.hash.Murmur3_x86_32

/**
 * The single definition of the key hash used by lineage capture and tracing.
 *
 * The fork hashed `key.toString` with murmur3 at every site — a per-record String
 * allocation that dominated capture overhead. Only internal consistency matters
 * (capture and trace must hash identically, and `LineageHashPartitioner` must route
 * with the same function), so we hash the key's own `hashCode` through a murmur3
 * finalizer instead: zero allocation, well-mixed bits for partition routing.
 */
object LineageHashing {

  def hashKey(key: Any): Int =
    if (TitianAblation.legacyHash) legacyHashKey(key)
    else key match {
      case null => 0
      case k => Murmur3_x86_32.hashInt(k.hashCode(), 42)
    }

  // ablation: the fork's per-record toString + murmur over the bytes (P1a off)
  private def legacyHashKey(key: Any): Int = key match {
    case null => 0
    case k => com.google.common.hash.Hashing.murmur3_32_fixed()
      .hashString(k.toString, java.nio.charset.StandardCharsets.UTF_8).asInt()
  }
}
