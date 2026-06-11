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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * Accessor for HashJoin's key rewriting (private[execution]): the broadcast mode's
 * build keys are rewritten (e.g. multiple integral keys packed into one long), so the
 * stream-side hash used by Titian's broadcast-join tap must apply the same rewrite to
 * match the build-side pre-broadcast tap.
 */
object TitianJoinKeys {
  def rewrite(keys: Seq[Expression]): Seq[Expression] =
    joins.HashJoin.rewriteKeyExpr(keys)
}
