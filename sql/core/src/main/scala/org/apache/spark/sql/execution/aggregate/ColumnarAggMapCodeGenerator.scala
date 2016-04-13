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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.types.StructType

/**
 * This is a helper object to generate an append-only single-key/single value aggregate hash
 * map that can act as a 'cache' for extremely fast key-value lookups while evaluating aggregates
 * (and fall back to the `BytesToBytesMap` if a given key isn't found). This is 'codegened' in
 * TungstenAggregate to speed up aggregates w/ key.
 *
 * It is backed by a power-of-2-sized array for index lookups and a columnar batch that stores the
 * key-value pairs. The index lookups in the array rely on linear probing (with a small number of
 * maximum tries) and use an inexpensive hash function which makes it really efficient for a
 * majority of lookups. However, using linear probing and an inexpensive hash function also makes it
 * less robust as compared to the `BytesToBytesMap` (especially for a large number of keys or even
 * for certain distribution of keys) and requires us to fall back on the latter for correctness.
 */
class ColumnarAggMapCodeGenerator(
    ctx: CodegenContext,
    generatedClassName: String,
    groupingKeySchema: StructType,
    bufferSchema: StructType) {
  val groupingKeys = groupingKeySchema.map(k => (k.dataType.typeName, ctx.freshName("key")))
  val bufferValues = bufferSchema.map(k => (k.dataType.typeName, ctx.freshName("value")))
  val groupingKeySignature = groupingKeys.map(_.productIterator.toList.mkString(" ")).mkString(", ")

  def generate(): String = {
    s"""
       |public class $generatedClassName {
       |${initializeAggregateHashMap()}
       |
       |${generateFindOrInsert()}
       |
       |${generateEquals()}
       |
       |${generateHashFunction()}
       |}
     """.stripMargin
  }

  private def initializeAggregateHashMap(): String = {
    val generatedSchema: String =
      s"""
         |new org.apache.spark.sql.types.StructType()
         |${(groupingKeySchema ++ bufferSchema).map(key =>
          s""".add("${key.name}", org.apache.spark.sql.types.DataTypes.${key.dataType})""")
          .mkString("\n")};
      """.stripMargin

    s"""
       |  private org.apache.spark.sql.execution.vectorized.ColumnarBatch batch;
       |  private int[] buckets;
       |  private int numBuckets;
       |  private int maxSteps;
       |  private int numRows = 0;
       |  private org.apache.spark.sql.types.StructType schema = $generatedSchema
       |
       |  public $generatedClassName(int capacity, double loadFactor, int maxSteps) {
       |    assert (capacity > 0 && ((capacity & (capacity - 1)) == 0));
       |    this.maxSteps = maxSteps;
       |    numBuckets = (int) (capacity / loadFactor);
       |    batch = org.apache.spark.sql.execution.vectorized.ColumnarBatch.allocate(schema,
       |      org.apache.spark.memory.MemoryMode.ON_HEAP, capacity);
       |    buckets = new int[numBuckets];
       |    java.util.Arrays.fill(buckets, -1);
       |  }
       |
       |  public $generatedClassName() {
       |    new $generatedClassName(1 << 16, 0.25, 5);
       |  }
     """.stripMargin
  }

  /**
   * Generates a method that computes a hash by currently xor-ing all individual group-by keys. For
   * instance, if we have 2 long group-by keys, the generated function would be of the form:
   *
   * {{{
   * private long hash(long agg_key, long agg_key1) {
   *   return agg_key ^ agg_key1;
   *   }
   * }}}
   */
  private def generateHashFunction(): String = {
    s"""
       |// TODO: Improve this hash function
       |private long hash($groupingKeySignature) {
       |  return ${groupingKeys.map(_._2).mkString(" ^ ")};
       |}
     """.stripMargin
  }

  /**
   * Generates a method that returns true if the group-by keys exist at a given index in the
   * associated [[org.apache.spark.sql.execution.vectorized.ColumnarBatch]]. For instance, if we
   * have 2 long group-by keys, the generated function would be of the form:
   *
   * {{{
   * private boolean equals(int idx, long agg_key, long agg_key1) {
   *   return batch.column(0).getLong(buckets[idx]) == agg_key &&
   *     batch.column(1).getLong(buckets[idx]) == agg_key1;
   * }
   * }}}
   */
  private def generateEquals(): String = {
    s"""
       |private boolean equals(int idx, $groupingKeySignature) {
       |  return ${groupingKeys.zipWithIndex.map(k =>
            s"batch.column(${k._2}).getLong(buckets[idx]) == ${k._1._2}").mkString(" && ")};
       |}
     """.stripMargin
  }

  /**
   * Generates a method that returns a mutable
   * [[org.apache.spark.sql.execution.vectorized.ColumnarBatch.Row]] which keeps track of the
   * aggregate value(s) for a given set of keys. If the corresponding row doesn't exist, the
   * generated method adds the corresponding row in the associated
   * [[org.apache.spark.sql.execution.vectorized.ColumnarBatch]]. For instance, if we
   * have 2 long group-by keys, the generated function would be of the form:
   *
   * {{{
   * public org.apache.spark.sql.execution.vectorized.ColumnarBatch.Row findOrInsert(
   *     long agg_key, long agg_key1) {
   *   long h = hash(agg_key, agg_key1);
   *   int step = 0;
   *   int idx = (int) h & (numBuckets - 1);
   *   while (step < maxSteps) {
   *     // Return bucket index if it's either an empty slot or already contains the key
   *     if (buckets[idx] == -1) {
   *       batch.column(0).putLong(numRows, agg_key);
   *       batch.column(1).putLong(numRows, agg_key1);
   *       batch.column(2).putLong(numRows, 0);
   *       buckets[idx] = numRows++;
   *       return batch.getRow(buckets[idx]);
   *     } else if (equals(idx, agg_key, agg_key1)) {
   *       return batch.getRow(buckets[idx]);
   *     }
   *     idx = (idx + 1) & (numBuckets - 1);
   *     step++;
   *   }
   *   // Didn't find it
   *   return null;
   * }
   * }}}
   */
  private def generateFindOrInsert(): String = {
    s"""
       |public org.apache.spark.sql.execution.vectorized.ColumnarBatch.Row findOrInsert(${
            groupingKeySignature}) {
       |  long h = hash(${groupingKeys.map(_._2).mkString(", ")});
       |  int step = 0;
       |  int idx = (int) h & (numBuckets - 1);
       |  while (step < maxSteps) {
       |    // Return bucket index if it's either an empty slot or already contains the key
       |    if (buckets[idx] == -1) {
       |      ${groupingKeys.zipWithIndex.map(k =>
                s"batch.column(${k._2}).putLong(numRows, ${k._1._2});").mkString("\n")}
       |      ${bufferValues.zipWithIndex.map(k =>
                s"batch.column(${groupingKeys.length + k._2}).putLong(numRows, 0);")
                .mkString("\n")}
       |      buckets[idx] = numRows++;
       |      return batch.getRow(buckets[idx]);
       |    } else if (equals(idx, ${groupingKeys.map(_._2).mkString(", ")})) {
       |      return batch.getRow(buckets[idx]);
       |    }
       |    idx = (idx + 1) & (numBuckets - 1);
       |    step++;
       |  }
       |  // Didn't find it
       |  return null;
       |}
     """.stripMargin
  }
}
