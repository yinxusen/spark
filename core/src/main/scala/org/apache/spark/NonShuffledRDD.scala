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

package org.apache.spark

import java.io.{IOException, ObjectOutputStream}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

private case class NonShuffledDep(
    @transient dep: NarrowDependency[_],
    @transient splitIndex: Int,
    var split: Partition
  ) extends Serializable {

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    split = dep.rdd.partitions(splitIndex)
    oos.defaultWriteObject()
  }
}

private[spark] class NonShuffledPartition(
    idx: Int, val nonShuffledDeps: Array[Option[NonShuffledDep]])
  extends Partition with Serializable {
  override val index: Int = idx
  override def hashCode(): Int = idx
}

/**
 * :: DeveloperApi ::
 * The resulting RDD from a non-shuffle combination.
 */
@DeveloperApi
class NonShuffledRDD[T](
    sc: SparkContext,
    deps: Seq[NarrowDependency[_]],
    part: Partitioner)
  extends RDD[T](sc, deps) {

  override val partitioner = Some(part)

  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](part.numPartitions)
    for (i <- array.indices) {
      // Each CoGroupPartition will have a dependency per contributing RDD
      array(i) = new NonShuffledPartition(i, deps.zipWithIndex.map { case (dep, j) =>
        // Assume each RDD contributed a single dependency, and get it
        dependencies(j) match {
          case s: ShuffleDependency[_, _, _] =>
            None
          case _ =>
            Some(new NonShuffledDep(dep, i, dep.rdd.partitions(i)))
        }
      }.toArray)
    }
    array
  }

  override def compute(s: Partition, context: TaskContext): Iterator[T] = {
    val split = s.asInstanceOf[NonShuffledPartition]

    // A list of (rdd iterator, dependency number) pairs
    val rddIterators = new ArrayBuffer[Iterator[T]]
    for ((dep, depNum) <- dependencies.zipWithIndex) dep match {
      case oneToOneDependency: OneToOneDependency[T] @unchecked =>
        val dependencyPartition = split.nonShuffledDeps(depNum).get.split
        // Read them from the parent
        val it = oneToOneDependency.rdd.iterator(dependencyPartition, context)
        rddIterators += it

      case shuffleDependency: ShuffleDependency[_, _, _] =>
    }
    rddIterators.toArray.foldLeft(Iterator[T]())(_ ++ _)
  }
}
