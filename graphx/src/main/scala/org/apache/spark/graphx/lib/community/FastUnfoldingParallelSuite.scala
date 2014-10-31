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

package org.apache.spark.graphx.lib.community

import org.scalatest.FunSuite
import org.apache.spark.graphx.{Graph, LocalSparkContext}
import org.apache.spark.SparkContext

class FastUnfoldingParallelSuite extends FunSuite {
  test("calcCurModularity when all node in the same community") {
    val sc = new SparkContext("local", "FastUnfoldingParallel")
    val edgeArray = Array((1, 2), (1, 4), (2, 3), (3, 4), (4, 4), (4, 4))
    val edgeRdd = sc.parallelize(edgeArray).map(e => (e._1.toLong, e._2.toLong))
    val g = Graph.fromEdgeTuples(edgeRdd, 1L)
    FastUnfoldingParallel.graphDegrees = g.degrees
    FastUnfoldingParallel.totalDegree = FastUnfoldingParallel.graphDegrees.map(e => e._2).sum()
    FastUnfoldingParallel.graphEdges = g.edges.map{
      case (edge) =>
        if (edge.srcId > edge.dstId)
          (edge.dstId, edge.srcId)
        else
          (edge.srcId, edge.dstId)
    }
    val communityArray = Array((1,1), (2, 1), (3, 1), (4, 1))
    FastUnfoldingParallel.communityResult = sc.parallelize(communityArray).map(e => (e._1.toLong, e._2.toLong))

    val result = FastUnfoldingParallel.calcCurrentModularity()

    assert(result === 0.0)
  } // end of calcCurModularity when all node in the same community

  test("calcCurModularity when there is two communities") {
    val sc = new SparkContext("local", "FastUnfoldingParallel")
    val edgeArray = Array((1,2), (1,3), (2,3), (2,4), (4,6), (5,6), (5,7), (6,7))
    val edgeRdd = sc.parallelize(edgeArray).map(e => (e._1.toLong, e._2.toLong))
    val g = Graph.fromEdgeTuples(edgeRdd, 1L)
    FastUnfoldingParallel.graphDegrees = g.degrees
    FastUnfoldingParallel.totalDegree = FastUnfoldingParallel.graphDegrees.map(e => e._2).sum()
    FastUnfoldingParallel.graphEdges = g.edges.map{
      case (edge) =>
        if (edge.srcId > edge.dstId)
          (edge.dstId, edge.srcId)
        else
          (edge.srcId, edge.dstId)
    }
    val communityArray = Array((1,1), (2, 1), (3, 1), (4, 2), (5, 2), (6, 2), (7, 2))
    FastUnfoldingParallel.communityResult = sc.parallelize(communityArray).map(e => (e._1.toLong, e._2.toLong))

    val result = FastUnfoldingParallel.calcCurrentModularity()

    assert(result === 0.3671875)
  } // end of calcCurModularity when there is two communities
}
