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

import org.apache.spark.graphx._
import org.scalatest.FunSuite

class PregelUnfoldingSuite extends FunSuite with LocalSparkContext {

  test("2 Cycle Strongly Connected Components") {
    withSpark { sc =>
      val edges =
        Array(0L -> 1L, 1L -> 2L, 2L -> 0L) ++
        Array(3L -> 4L, 4L -> 5L, 5L -> 3L) ++
        Array(6L -> 0L, 5L -> 7L)
      val rawEdges = sc.parallelize(edges)
      val graph = Graph.fromEdgeTuples(rawEdges, -1)
      val puGraph = PregelUnfolding.run(graph, 1)
      for ((id, com) <- puGraph.vertices.collect()) {
        println(id)
        println(com)
      }
    }
  }

}
