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
import scala.reflect.ClassTag

case class UnfoldMsg(myId: VertexId, myNeighbors: Array[VertexId], myLinkCount: Int)
case class NodeAttr(neighbors: Array[VertexId], community: Set[VertexId], outerLinkCount: Int, innerLinkCount: Int, largestGain: Double)

object PregelUnfolding {

  /**
   * TBD.
   */
  def run[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED],
      maxIter: Int): Graph[NodeAttr, ED] = {

    graph.cache()
    val numGraphEdges = graph.degrees.map(_._2).fold(0)(_ + _) / 2
    /**

    /**
    * Count the number of edges of a sub-graph, given a vertex predication.
    */
    def numberOfEdges(
        predicate: (VertexId, VD) => Boolean): Double = {
      val subgraph = graph.subgraph(vpred = predicate)
      val vert = subgraph.vertices.collect()
      subgraph.degrees.map(_._2).fold(0)(_ + _) / 2.0
    }

    /**
     * Count the number of edges inside a community.
     */
    def countInnerEdges(
        com: Set[VertexId]): Double = {
      numberOfEdges((id, _: VD) => com.contains(id))
    }

    /**
     * Count the number of edges of a sub-graph, which composed of all nodes not in the given
     * community.
     */
    def countOuterEdges(
        com: Set[VertexId]): Double = {
      numberOfEdges((id, _: VD) => ! com.contains(id))
    }

    /**
     * Count the number of edges of a sub-graph, which composed of all nodes in the given two
     * communities.
     */
    def countBipletInnerEdges(
        com1: Set[VertexId],
        com2: Set[VertexId]): Double = {
      numberOfEdges((id, _: VD) => com1.contains(id) || com2.contains(id))
    }

    /**
     * Compute the modularity-gain of merging lhs community with rhs one.
     */
    def modularityGain(
        lhsComm: Set[VertexId],
        rhsComm: Set[VertexId]): Double = {
      val lhsInnerEdges = countInnerEdges(lhsComm)
      val lhsOuterEdges = countOuterEdges(lhsComm)
      val rhsInnerEdges = countInnerEdges(rhsComm)
      val rhsOuterEdges = countOuterEdges(rhsComm)

      val bridgeEdges =
        countBipletInnerEdges(lhsComm, rhsComm) - lhsInnerEdges - rhsInnerEdges

      val edgesConnectedToLHS = numGraphEdges - lhsInnerEdges - lhsOuterEdges
      val edgesConnectedToRHS = numGraphEdges - rhsInnerEdges - rhsOuterEdges

      bridgeEdges / (2 * numGraphEdges)
        - (edgesConnectedToRHS * edgesConnectedToLHS) / (2 * numGraphEdges * numGraphEdges)
    }

    /**
     * Check whether the modularity actually raise after merging two communities.
     */
    def isModularityGained(
        lhsComm: Set[VertexId],
        rhsComm: Set[VertexId]): Boolean = {
      if (modularityGain(lhsComm, rhsComm) > 0) true else false
    }
    */

    /**
     * start all over again from here
     */

    /**
     * generate a graph with vertex property as neighbor vertex' indexes
     */
    val neighID = graph.collectNeighborIds(EdgeDirection.Either)
    val ufWorkGraph = graph.outerJoinVertices(neighID)((vid, _, neighOpt) =>
      neighOpt.getOrElse(Array[VertexId]()))
    var firstPassGraph = ufWorkGraph.mapVertices((vid, attr) => NodeAttr(attr, Set(vid), attr.size, 0, 0))

    var iter = 0

    while (iter < maxIter) {
      iter += 1

      /**
      //generate a graph with vertex property as neighbor vertex' indexes
      ufWorkGraph = Pregel(ufWorkGraph, initialMessage, activeDirection = EdgeDirection.Either)(
        (vid, attr, message) => message,

        e => Iterator((e.dstId, Set(e.srcId)),(e.srcId, Set(e.dstId))),

        (neighbor1, neighbor2) => neighbor1 ++ neighbor2
      )
      */

      //generate a graph with four elements as its vertex properties, including
      //1. neighbor vertex's indexes;
      //2. its community's members' indexes;
      //3. the links incidents to its community;
      //4. the links inside its community
      //the 2,3,4 are all for the first pass in fastunfolding

      val InitialMessage: List[UnfoldMsg] = Nil  //how to define initial message

      firstPassGraph = Pregel(firstPassGraph, InitialMessage, maxIterations = 2, activeDirection = EdgeDirection.Either)(
        (vid, attr, message) => {
          def kiin(a: Array[VertexId], b: Set[VertexId]) = a.map(x => if (b.contains(x)) 1 else 0).reduce(_ + _)

          def modGain(msg: UnfoldMsg): Double = kiin(msg.myNeighbors, attr.community) / (2 * numGraphEdges) - attr.outerLinkCount * msg.myLinkCount / (2 * numGraphEdges * numGraphEdges)

          if (message.isEmpty) {
            attr
          } else {
            val largestMes = message.maxBy(modGain)
            val largestGain = modGain(largestMes)

            //if (largestGain <= 0) attr
            //else
              NodeAttr(attr.neighbors, attr.community + largestMes.myId, attr.outerLinkCount + largestMes.myLinkCount - 2, attr.innerLinkCount + kiin(largestMes.myNeighbors, attr.community), largestGain)
          }

        } ,

        e => {
          if (e.srcAttr.community.size == 1) {
            Iterator((e.dstId, List(UnfoldMsg(e.srcId, e.srcAttr.neighbors, e.srcAttr.outerLinkCount))))
          } else {
            Iterator()
          }
        },

        (neighbor1, neighbor2) => neighbor1 ++ neighbor2
      )
    }
    firstPassGraph
  }
}

