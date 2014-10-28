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

case class UnfoldMsg(myId: VertexId, myNeighbors: Array[VertexId], myCommunity: Set[VertexId],
                     myOuterLinkCount: Int, myIfSingle: Boolean)

case class NodeAttr(neighbors: Array[VertexId], community: Set[VertexId], outerLinkCount: Int,
                    innerLinkCount: Int, largestGain: Double, ifSingle: Boolean)

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
    var firstPassGraph = ufWorkGraph.mapVertices((vid, attr) => NodeAttr(attr, Set(vid), attr.size, 0, 0, true))

    var iter = 0

    while (iter < maxIter) {
      iter += 1

      //generate a graph with four elements as its vertex properties, including
      //1. neighbor vertex's indexes;
      //2. its community's members' indexes;
      //3. the links incidents to its community;
      //4. the links inside its community
      //the 2,3,4 are all for the first pass in fastunfolding

      val InitialMessage: List[UnfoldMsg] = Nil  //how to define initial message

      firstPassGraph = Pregel(firstPassGraph, InitialMessage, maxIterations = 1, activeDirection = EdgeDirection.Either)(
        vprog = (vid, attr, message) => {

          val messageReborn = message.map(msg =>  if(!msg.myIfSingle) UnfoldMsg(msg.myId, msg.myNeighbors,
            Set(msg.myId), msg.myNeighbors.size, true) else msg)

          val attrReborn =
            if (attr.ifSingle) NodeAttr(attr.neighbors, Set(vid), attr.neighbors.size, 0, 0, true)
            else attr


          def kiin(a: Array[VertexId], b: Set[VertexId]) = a.map(x => if (b.contains(x)) 1 else 0).reduce(_ + _)

          def modGain(msg: UnfoldMsg): Double = kiin(msg.myNeighbors, attrReborn.community) / (2 * numGraphEdges) -
            attrReborn.outerLinkCount * msg.myOuterLinkCount / (2 * numGraphEdges * numGraphEdges)

          if (message.isEmpty) {
            attrReborn
          } else {

            val largestMes = messageReborn.maxBy(a => modGain(a)) // message.maxBy{a => modGain(a)}
            val largestGain = modGain(largestMes)

            //if (largestGain <= 0) attr
            //else 
            NodeAttr(attrReborn.neighbors, attrReborn.community + largestMes.myId,
              attrReborn.outerLinkCount + largestMes.myOuterLinkCount -
                2 * kiin(largestMes.myNeighbors, attrReborn.community),
              attrReborn.innerLinkCount + kiin(largestMes.myNeighbors, attrReborn.community),
              largestGain, attrReborn.ifSingle)//
          }

        } ,


        sendMsg = e => {
          if (e.srcAttr.ifSingle) {
            Iterator((e.dstId, List(UnfoldMsg(e.srcId, e.srcAttr.neighbors, e.srcAttr.community,
              e.srcAttr.outerLinkCount, e.srcAttr.ifSingle))))
          } else if(e.dstAttr.ifSingle) {
            Iterator((e.srcId, List(UnfoldMsg(e.dstId, e.dstAttr.neighbors, e.dstAttr.community,
              e.dstAttr.outerLinkCount, e.srcAttr.ifSingle))))
          } else Iterator()
        },

        mergeMsg = (neighbor1, neighbor2) => neighbor1 ++ neighbor2
      )

      val sInitialMessage = true
      firstPassGraph = Pregel(firstPassGraph, sInitialMessage, maxIterations = 1,
        activeDirection = EdgeDirection.Either)(
        vprog = (vid, attr, message) =>
          NodeAttr(attr.neighbors, attr.community, attr.outerLinkCount,
            attr.innerLinkCount, attr.largestGain, message),
        sendMsg = e => {
          if (e.srcAttr.community == e.dstAttr.community) {
            Iterator((e.dstId, false),(e.srcId, false))
          } else Iterator()
        },
        mergeMsg = (a, b) => a && b
      )

      // propogate the community newest changes, no way to unify but keep one of two
      val tInitialMessage = NodeAttr(Array(), Set(), 0, 0, Double.MinValue, false)
      firstPassGraph = Pregel(firstPassGraph, tInitialMessage, activeDirection = EdgeDirection.Either)(
          vprog = (vid, attr, message) => {
            message
          }
            ,
          sendMsg = e => {
            if (!e.srcAttr.ifSingle && !e.dstAttr.ifSingle) {
              if (e.srcAttr.community subsetOf e.dstAttr.community)
              Iterator((e.srcId, e.dstAttr))
              else if (e.dstAttr.community subsetOf e.srcAttr.community)
              Iterator((e.dstId, e.srcAttr))
              else if (e.srcAttr.largestGain > e.dstAttr.largestGain)
              Iterator((e.dstId, e.srcAttr))
              else if (e.dstAttr.largestGain > e.srcAttr.largestGain)
                Iterator((e.srcId, e.dstAttr))
              else Iterator()

            } else if (!e.srcAttr.ifSingle && e.dstAttr.ifSingle){
              if (e.srcAttr.community(e.dstId))
              Iterator((e.dstId, e.srcAttr))
              else if (e.dstAttr.community(e.srcId))
              Iterator((e.srcId, e.dstAttr))
              else Iterator()
            }
              else Iterator()
          },
          mergeMsg = (a, b) => {
            if (a.largestGain > b.largestGain) a else b
          }
        )



      /**
      secondPassGraph = Pregel(firstPassGraph, InitialMessage, maxIterations = 2, activeDirection = EdgeDirection.Either)(
        (vid, attr, message) => {


        } ,

        e => {
          if (e.srcAttr.community.size == 2 && e.srcAttr.community == e.dstAttr.community) {
            Iterator((e.dstId, true,
              e.srcAttr.outerLinkCount, e.srcAttr.ifMatch))))
          } else {
            Iterator()
          }
        },

        (neighbor1, neighbor2) => neighbor1 ++ neighbor2
      )*/
    }
    firstPassGraph
  }
}

