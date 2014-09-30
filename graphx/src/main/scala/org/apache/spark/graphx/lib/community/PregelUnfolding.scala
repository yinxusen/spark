package org.apache.spark.graphx.lib.community

/**
 * Created by lan on 9/18/14.
 */
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.conf.Configuration

object PregelUnfolding {

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], maxIter: Int): Graph[Set[VertexId], ED] = {

    //each node belongs to its own community
    var ufGraph = graph
    // m is the sum of all links in the graph
    val m = graph.degrees.map(_._2).fold(0)(_ + _)

    var ufWorkGraph = graph.mapVertices((vid, _) => Set(vid))

    var iter = 0

    while (iter < maxIter) {
      iter += 1

      //Recording which community each vertex belongs to in this iteration
      //ufGraph = ufGraph.joinVertices(ufWorkGraph.vertices)((vid, attr, cid) => cid :: attr :: Nil

      /*ufWorkGraph = Pregel(ufWorkGraph, initialMessage, activeDirection = EdgeDirection.Either)(
        (vid, attr, message) => message.maxBy(x => modGained(vid, x)),

        e =>
          if (gainMod(e.srcId, e.dstId) && e.srcId < e.dstId) {
            Iterator((e.dstId, e.srcAttr))
          } else if (gainMod(e.srcId, e.dstId) && e.dstId < e.srcId) {
            Iterator(e.srcId, e.dstAttr)
          } else {
            Iterator()

          },
        (neighbor1, neighbor2) => (List(neighbor1) ++ List(neighbor2)).flatten
      )*/
      /*

      def sendMessage(e: EdgeTriplet[VertexId, ED]) = {
        if(gainMod(e.dstAttr, e.srcAttr)) {
          Iterator((e.dstId, Set(e.srcAttr).union(Set(e.dstAttr))),
            (e.srcId, Set(e.srcAttr).union(Set(e.dstAttr))))
        }
        else {
          Iterator()
        }
      }

      def mergeMessage(neighbor1: Set[VertexId], neighbor2: Set[VertexId]): List[Set[VertexId]] =
        List(neighbor1) ++ List(neighbor2)

      def vertexProgram(vid: VertexId, attr: Set[VertexId], message: List[Set[VertexId]]) =
        message.maxBy(x => modGained(attr, x))
        */

      def countInside(com: Set[VertexId]): Long = ufWorkGraph.subgraph(vpred = (id, attr) => com.subsetOf(attr)).degrees
        .map(_._2).fold(0)(_ + _)

      def countOutside(com: Set[VertexId]): Long = ufWorkGraph.subgraph(vpred = (id, attr) => !com.subsetOf(attr)).degrees
        .map(_._2).fold(0)(_ + _)

      def countSumInside(com1: Set[VertexId], com2: Set[VertexId]): Long = ufWorkGraph.subgraph(vpred = (id, attr)
      => com1.subsetOf(attr) || com2.subsetOf(attr)).degrees.map(_._2).fold(0)(_ + _)

      //the modularity gained by moving neigh1 into neigh2
      def modGained(neigh1: Set[VertexId], neigh2: Set[VertexId]): Long = {
        // the sum of links inside neigh1-related community
        val inside1 = countInside(neigh1)
        //the sum of links inside rest of the vertex except neigh1-related community
        val outside1 = countOutside(neigh1)

        val inside2 = countInside(neigh2)

        val outside2 = countOutside(neigh2)

        val connect12 = countSumInside(neigh1, neigh2) - inside1 - inside2

        val connect2out = m - inside2 - outside2

        val connect1out = m - inside1 - outside1

        connect12 / (2 * m) - (connect2out + connect1out) /(2 * m * m)

      }

      def gainMod(a: Set[VertexId], b: Set[VertexId]): Boolean = if (modGained(a, b) > 0) true else false



      val initialMessage = List[Set[VertexId]]()
      ufWorkGraph = Pregel(ufWorkGraph, initialMessage, activeDirection = EdgeDirection.Either)(
        //need to return the attr if x is empty, do it in "modGained"
        (vid, attr, message) => if (message.isEmpty) attr else message.maxBy(x => modGained(attr, x)),

        e => {
          if (gainMod(e.dstAttr, e.srcAttr)) {
            Iterator((e.dstId, List(e.srcAttr.union(e.dstAttr))),
              (e.srcId, List(e.srcAttr.union(e.dstAttr))))
          }
          else {
            Iterator()
          }
        },

        (neighbor1, neighbor2) => neighbor1 ++ neighbor2
      )


    }
    ufWorkGraph
  }
}

