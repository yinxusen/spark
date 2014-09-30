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

    // m is the sum of all links in the graph
    val m = graph.degrees.map(_._2).fold(0)(_ + _)/2

    var ufWorkGraph = graph.mapVertices((vid, _) => Set(vid))

    var iter = 0

    while (iter < maxIter) {
      iter += 1
      //sum of links inside the community composed of vertex in com
      def countInside(com: Set[VertexId]): Long = ufWorkGraph.subgraph(vpred = (id, attr) =>
        com.subsetOf(attr)).degrees.map(_._2).fold(0)(_ + _)/2

      //sum of links inside the community composed of vertex not in com
      def countOutside(com: Set[VertexId]): Long = ufWorkGraph.subgraph(vpred = (id, attr) =>
        !com.subsetOf(attr)).degrees.map(_._2).fold(0)(_ + _)/2

      //sum of links inside the community composed of vertex both in com1 & com2
      def countSumInside(com1: Set[VertexId], com2: Set[VertexId]): Long = ufWorkGraph
        .subgraph(vpred = (id, attr) =>
        com1.subsetOf(attr) || com2.subsetOf(attr)).degrees.map(_._2).fold(0)(_ + _)/2

      //compute the modularity gained by moving neigh1 into neigh2
      def modGained(neigh1: Set[VertexId], neigh2: Set[VertexId]): Long = {
        //sum of links inside community composed of vertex in neigh1
        val inside1 = countInside(neigh1)
        //um of links inside community composed of vertex not in neigh1
        val outside1 = countOutside(neigh1)
        //sum of links inside community composed of vertex in neigh2
        val inside2 = countInside(neigh2)
        //sum of links inside community composed of vertex not in neigh2
        val outside2 = countOutside(neigh2)
        //sum of links connect two communities composed of vertex in neigh1 & neigh2 respectively
        val connect12 = countSumInside(neigh1, neigh2) - inside1 - inside2
        //sum of links connect community composed of vertex in neigh2 & outside neigh2 respectively
        val connect2out = m - inside2 - outside2
        //sum of links connect community composed of vertex in neigh1 & outside neigh1 respectively
        val connect1out = m - inside1 - outside1
        //return the modularity gained by moving neigh1 into neigh2
        connect12 / (2 * m) - (connect2out + connect1out) /(2 * m * m)

      }

      //whether putting community composed of com1 into com2 raise the modularity
      def gainMod(com1: Set[VertexId], com2: Set[VertexId]): Boolean =
        if (modGained(com1, com2) > 0) true else false

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

