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

  def run[VD, ED: ClassTag](graph: Graph[VD, ED], maxIter: Int): Graph[VertexId, ED] = {

    //each node belongs to its own community
    var ufGraph = graph

    var ufWorkGraph = graph.mapVertices((vid, _) => Set(vid))

    var iter = 0

    while (iter < maxIter) {
      iter += 1

      //Recording which community each vertex belongs to in this iteration
      ufGraph = ufGraph.joinVertices(ufWorkGraph.vertices)((vid, attr, cid) => cid :: attr :: Nil

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



      def modGained(neigh1: Set[VertexId], neigh2: Set[VertexId]): Long = ???

      def gainMod(a: Set[VertexId], b: Set[VertexId]): Boolean = if (modGained(a, b) > 0) true else false



      val initialMessage = List[Set[VertexId]]()
      ufWorkGraph = Pregel(ufWorkGraph, initialMessage, activeDirection = EdgeDirection.Either)(
        //need to return the attr if x is empty, do it in "modGained"
        (vid, attr, message) => message.maxBy(x => modGained(attr, x)),

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
  }
}

