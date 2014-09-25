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
    var ufGraph = graph.mapVertices((vid, _) => List[Long]())

    var ufWorkGraph = graph.mapVertices((vid, _) => vid)

    var iter = 0

    while (iter < maxIter) {
      iter += 1

      //Recording which community each vertex belongs to in this iteration
      ufGraph = ufGraph.joinVertices(ufWorkGraph.vertices)((vid, attr, cid) => cid :: attr)


      def gainMod(a: Long, b: Long): Boolean = ???
      def modGained(a: Long, b: Long): Long = ???


      val initialMessage = List[Long]()

      ufWorkGraph = Pregel(ufWorkGraph, initialMessage, activeDirection = EdgeDirection.Either)(
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
      )

      def sendMessage(e: EdgeTriplet[VertexId, ED]) = ???



      def mergeMessage(neighbor1: Long, neighbor2: Long): List[Long] = ???

      def vertexProgram(vid: VertexId, attr: Long, message: List[Long]) = ???

      ufWorkGraph = Pregel(ufWorkGraph, initialMessage, activeDirection = EdgeDirection.Either)(
        vprog = vertexProgram,
        sendMsg = sendMessage,
        mergeMsg = mergeMessage)
    }
  }
}
