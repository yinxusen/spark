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
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{Level, Logger}
import scala.Array
import scala.collection.mutable
import scala.Some

object FastUnfoldingParallel {
  def main(args: Array[String]) {
    if (args.size < 4) {
      println("ERROR INPUT!")
      return
    }

    println("FastUnfoldingParallel Begins...")

    val mode = args(0)  // "local" or yarn-standalone
    if(mode.startsWith("local"))
      Logger.getRootLogger.setLevel(Level.OFF)
    val input = args(1) // input file of edge information
    val partitionNum = args(2).toInt  // partition number
    val output = args(3)  // output file path
    val maxProcessTimes = args(4).toInt
    val minChange = args(5).toDouble
    val maxIters = args(6).toInt

    val fs = FileSystem.get(new Configuration())
    if (fs.exists(new Path(args(3)))) fs.delete(new Path(args(3)), true)

    val sc = new SparkContext(mode, "FastUnfolding")

    val result = process(input, partitionNum, sc, maxProcessTimes, minChange, maxIters)

    outputCommunity(output, result)

    println("FastUnfoldingParallel Ends...")
  }

  var improvement = false
  var communityResult: RDD[(Long, Long)] = null
  var graphEdges: RDD[(Long, Long)] = null
  var graphEdgesWeight: RDD[((Long, Long), Int)] = null
  var graphDegrees: RDD[(Long, Int)] = null
  var totalDegree: Double = 0.0
  var bestModularity: Double = 0.0

  /**
   * Load edges from file.
   */
  def loadRawEdge(edgeFile: String, partitionNum: Int, sc: SparkContext): RDD[(Long, Long)] = {
    println("Get in LoadRawEdge. File Is: " + edgeFile)

    val edgeRdd = sc.textFile(edgeFile, partitionNum).repartition(partitionNum).flatMap {
      case (line) =>
        val arr = ArrayBuffer[(Long, Long)]()
        val regex = ","
        val ss = line.split(regex)
        if (ss.size >= 2) {
          val src = ss(0).toLong
          val dst = ss(1).toLong
          if (src < dst)
            arr += ((src, dst))
          else
            arr += ((dst, src))
        }
        arr
    }

    edgeRdd
  }

  def getEdgeRdd(rawEdges: RDD[(Long, Long)]): RDD[Edge[Long]] = {
    rawEdges.groupBy(e => e).map{
      case ((srcComm, dstComm), arrays) => new Edge[Long](srcComm, dstComm, arrays.size)
    }
  }

  /**
   * Generate a new graph with VertexData
   * @param graph original graph
   * @tparam VD
   * @return
   */
  def generateInitGraph[VD: ClassTag](graph: Graph[VD, Long], vertexTotalWeight: RDD[(Long, Long)]): Graph[VertexData, Long] = {

    val newGraph = graph.mapVertices[VertexData]((vid, vd) => new VertexData(vid.toLong))
      .joinVertices(vertexTotalWeight){
      case (vid, vertexData, degree) => vertexData.setDegreeAndCommWeight(degree)
    }

    newGraph
  }

  /**
   * Calculate the modularity gain, and return the best community choice.
   * @param array node's neighbors
   * @param totalDegree total degree in the graph
   * @return
   */
  def getBestCommunity(array: Array[VertexData], totalDegree: Double): Long = {
    if (null == array || 0 == array.size) {
      return 0L
    }

    val degree = array(0).neighDegree
    val oriCommunity = array(0).neighCommunity

    //    if (rand.nextDouble() > 0.8)
    //      return oriCommunity

    val insideWeightMap = new mutable.HashMap[Long, Long]
    val outsideWeightMap = new mutable.HashMap[Long, Long]
    for(i <- 0 until array.size) {
      val vertexData = array(i)
      val community = vertexData.community
      val edgeAttr = vertexData.edgeAttr
      val preValue = insideWeightMap.getOrElse(community, 0L)
      insideWeightMap.put(community, preValue + edgeAttr)
      outsideWeightMap.put(community, vertexData.communityWeight)
    }

    if (outsideWeightMap.contains(oriCommunity)) {
      val preValue = outsideWeightMap.getOrElse(oriCommunity,0L)
      outsideWeightMap.put(oriCommunity, preValue - degree)
    }

    assert(insideWeightMap.size == outsideWeightMap.size)

    val iter = insideWeightMap.keysIterator
    var bestCommunity = oriCommunity
    var bestGain = 0.0
    while(iter.hasNext) {
      val key = iter.next()
      val insideWeight = insideWeightMap.get(key).getOrElse(0L).toDouble
      val outsideWeight = outsideWeightMap.get(key).getOrElse(0L).toDouble
      val gain = insideWeight - 2 * degree * outsideWeight / totalDegree
      if (gain > bestGain) {
        bestGain = gain
        bestCommunity = key
      }
    }

    bestCommunity
  }

  def edgeMapFunc(et: EdgeTriplet[VertexData, Long]): Iterator[(VertexId, Array[VertexData])] = {
    if (et.srcId != et.dstId) {
      val edgeAttr = et.attr

      val srcDegree = et.srcAttr.degree
      val srcComm = et.srcAttr.community
      val srcCommWeight = et.srcAttr.communityWeight
      val dstVertexData = et.dstAttr.setNeighbor(srcDegree, srcComm, srcCommWeight, edgeAttr)

      val dstDegree = et.dstAttr.degree
      val dstComm = et.dstAttr.community
      val dstCommWeight = et.dstAttr.communityWeight
      val srcVertexData = et.srcAttr.setNeighbor(dstDegree, dstComm, dstCommWeight, edgeAttr)
      Iterator((et.srcId, Array(dstVertexData)), (et.dstId, Array(srcVertexData)))
    } else {
      Iterator.empty
    }
  }

  def calcChangeRate(preRdd: RDD[(Long, Long)], curRdd: RDD[(Long, Long)]): Double = {
    if (null == preRdd)
      return Double.MaxValue

    val preCount = preRdd.count()
    val curCount = curRdd.count()

    if (preCount != curCount) {
      println("Count Not Right! Pre Is " + preCount + "\tCur Is " + curCount)
      return -1
    }

    val changeNum = preRdd.join(curRdd)
      .filter{case (vid, (preComm, curComm)) => preComm != curComm}
      .count()

    changeNum.toDouble / preCount.toDouble
  }

  def mergeCommunity(oriRdd: RDD[(Long, Long)]): RDD[(Long, Long)] = {

    println("begin mergeCommunity")

    val rawG = Graph.fromEdgeTuples(oriRdd, 1L)
      .mapVertices {
      case (vid, _) => vid.toLong
    }.cache()
    var messages = rawG.mapReduceTriplets(mcSendMsg, mcMergeMsg).cache()
    var activeMessages = messages.count()
    println("messages count " + messages.count())
    var i = 0
    var g = rawG.mapVertices((idx, vd) => vd).cache()
    val maxiter = 30
    while (i < maxiter && activeMessages > 0) {

      i = i + 1
      val newVerts = g.vertices.innerJoin(messages)((id, data, update)
      => mcVprog(id, data, Some(update))).cache()

      val prevG = g
      g = g.outerJoinVertices(newVerts) {
        (vid, oldV, optNewV) => optNewV.getOrElse(oldV)
      }.cache()

      val oldMessage = messages
      messages = g.mapReduceTriplets(mcSendMsg, mcMergeMsg, Some((newVerts, EdgeDirection.Either))).cache()
      activeMessages = messages.count()

      newVerts.unpersist()
      prevG.unpersistVertices()
      prevG.edges.unpersist()
      oldMessage.unpersist()
    }

    rawG.unpersistVertices()
    rawG.edges.unpersist()
    messages.unpersist()

    val result = g.vertices.map(e => (e._1.toLong, e._2)).cache()

    result.count()

    g.unpersistVertices()
    g.edges.unpersist()

    result
  }

  def mcVprog(vid: Long, vert: Long, aggMsg: Option[Long]): Long = {
    var newvert = vert
    aggMsg match {
      case Some(msg) => newvert = math.max(newvert, msg)
      case None => newvert = newvert
    }
    newvert
  }

  def mcSendMsg[ED: ClassTag](edge: EdgeTriplet[VertexId, ED]) = {
    if (edge.srcAttr > edge.dstAttr) {
      Iterator((edge.dstId, edge.srcAttr))
    } else if (edge.srcAttr < edge.dstAttr) {
      Iterator((edge.srcId, edge.dstAttr))
    } else {
      Iterator.empty
    }
  }

  def mcMergeMsg(a:Long, b:Long): Long = {
    math.max(a, b)
  }

  /**
   * Try to reassign each node to its neighbor community in parallel method.
   * @param graph the original graph
   * @param sc current Spark context
   * @param maxIters maximum times for total iterations
   * @param minChange minimum change, iterations stops if change less than this value
   * @tparam VD
   * @return
   */
  def reCommunityParallel[VD: ClassTag](
                                         graph: Graph[VD, Long],
                                         sc: SparkContext,
                                         maxIters: Int = Int.MaxValue,
                                         minChange: Double = 0.01): RDD[(Long,Long)] = {

    println("reCommunityParallel...")
    var iters = 0
    val curDegree = totalDegree

    val vertexTotalWeight = graph.edges.flatMap{
      case (edge) =>
        val array = ArrayBuffer[(VertexId, Long)]()
        array += ((edge.srcId, edge.attr))
        array += ((edge.dstId, edge.attr))
        array
    }.reduceByKey(_ + _).cache()
    println("vertexTotalWeight " + vertexTotalWeight.count())

    var newGraph = generateInitGraph(graph, vertexTotalWeight).cache()
    var currentCommunity: RDD[(Long, Long)] = null
    var changeRate = 0.0

    do {
      val vertexRdd = newGraph.mapReduceTriplets[Array[VertexData]](edgeMapFunc, _ ++ _).cache()
      println("---iters: " + iters + "\tvertexRdd count:" + vertexRdd.count())

      val idCommunity = vertexRdd.map{
        case (vid, vdArray) => (vid, getBestCommunity(vdArray, curDegree))
      }.cache()

      println("---iters: " + iters + "\tidcommunity count:" + idCommunity.count())

      val commWeightTmp = idCommunity.join(vertexTotalWeight).map{
        case (vid, (community, degree)) => (community, degree.toLong)
      }

      val commWeight = commWeightTmp.reduceByKey(_ + _).cache()
      println("---iters: " + iters + "\tcommWeight count:" + commWeight.count())

      val reverseIdCommunity = idCommunity.map(e => (e._2, e._1))
      val updateMessage = reverseIdCommunity.leftOuterJoin(commWeight).map{
        case (community, (vid, weight)) => (vid, (community, weight.getOrElse(0L)))
      }.cache()

      println("---iters: " + iters + "\tupdateMessage count:" + updateMessage.count())

      val preGraph = newGraph
      newGraph = newGraph.joinVertices(updateMessage){
        case (vid, vertexData, (community, weight))
        =>
          val newVertexData = new VertexData(vertexData)
          newVertexData.setCommAndCommWeight(community, weight)
      }.cache()

      println("---iters: " + iters + "\tnewGraph edges count:" +  newGraph.edges.count()
        + "\tnewGraph vertex count:" +  newGraph.vertices.count())

      val preCurrentCommunity = currentCommunity

      val mergedCommunity = mergeCommunity(idCommunity)

      changeRate = calcChangeRate(preCurrentCommunity, mergedCommunity)
      println("---iters: " + iters + "\tchangeRate:" + changeRate)

      if (null != currentCommunity) {
        currentCommunity.unpersist()
      }

      currentCommunity = mergedCommunity
      println("---iters: " + iters + "\tcurrentCommunity count:" + currentCommunity.count())

      iters += 1

      preGraph.unpersistVertices()
      preGraph.edges.unpersist()
      vertexRdd.unpersist()
      commWeight.unpersist()
      updateMessage.unpersist()
      idCommunity.unpersist()

      // TODO 算法终止条件需要再考虑
      //    } while((newModularity - curModularity) > minChange && iters < maxIters)
    } while(iters < maxIters)

    newGraph.unpersistVertices()
    newGraph.edges.unpersist()
    vertexTotalWeight.unpersist()

    val result = currentCommunity

    result
  }

  /**
   * Update each node's community information.
   */
  def updateCommunity(currentResult: RDD[(Long, Long)]) {
    println("updateCommunity...")
    if (null == currentResult) {
      communityResult = null
    } else if (null == communityResult) {
      communityResult = currentResult.cache()
      println("communityResult.count() is " + communityResult.count())
    } else {
      val preRdd = communityResult
      println("top 10")
      preRdd.top(10).foreach(println)
      println("begin.. " + preRdd.count())
      communityResult = preRdd.map(e => (e._2, e._1)).join(currentResult).map{
        case (oldCommunity, (vid, curCommunity)) => (vid, curCommunity)
      }.cache()
      println("end.. " + communityResult.count())
      preRdd.unpersist()
      currentResult.unpersist()
    }
  }

  /**
   * Output the community assignment into file.
   * @param file output file
   */
  def outputCommunity(file: String, result: RDD[(Long, Long)]) {
    if (null == result) {
      println("Community Result Rdd Is Empty.")
      return
    }

    result.map{
      e => e._1 + "," + e._2
    }.saveAsTextFile(file)
  }

  /**
   * Generate a new edge rdd, according to graph.
   * @param graph the original graph
   * @tparam VD
   * @return
   */
  def reGraphEdges[VD: ClassTag](graph: Graph[VD,Long], result: RDD[(Long, Long)]): RDD[Edge[Long]] = {

    val edgeRdd = graph.edges.flatMap{
      case(e) =>
        val arr = ArrayBuffer[(Long, Long)]()
        val weight = e.attr.toInt
        for (i <- 0 until weight) {
          arr += ((e.srcId, e.dstId))
        }
        arr
    }

    val newEdgeRdd = edgeRdd.leftOuterJoin(result)
      .map{ case (srcId, (dstId, srcComm)) => (dstId, srcComm.getOrElse(0L)) }
      .leftOuterJoin(result)
      .map{
      case (dstId, (srcComm, dstComm)) =>
        if (srcComm > dstComm.getOrElse(0L))
          (dstComm.getOrElse(0L), srcComm)
        else
          (srcComm, dstComm.getOrElse(0L))
    }

    val resultEdgeRdd = getEdgeRdd(newEdgeRdd)

    resultEdgeRdd
  }

  /**
   * Calculation of current modularity (original version, not capable for large network)
   */
  def calcCurrentModularityOri(): Double = {
    if (null == communityResult || null == graphEdges)
      return 0.0

    val reverseRdd = communityResult.map(e => (e._2, e._1)).cache()

    println("calcCurrentModularity... reverseCommunityRdd " + reverseRdd.count())

    val commEdgeRawRdd = reverseRdd.join(reverseRdd).cache()
    println("calcCurrentModularity... commEdgeRawRdd " + commEdgeRawRdd.count())

    val commEdgeRdd = commEdgeRawRdd.map{
      case(community, (srcId, dstId)) =>
        if (srcId < dstId)
          ((srcId, dstId), community)
        else
          ((dstId, srcId), community)
    }.distinct().cache()

    println("calcCurrentModularity... commEdgeRdd " + commEdgeRdd.count())

    commEdgeRawRdd.unpersist()

    val edgeCountRdd = graphEdges.map(e => (e, 1)).reduceByKey(_ + _).cache()

    println("calcCurrentModularity... edgeCountRdd " + edgeCountRdd.count())

    val commEdgeWeightRdd = commEdgeRdd.join(edgeCountRdd).map{
      case (edge, (community, count)) => (edge, count)
    }.cache()

    val commEdgeWeightRddCount = commEdgeWeightRdd.count()
    println("calcCurrentModularity... commEdgeWeightRdd " + commEdgeWeightRddCount)

    edgeCountRdd.unpersist()
    commEdgeRdd.unpersist()

    var result = 0.0

    if (commEdgeWeightRddCount != 0) {

      val partResult = commEdgeWeightRdd.join(graphEdgesWeight)
        .map{
        case (edge, (weight, degree)) => weight.toDouble - degree.toDouble / totalDegree
      }
        .reduce(_ + _)
      result += partResult
      println("calcCurrentModularity... result " + result + "\tpart " + partResult + "\ttotaldegree " + totalDegree)
    }
    commEdgeWeightRdd.unpersist()
    reverseRdd.unpersist()

    result / totalDegree
  }

  /**
   * Calculation of current modularity (updated version)
   */
  def calcCurrentModularity(): Double = {
    if (null == communityResult || null == graphEdges)
      return 0.0

    val partEdgeWithCommunity = graphEdges.join(communityResult).map{
      case (srcId, (dstId, srcComm)) => (dstId, srcComm)
    }

    val icEdgeCount = partEdgeWithCommunity.join(communityResult).filter{
      case (dstId, (srcComm, dstComm)) => srcComm == dstComm
    }.map{
      case (dstId, (srcComm, dstComm)) => (srcComm, 1)
    }

    val icEdge = icEdgeCount.groupBy(e => e._1).map(e => (e._1, e._2.size)).cache()

    println("calcCurrentModularity... icEdge " + icEdge.count())

    val dcDegree = graphDegrees.join(communityResult).map{
      case (vid, (degree, community)) => (community, degree)
    }.reduceByKey(_ + _).cache()

    println("calcCurrentModularity... dcDegree " + dcDegree.count())

    val resultRdd = icEdge.join(dcDegree).cache()

    println("calcCurrentModularity... resultRdd " + resultRdd.count())

    val currentDegree = totalDegree

    val result = resultRdd.map{
      case (community, (ic, dc)) => 2 * ic.toDouble / currentDegree - Math.pow(dc.toDouble / currentDegree, 2)
    }.sum()

    println("calcCurrentModularity... result " + result)

    resultRdd.unpersist()
    dcDegree.unpersist()
    icEdge.unpersist()

    result
  }

  /**
   * Calculation of current modularity (step-by-step of original version)
   */
  def calcCurrentModularityStepByStep(): Double = {
    if (null == communityResult || null == graphEdges)
      return 0.0

    println("calcCurrentModularity... communityResult " + communityResult.count())

    val reverseRdd = communityResult.map(e => (e._2, e._1)).cache()

    println("calcCurrentModularity... reverseCommunityRdd " + reverseRdd.count())

    val partition = 100
    var result = 0.0

    for (i <- 0 to partition) {

      println("******* iter : " + i)

      val partReverseRdd = reverseRdd.filter(e => e._1 % partition == i).cache()

      val partCount = partReverseRdd.count()
      println("calcCurrentModularity... partReverseRdd " + partCount)

      if (partCount == 0) {
        partReverseRdd.unpersist()
      } else {

        val commEdgeRawRdd = partReverseRdd.join(partReverseRdd).cache()
        println("calcCurrentModularity... commEdgeRawRdd " + commEdgeRawRdd.count())

        partReverseRdd.unpersist()

        val commEdgeRdd = commEdgeRawRdd.map{
          case(community, (srcId, dstId)) =>
            if (srcId < dstId)
              ((srcId, dstId), community)
            else
              ((dstId, srcId), community)
        }.distinct().cache()

        println("calcCurrentModularity... commEdgeRdd " + commEdgeRdd.count())

        commEdgeRawRdd.unpersist()

        val edgeCountRdd = graphEdges.map(e => (e, 1)).reduceByKey(_ + _).cache()

        println("calcCurrentModularity... edgeCountRdd " + edgeCountRdd.count())

        val commEdgeWeightRdd = commEdgeRdd.join(edgeCountRdd).map{
          case (edge, (community, count)) => (edge, count)
        }.cache()

        val commEdgeWeightRddCount = commEdgeWeightRdd.count()
        println("calcCurrentModularity... commEdgeWeightRdd " + commEdgeWeightRddCount)

        edgeCountRdd.unpersist()
        commEdgeRdd.unpersist()

        if (commEdgeWeightRddCount != 0) {

          val partResult = commEdgeWeightRdd.join(graphEdgesWeight)
            .map{
            case (edge, (weight, degree)) => weight.toDouble - degree.toDouble / totalDegree
          }
            .reduce(_ + _)

          result += partResult

          println("calcCurrentModularity... result " + result + "\tpart " + partResult + "\ttotaldegree " + totalDegree)
        }
        commEdgeWeightRdd.unpersist()
      }

    }

    reverseRdd.unpersist()

    result / totalDegree
  }

  /**
   * Initialization for graphDegree,graphEdges and graphEdgesWeight
   * @param edgeFile
   * @param partitionNum
   * @param sc
   */
  def initialization(edgeFile: String,
                     partitionNum: Int,
                     sc: SparkContext) {

    graphEdges = loadRawEdge(edgeFile, partitionNum, sc).cache()
    println("graphEdges count " + graphEdges.count())
    val graph = Graph.fromEdgeTuples(graphEdges, 1L)
    graphDegrees = graph.degrees.cache()
    println("graphDegrees count " + graphDegrees.count())
    graphDegrees.count()
    totalDegree = graphDegrees.map(e => e._2).sum()

    val graphEgdesWithDegree = graphEdges.distinct()
      .join(graphDegrees)
      .map{
      case (srcId, (dstId, srcDegree)) => (dstId, (srcId, srcDegree))
    }.cache()

    graphEgdesWithDegree.count()

    graphEdgesWeight = graphEgdesWithDegree.join(graphDegrees)
      .map{
      case (dstId, ((srcId, srcDegree), dstDegree)) =>
        if (dstId < srcId)
          ((dstId, srcId), srcDegree * dstDegree)
        else
          ((srcId, dstId), srcDegree * dstDegree)
    }.cache()

    println("graphEdgesWeight count" + graphEdgesWeight.count())

    graphEgdesWithDegree.unpersist()
  }

  /**
   * Construct graph from input edge file, and finish the community assignment task.
   * @param edgeFile source file of edge information
   * @param partitionNum partition number
   * @param sc current Spark context
   * @param maxProcessTimes times for total iterations
   * @param minChange minimum change, iterations stops if change less than this value
   * @param maxIters maximum times for "pass"
   */
  def process(
               edgeFile: String,
               partitionNum: Int,
               sc: SparkContext,
               maxProcessTimes: Int = Integer.MAX_VALUE,
               minChange: Double = 0.001,
               maxIters: Int = Integer.MAX_VALUE): RDD[(Long, Long)] = {

    initialization(edgeFile, partitionNum, sc)

    var preModularity = Double.MinValue
    var bestResult: RDD[(Long, Long)] = null
    var current = 0
    var edgeRdd = getEdgeRdd(graphEdges).cache()
    println("In the beginning edgeRdd count " + edgeRdd.count())

    do{
      val newEdgeRdd = edgeRdd
      val graph = Graph.fromEdges(newEdgeRdd, 1L).cache()

      val curResult = reCommunityParallel(graph, sc, maxIters, minChange)
      //      println("################ times: " + current + "\tcurResult is: " + curResult.count())

      updateCommunity(curResult)

      val modularity = calcCurrentModularity()
      println("################ times: " + current + "\tmodularity is: " + modularity)

      edgeRdd.unpersist()
      if (modularity - preModularity > minChange) {
        improvement = true
        preModularity = modularity
        bestModularity = modularity
        bestResult = communityResult
        edgeRdd = reGraphEdges(graph, curResult).cache()
        println("################ times: " + current + "\tupdate! edgeRdd is: " + edgeRdd.count())
      } else {
        improvement = false
        println("################ times: " + current + "\tnot update!")
      }

      graph.unpersistVertices()
      graph.edges.unpersist()
      current += 1
    } while(improvement && current < maxProcessTimes)

    edgeRdd.unpersist()
    graphEdges.unpersist()
    graphEdgesWeight.unpersist()
    graphDegrees.unpersist()

    bestResult
  }
}