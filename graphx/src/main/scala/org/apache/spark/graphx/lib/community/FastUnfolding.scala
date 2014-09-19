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

import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{Level, Logger}

object FastUnfolding {

  var improvement = false
  var communityRdd: RDD[(Long, Long)] = null
  /**
   * the sum of degrees of links incident to each node in its community
   */
  var totRdd: RDD[(Long, Long)] = null
  /**
   * the sum of the degrees of links inside each community
   */
  var inRdd: RDD[(Long, Long)] = null
  /**
   * represents current community of each node
   */
  var n2cRdd: RDD[(Long, Long)] = null

  /**
   * Load edges from file.
   */
  def loadEdgeRdd(edgeFile: String, partitionNum: Int, sc: SparkContext): RDD[(Long, Long)] = {
    val edgeRdd = sc.textFile(edgeFile, partitionNum).flatMap {
      case (line) =>
        val arr = ArrayBuffer[(Long, Long)]()
        val regex = " "
        val ss = line.split(regex)
        if (ss.size >= 2) {
          val src = ss(0).toLong
          val dst = ss(1).toLong
          arr += ((src, dst))
        }
        arr
    }
    edgeRdd
  }

  /**
   * Generate a random array from the original vertex array.
   */
  def generateRandomArray[T:ClassTag](oriArray: Array[T]): Array[T] = {
    val size = oriArray.size
    val result = new Array[T](size)
    for (i <- 0 until size) {
      result(i) = oriArray(i)
    }
    // TODO 暂时不用，为了测试，使用固定的序列
    //    val random = new Random()
    //    for (i <- 0 until size){
    //      val randPos = random.nextInt(size)
    //      val tmp = result(i)
    //      result(i) = result(randPos)
    //      result(randPos) = tmp
    //    }
    result
  }

  /**
   * Calculate the self loop numbers for every vertex in graph.
   */
  def generateSelfLoopRdd[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): RDD[(Long, Long)] = {
    val firstPart = graph.edges
      .map(e => if (e.srcId == e.dstId) (e.srcId, 1L) else (e.srcId, 0L))

    val secondPart = graph.edges
      .map(e => if (e.srcId == e.dstId) (e.srcId, 0L) else (e.dstId, 0L))

    (firstPart ++ secondPart).reduceByKey(_ + _)
  }

  /**
   * Generate an one row RDD with a number pair <value1, value2>.
   * @param value1 the first value of the row in the RDD
   * @param value2 the second value of the row in the RDD
   * @param sc current Spark context
   * @return
   */
  def changeIntoRdd(
      value1: Long,
      value2: Long,
      sc: SparkContext): RDD[(Long, Long)] = {
    val tmpArray = Array((value1, value2))
    val resultRdd = sc.parallelize(tmpArray).map(e => (e._1, e._2))
    resultRdd
  }

  /**
   * Generate an multi-edge RDD from a graph.
   * For example, original edge contains (a,b), this function will return
   * two pairs (a,b),(b,a).
   */
  def loadMultiEdgeRdd[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED]): RDD[(Long, Long)] = {
    val edgeRdd = graph.edges
      .filter(e => e.srcId != e.dstId)
      .flatMap(e => Array((e.srcId, e.dstId), (e.dstId, e.srcId)))

    edgeRdd
  }

  /**
   * Calculate the total degree for a specified node's all neighbor communities.
   * The first value in the RDD represents a neighbor community's id,
   * and the second value represents the total degree for this community.
   * @param node the specified node id
   * @param graph the original graph
   * @param sc current Spark context
   * @tparam VD
   * @tparam ED
   * @return
   */
  def generateNeighCommRdd[VD: ClassTag, ED: ClassTag](
      node: Long,
      graph: Graph[VD,ED],
      sc: SparkContext): RDD[(Long,Long)] = {

    val edgeRdd = loadMultiEdgeRdd(graph)

    val nodeCommRdd = edgeRdd.distinct()
      .filter(e => e._1 == node)
      .map(e => (e._2, 1))
      .leftOuterJoin(n2cRdd)
      .map(e => (e._1, e._2._2.getOrElse(0L)))

    val curCommRdd = n2cRdd.filter(e => e._1 == node).map(e => (e._2, 0L))

    val neighCommRdd = edgeRdd.filter(e => e._1 == node)
      .map(e => (e._2, e._1))
      .groupBy(e => e._1)
      .map(e => (e._1, e._2.size))
      .join(nodeCommRdd)
      .map(e => (e._2._2, e._2._1.toLong))
      .++(curCommRdd)
      .reduceByKey(_ + _)

    neighCommRdd
  }

  /**
   * Calculate the modularity change after we add a node to
   * a specified community.
   * @param neighCommRdd the sum of degrees of current node's every neighbor community
   * @param comm we will add current node to this community
   * @param nodeDegree degree of current node
   * @param totalDegree total degree of the graph
   * @return
   */
  def modularityGain(
      neighCommRdd: RDD[(Long, Long)],
      comm: Long,
      nodeDegree: Long,
      totalDegree: Double): Long = {

    var bestIncrease = 0.0
    var bestComm = comm

    val commWeighTotArray = neighCommRdd.join(totRdd).collect()

    for (i <- 0 until commWeighTotArray.size) {
      val comm = commWeighTotArray(i)._1
      val dnc = commWeighTotArray(i)._2._1.toDouble
      val totc = commWeighTotArray(i)._2._2.toDouble
      val increase = dnc - totc * nodeDegree/totalDegree
      if (increase > bestIncrease) {
        bestComm = comm
        bestIncrease = increase
      }
    }

    bestComm
  }

  /**
   * In remove step, update totRdd
   */
  def removeNodeTot(
      node: Long,
      degree: Long,
      sc: SparkContext) = {
    val nodeDegreeRdd = changeIntoRdd(node, degree, sc)
    val commDegreeRdd = n2cRdd.filter(e => e._1 == node)
      .join(nodeDegreeRdd)
      .map(e => (e._2._1, e._2._2.toLong))

    val oldTotRdd = totRdd
    totRdd = totRdd.leftOuterJoin(commDegreeRdd)
      .map(e => (e._1, e._2._1 - e._2._2.getOrElse(0L)))
      .cache()

    totRdd.count()

    nodeDegreeRdd.unpersist()
    commDegreeRdd.unpersist()
    oldTotRdd.unpersist()
  }

  /**
   * In remove step, update inRdd
   */
  def removeNodeIn(neighCommRdd: RDD[(Long, Long)],selfLoopRdd: RDD[(Long, Long)], node: Long) = {
    val commNeighRdd = n2cRdd.filter(e => e._1 == node)
      .map(e => (e._2, e._1))
      .join(neighCommRdd)
      .map(e => (e._1, e._2._2))
    val commSelfLoopRdd = selfLoopRdd.filter(e => e._1 == node)
      .join(n2cRdd)
      .map(e => (e._2._2, e._2._1))

    val oldInRdd = inRdd
    inRdd = inRdd.leftOuterJoin(commNeighRdd)
      .map(e => (e._1, e._2._1 - 2 * e._2._2.getOrElse(0L)))
      .leftOuterJoin(commSelfLoopRdd)
      .map(e => (e._1, e._2._1 - e._2._2.getOrElse(0L)))
      .cache()

    inRdd.count()

    oldInRdd.unpersist()
    commNeighRdd.unpersist()
    commSelfLoopRdd.unpersist()
  }

  /**
   * In remove step, update n2cRdd
   */
  def removeNodeN2c(node: Long, sc:SparkContext) = {
    val nodeRdd = changeIntoRdd(node, node, sc).cache()
    val oldN2cRdd = n2cRdd
    n2cRdd = n2cRdd.leftOuterJoin(nodeRdd).map{
      e =>
        if (e._2._2.getOrElse(-1L) != -1L)
          (e._1, -1L)
        else
          (e._1, e._2._1)
    }.cache()

    n2cRdd.count()

    nodeRdd.unpersist()
    oldN2cRdd.unpersist()
  }

  /**
   * Remove a node from its current community.
   * @param neighCommRdd the sum of degrees of current node's every neighbor community
   * @param selfLoopRdd the number of each node's self loop
   * @param node id of the specified node
   * @param degree degree of the specified node
   * @param sc current Spark context
   * @return
   */
  def removeNode(
      neighCommRdd: RDD[(Long, Long)],
      selfLoopRdd: RDD[(Long, Long)],
      node: Long,
      degree: Long,
      sc: SparkContext) = {
    // update in
    removeNodeIn(neighCommRdd, selfLoopRdd, node)

    // update tot
    removeNodeTot(node,degree,sc)

    // update n2c
    removeNodeN2c(node, sc)
  }

  /**
   * In insert step, update totRdd
   */
  def insertNodeTot(
      bestComm: Long,
      degree: Long,
      sc: SparkContext) = {
    val totInsertRdd = changeIntoRdd(bestComm, degree, sc)
    val oldTotRdd = totRdd
    totRdd = totRdd.leftOuterJoin(totInsertRdd).map{
      e => (e._1, e._2._1 + e._2._2.getOrElse(0L))
    }.cache()

    totRdd.count()

    oldTotRdd.unpersist()
  }

  /**
   * In insert step, update inRdd
   */
  def insertNodeIn(
      neighCommRdd: RDD[(Long, Long)],
      selfLoopRdd: RDD[(Long, Long)],
      node: Long,
      bestComm: Long,
      sc: SparkContext) = {
    val selfLoop = selfLoopRdd.filter(e => e._1 == node).map(e => e._2).first()
    val bestCommWeigh = neighCommRdd.filter(e => e._1 == bestComm).first()._2
    val inChangeValue = 2 * bestCommWeigh + selfLoop
    val inInsertRdd = changeIntoRdd(bestComm, inChangeValue, sc)

    val oldInRdd = inRdd
    inRdd = inRdd.leftOuterJoin(inInsertRdd).map{
      e => (e._1, e._2._1 + e._2._2.getOrElse(0L))
    }.cache()

    inRdd.count()

    oldInRdd.unpersist()
  }

  /**
   * In insert step, update n2cRdd
   */
  def insertNodeN2c(node: Long, bestComm: Long, sc: SparkContext) = {
    val nodeRdd = changeIntoRdd(node, node, sc)
    val oldN2cRdd = n2cRdd
    n2cRdd  = n2cRdd.leftOuterJoin(nodeRdd).map{
      e =>
        if (e._2._2.getOrElse(-1L) != -1L)
          (e._1, bestComm)
        else
          (e._1, e._2._1)
    }.cache()

    n2cRdd.count()

    nodeRdd.unpersist()
    oldN2cRdd.unpersist()
  }

  /**
   * Insert a node into a specified community.
   * @param neighCommRdd the sum of degrees of current node's every neighbor community
   * @param selfLoopRdd the number of each node's self loop
   * @param bestComm the specified community to insert current node
   * @param node id of the specified node
   * @param degree degree of the specified node
   * @param sc current Spark context
   * @return
   */
  def insertNode(
      neighCommRdd: RDD[(Long, Long)],
      selfLoopRdd: RDD[(Long, Long)],
      bestComm: Long,
      node: Long,
      degree: Long,
      sc: SparkContext) = {
    // update tot
    insertNodeTot(bestComm, degree, sc)

    // update in
    insertNodeIn(neighCommRdd, selfLoopRdd, node, bestComm, sc)

    // update n2c
    insertNodeN2c(node,bestComm, sc)
  }

  /**
   * Reassign community for the specified node.
   * @param node id of the specified node
   * @param oriComm original community of the specified node
   * @param totalDegree graph's total degree
   * @param selfLoopRdd rdd records graph's self loops
   * @param graph original graph
   * @param sc current Spark context
   * @tparam VD
   * @tparam ED
   * @return
   */
  def reCommunityForOneNode[VD: ClassTag, ED: ClassTag](
      node: Long, oriComm: Long,
      totalDegree: Double,
      selfLoopRdd: RDD[(Long, Long)],
      graph: Graph[VD, ED], sc: SparkContext): Long = {

    val degree = graph.degrees.filter(e => e._1.toLong == node).map(e => e._2).first().toLong
    val neighCommRdd = generateNeighCommRdd(node, graph, sc).cache()

    removeNode(neighCommRdd, selfLoopRdd, node, degree, sc)
    val bestComm = modularityGain(neighCommRdd, oriComm, degree, totalDegree)
    insertNode(neighCommRdd, selfLoopRdd, bestComm, node, degree, sc)

    neighCommRdd.unpersist()

    bestComm
  }

  /**
   * regenerate totRdd, inRdd and n2cRdd
   * @param sc current Spark context
   */
  def reGenerateRdd(sc: SparkContext) {
    val totArray: Array[(Long, Long)] = totRdd.collect()
    val inArray: Array[(Long, Long)] = inRdd.collect()
    val n2cArray: Array[(Long, Long)] = n2cRdd.collect()

    val oldTotRdd = totRdd
    totRdd = sc.parallelize(totArray).cache()
    oldTotRdd.unpersist()
    val oldInRdd = inRdd
    inRdd = sc.parallelize(inArray).cache()
    oldInRdd.unpersist()
    val oldN2cRdd = n2cRdd
    n2cRdd = sc.parallelize(n2cArray).cache()
    oldN2cRdd.unpersist()
  }

  /**
   * Try to reassign each node to its neighbor community.
   * @param graph the original graph
   * @param sc current Spark context
   * @param maxIters maximum times for total iterations
   * @param minChange minimum change, iterations stops if change less than this value
   * @tparam VD
   * @tparam ED
   * @return
   */
  def reCommunity[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED],
      sc: SparkContext,
      maxIters: Int = Int.MaxValue,
      minChange: Double = 0.01): RDD[(Long,Long)] = {

    n2cRdd = graph.vertices.map(e => (e._1.toLong, e._1.toLong))
    val selfLoopRdd = generateSelfLoopRdd(graph).cache()
    inRdd = selfLoopRdd
    // TODO degree的定义，需要再考虑
    //  var totRdd = graph.degrees.join(selfLoopRdd).map(e => (e._1.toLong, e._2._1.toLong - e._2._2))
    totRdd = graph.degrees.map(e => (e._1.toLong, e._2.toLong))
    val totalDegree = graph.degrees.map(_._2).sum()
    var curModularity = calcModularity(totalDegree)
    var newModularity = curModularity
    val vertexArray = graph.vertices.map(e => e._1.toLong).collect()
    val randomArray = generateRandomArray(vertexArray)
    var iters = 0
    var movement = 0

    do {
      movement = 0
      curModularity = newModularity

      for (i <- 0 until randomArray.size) {
        val node = randomArray(i)
        val oriComm = n2cRdd.filter(e => e._1 == node).first()._2
        val bestComm = reCommunityForOneNode(node, oriComm, totalDegree, selfLoopRdd, graph, sc)

        if (bestComm != oriComm) {
          movement += 1
        }
        println("** iters: " + iters + "\ti: " + i)
      }

      newModularity = calcModularity(totalDegree)

      if (movement > 0)
        improvement = true

      iters += 1

      reGenerateRdd(sc)

    } while (movement > 0 && (newModularity - curModularity) > minChange && iters < maxIters)

    selfLoopRdd.unpersist()

    if (improvement) {
      updateCommunity()
    }

    reGraphEdges(graph)
  }

  /**
   * Generate a new edge rdd, according to graph.
   * @param graph the original graph
   * @tparam VD
   * @tparam ED
   * @return
   */
  def reGraphEdges[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED]): RDD[(Long, Long)] = {

    val edgeRdd = graph.edges.flatMap{
      case(e) =>
        val arr = ArrayBuffer[(Long, Long)]()
        arr += ((e.srcId, e.dstId))
        arr
    }

    val newEdgeRdd = edgeRdd.leftOuterJoin(n2cRdd)
      .map(e => (e._2._1, e._2._2.get))
      .leftOuterJoin(n2cRdd)
      .map(e => (e._2._1, e._2._2.get))

    newEdgeRdd
  }

  /**
   * Calculate modularity of current community assignment.
   * @param totalDegree total degree of the graph
   * @return
   */
  def calcModularity(totalDegree: Double): Double = {
    inRdd.join(totRdd)
      .filter(e => e._2._2 > 0)
      .map{ e =>
      val inValue = e._2._1.toDouble
      val totValue = e._2._2.toDouble
      inValue / totalDegree - Math.pow(totValue / totalDegree, 2)
    }.reduce(_ + _)
  }

  /**
   * Update each node's community information.
   */
  def updateCommunity() {
    if (null == communityRdd) {
      communityRdd = n2cRdd.cache()
    } else {
      val oldCommunityRdd = communityRdd
      communityRdd = communityRdd.map(e => (e._2, e._1))
        .join(n2cRdd)
        .map(e => (e._2._1, e._2._2)).cache()
      oldCommunityRdd.unpersist()
    }
  }

  /**
   * Output the community assignment into file.
   * @param file output file
   */
  def outputCommunity(file: String) {
    if (null == communityRdd) {
      println("Community Rdd is empty.")
      return
    }
    communityRdd.map{
      e => e._1 + "," + e._2
    }.saveAsTextFile(file)
  }

  /**
   * Calculation of current modularity
   */
  def calcCurrentModularity(edgeRdd: RDD[(Long, Long)]): Double = {
    if (null == communityRdd)
      return 0.0

    val reverseCommunityRdd = communityRdd.map(e => (e._2, e._1))

    val commEdgeRdd = reverseCommunityRdd.join(reverseCommunityRdd).map{
      e =>
        if (e._2._1 < e._2._2)
          ((e._2._1, e._2._2), e._1)
        else
          ((e._2._2, e._2._1), e._1)
    }.distinct()

    val edgeCountRdd = edgeRdd.map(e => (e, 1)).reduceByKey(_ + _)

    val commEdgeWeightRdd = commEdgeRdd.join(edgeCountRdd).map(e => (e._1, e._2._2))

    val graph = Graph.fromEdgeTuples(edgeRdd, 1L)
    val degreeRdd = graph.degrees.map(e => (e._1.toLong, e._2.toLong))
    val totalDegree = graph.degrees.map(e => e._2).reduce(_+_).toDouble

    val edgeWeightRdd = edgeRdd.distinct()
      .join(degreeRdd)
      .map(e => (e._2._1, (e._1, e._2._2)))
      .join(degreeRdd)
      .map{e =>
      if (e._1 < e._2._1._1)
        ((e._1, e._2._1._1), e._2._1._2 * e._2._2)
      else
        ((e._2._1._1, e._1), e._2._1._2 * e._2._2)
    }

    val result = commEdgeWeightRdd.join(edgeWeightRdd)
      .map(e => e._2._1.toDouble - e._2._2.toDouble/totalDegree)
      .reduce(_+_)

    result / totalDegree
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
               maxIters: Int = Integer.MAX_VALUE) {

    var current = 0
    val oriEdgeRdd = loadEdgeRdd(edgeFile, partitionNum, sc).cache()
    var edgeRdd = loadEdgeRdd(edgeFile, partitionNum, sc)

    do{
      val newEdgeRdd = edgeRdd.cache()
      val graph = Graph.fromEdgeTuples(newEdgeRdd, 1L).cache()
      edgeRdd = reCommunity(graph, sc, maxIters, minChange)

      val modularity = calcCurrentModularity(oriEdgeRdd)
      println("-------- times: " + current + "\tmodularity is: " + modularity)

      newEdgeRdd.unpersist()
      graph.unpersistVertices()
      graph.edges.unpersist()
      current += 1
    } while(improvement && current < maxProcessTimes)

    oriEdgeRdd.unpersist()
  }

  def main(args: Array[String]) {
    if (args.size < 4) {
      println("ERROR INPUT!")
      return
    }

    println("FastUnfolding begins...")

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

    process(input, partitionNum, sc, maxProcessTimes, minChange, maxIters)

    outputCommunity(output)

    println("FastUnfolding ends...")
  }
}

