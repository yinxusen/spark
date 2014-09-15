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
import scala.util.Random
import scala.Array

object FastUnfolding {

  var improvement = false

  def loadEdgeRdd(edgeFile: String, partitionNum: Int, sc: SparkContext): RDD[(Long, Long)] = {
    val edgeRdd = sc.textFile(edgeFile, partitionNum).flatMap {
      case (line) =>
        val arr = ArrayBuffer[(Long, Long)]()
        val regex = ","
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

  def initialization(file: String, partitionNum: Int, sc: SparkContext):Graph[Long,Int] = {
    val edgeRdd = loadEdgeRdd(file, partitionNum, sc)
    val graph = Graph.fromEdgeTuples(edgeRdd, 1L)
    graph
  }

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

  def generateSelfLoopRdd[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): RDD[(Long, Long)] = {
    val partSelfLoopRdd = graph.edges.map(e => (e.srcId.toLong, e.dstId.toLong))
      .filter(e => e._1 == e._2)
      .groupBy(e => e._1)
      .map(e => (e._1, e._2.size))
    val result = graph.vertices.leftJoin(partSelfLoopRdd) {
      (vid, vdata, nbrsOpt) => nbrsOpt.getOrElse(0)
    }.map(e => (e._1.toLong, e._2.toLong))
    result
  }

  def changeIntoRdd(value1: Long,
                    value2: Long,
                    sc: SparkContext): RDD[(Long, Long)] = {
    val tmpArray = Array((value1, value2))
    val resultRdd = sc.parallelize(tmpArray).map(e => (e._1, e._2))
    resultRdd
  }

  def loadMultiEdgeRdd[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED]): RDD[(Long, Long)] = {
    val edgeRdd = graph.edges.filter(e => e.srcId != e.dstId).flatMap{
      case(e) =>
        val arr = ArrayBuffer[(Long, Long)]()
        arr += ((e.srcId.toLong, e.dstId.toLong))
        arr += ((e.dstId.toLong, e.srcId.toLong))
        arr
    }
    edgeRdd
  }

  def generateNeighCommRdd[VD: ClassTag, ED: ClassTag](nodeRdd: RDD[(Long, Long)],
                                                       graph: Graph[VD,ED],
                                                       n2cRdd: RDD[(Long, Long)],
                                                       sc: SparkContext): RDD[(Long,Long)] = {
    val edgeRdd = loadMultiEdgeRdd(graph)

    // 邻居所属的comm
    val nodeCommRdd = edgeRdd.distinct()
      .join(nodeRdd)
      .map(e => (e._2._1, 1))
      .leftOuterJoin(n2cRdd)
      .map(e => (e._1, e._2._2.getOrElse(0L)))

    val curCommRdd = nodeRdd.join(n2cRdd).map(e => (e._2._2, 0L))

    val neighCommRdd = edgeRdd.join(nodeRdd)
      .map(e => (e._2._1, e._1))
      .groupBy(e => e._1)
      .map(e => (e._1, e._2.size))
      .join(nodeCommRdd)
      .map(e => (e._2._2, e._2._1.toLong))
      .++(curCommRdd)
      .reduceByKey(_+_)

    neighCommRdd
  }

  def modularityGain(totRdd: RDD[(Long, Long)],
                     neighCommRdd: RDD[(Long, Long)],
                     oriComm: Long,
                     nodeDegree: Long,
                     totalDegree: Long): Long = {
    var bestIncrease = 0.0
    var bestComm = oriComm

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

    return bestComm
  }

  def reCommunity[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                              sc: SparkContext,
                                              maxTimes: Int = Int.MaxValue,
                                              minChange: Double = 0.01): RDD[(Long,Long)] = {
    var movement = 0
    var n2cRdd = graph.degrees.map(e => (e._1.toLong, e._1.toLong))
    val selfLoopRdd = generateSelfLoopRdd(graph).cache()
    var inRdd = selfLoopRdd
    // TODO degree的定义，需要再考虑，需不需要减去后面的部分
    //    var totRdd = graph.degrees.join(selfLoopRdd).map(e => (e._1.toLong, e._2._1.toLong - e._2._2))
    var totRdd = graph.degrees.map(e => (e._1.toLong, e._2.toLong))
    val totalDegree = graph.degrees.map(e => e._2).reduce(_+_)
    var curModularity = calcModularity(inRdd, totRdd, totalDegree)
    var newModularity = curModularity
    val vertexArray = graph.vertices.map(e => e._1.toLong).collect()
    val randomArray = generateRandomArray(vertexArray)

    var times = 0

    do{
      movement = 0
      curModularity = newModularity

      for (i <- 0 until randomArray.size) {
        val node = randomArray(i)
        val nodeRdd = changeIntoRdd(node, node, sc).cache()
        val oriComm = (nodeRdd.join(n2cRdd).collect())(0)._2._2

        // update tot
        val commDegreeRdd = nodeRdd.join(n2cRdd)
          .join(graph.degrees)
          .map(e => (e._2._1._2, e._2._2.toLong))

        totRdd = totRdd.leftOuterJoin(commDegreeRdd).map{
          e => (e._1, e._2._1 - e._2._2.getOrElse(0L))
        }
        // update in
        val n2cRddTmp = n2cRdd.cache()
        // 记录每个neighbor所属的community
        val neighCommRdd = generateNeighCommRdd(nodeRdd, graph, n2cRddTmp, sc).cache()
        // 记录这个comm的weight总值
        val commNeighRdd = nodeRdd.join(n2cRdd)
          .map(e => (e._2._2, e._1))
          .join(neighCommRdd)
          .map(e => (e._1, e._2._2))
        val commSelfLoopRdd = nodeRdd.join(selfLoopRdd)
          .join(n2cRdd)
          .map(e => (e._2._2, e._2._1._2))
        inRdd = inRdd.leftOuterJoin(commNeighRdd).map{
          e => (e._1, (e._2._1 - 2 * e._2._2.getOrElse(0L)))
        }.leftOuterJoin(commSelfLoopRdd).map{
          e => (e._1, (e._2._1 - e._2._2.getOrElse(0L)))
        }

        // update n2c
        n2cRdd = n2cRdd.leftOuterJoin(nodeRdd).map{
          e =>
            if (e._2._2.getOrElse(-1L) != -1L)
              (e._1, -1)
            else
              (e._1, e._2._1)
        }

        val totRddTmp = totRdd
        val nodeDegree = (nodeRdd.join(graph.degrees).collect())(0)._2._2.toLong
        val bestComm = modularityGain(totRddTmp, neighCommRdd, oriComm, nodeDegree, totalDegree)

        // update tot
        val totInsertRdd = changeIntoRdd(bestComm, nodeDegree, sc)
        totRdd = totRdd.leftOuterJoin(totInsertRdd).map{
          e => (e._1, e._2._1 + e._2._2.getOrElse(0L))
        }
        // update in
        val selfLoop = nodeRdd.join(selfLoopRdd).map(e => e._2._2).collect()(0)
        val bestCommWeigh = neighCommRdd.filter(e => e._1 == bestComm).collect()(0)._2
        val inChangeValue = 2 * bestCommWeigh + selfLoop
        val inInsertRdd = changeIntoRdd(bestComm, inChangeValue, sc)
        inRdd = inRdd.leftOuterJoin(inInsertRdd).map{
          e => (e._1, e._2._1 + e._2._2.getOrElse(0L))
        }
        // update n2c
        n2cRdd = n2cRdd.leftOuterJoin(nodeRdd).map{
          e =>
            if (e._2._2.getOrElse(-1L) != -1L)
              (e._1, bestComm)
            else
              (e._1, e._2._1)
        }

        if (bestComm != oriComm) {
          movement += 1
        }

        val tmpModularity = calcModularity(inRdd, totRdd, totalDegree)
        println("change in modularity is " + (tmpModularity - curModularity)
                + "\ttmp is " + tmpModularity + "\tcur is " + curModularity)

        nodeRdd.unpersist()
        n2cRddTmp.persist()
        neighCommRdd.unpersist()
      }

      newModularity = calcModularity(inRdd, totRdd, totalDegree)

      if (movement > 0)
        improvement = true

      times += 1

    } while(movement > 0 && (newModularity - curModularity) > minChange && times < maxTimes)

    selfLoopRdd.unpersist()

    reGraphEdges(graph, n2cRdd)
  }

  def reGraphEdges[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED],
                                               n2cRdd: RDD[(Long, Long)]): RDD[(Long, Long)] = {
    val edgeRdd = graph.edges.flatMap{
      case(e) =>
        val arr = ArrayBuffer[(Long, Long)]()
        arr += ((e.srcId.toLong, e.dstId.toLong))
        arr
    }

    val newEdgeRdd = edgeRdd.leftOuterJoin(n2cRdd)
      .map(e => (e._2._1, e._2._2.get))
      .leftOuterJoin(n2cRdd)
      .map(e => (e._2._1, e._2._2.get))

    newEdgeRdd
  }

  def calcModularity(inRdd: RDD[(Long, Long)],
                     totRdd: RDD[(Long, Long)],
                     totalDegree: Double): Double = {
    val q = inRdd.join(totRdd)
      .filter(e => e._2._2 > 0)
      .map{ e =>
      val inValue = e._2._1.toDouble
      val totValue = e._2._2.toDouble
      inValue / totalDegree - Math.pow(totValue / totalDegree, 2)
    }.reduce(_+_)

    return q
  }

  def process[VD: ClassTag, ED: ClassTag](oriGraph: Graph[VD, ED],
                                          sc: SparkContext,
                                          maxTimes: Int = Integer.MAX_VALUE,
                                          minChange: Double = 0.001,
                                          maxIter: Int = Integer.MAX_VALUE) {

    var edgeRdd = reCommunity(oriGraph, sc, maxTimes, minChange)

    var current = 0
    while(improvement && current < maxIter) {
      val graph = Graph.fromEdgeTuples(edgeRdd, 1L).cache()
      edgeRdd = reCommunity(graph, sc, maxTimes, minChange)
      graph.edges.unpersist()
      graph.vertices.unpersist()
      current += 1
    }

    /*
        var current = 0
        var graph = oriGraph.cache()
        do{
          val edgeRdd = reCommunity(graph, sc, maxTimes, minChange)
          graph.vertices.unpersist()
          graph.edges.unpersist()
          graph = Graph.fromEdgeTuples(edgeRdd, 1L)
    //      graph.cache()
          current += 1
        } while(improvement && current < maxTimes)
    */
  }


  def main(args: Array[String]) {
    if (args.size < 3) {
      println("ERROR INPUT!")
      return
    }

    println("FastUnfolding begins...")

    val mode = args(0)  // "local" or yarn-standalone
    val input = args(1) // input file of edge information
    val partitionNum = args(2).toInt  // partition number
    val sc = new SparkContext(mode, "FastUnfolding")

    val graph = initialization(input, partitionNum, sc)
    process(graph, sc, 1, 0.001, 1)

    println("FastUnfolding ends...")
  }
}
