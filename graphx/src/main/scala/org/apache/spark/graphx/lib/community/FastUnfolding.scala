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
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.conf.Configuration
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random
import scala.Array

object FastUnfolding {

  var oldModularity: Double = 0.0
  var newModularity: Double = 0.0
  var improvement: Boolean = true

  def loadEdgeRdd(edgeFile: String, partitionNum: Int, sc: SparkContext): RDD[(Long, Long)] = {
    val edgeRdd = sc.textFile(edgeFile, partitionNum).flatMap {
      case (line) =>
        val arr = ArrayBuffer[(Long, Long)]()
        val regex = ","
        val ss = line.split(regex)
        if (ss.size > 2) {
          val src = ss(0).toLong
          val dst = ss(1).toLong
          arr += ((src, dst))
        }
        arr
    }
    edgeRdd
  }

  def initialization[VD: ClassTag, ED: ClassTag](file: String, partitionNum: Int, sc: SparkContext):Graph[VD,ED] = {
    val edgeRdd = loadEdgeRdd(input, partitionNum, sc)
    println("get edgeRdd count " + edgeRdd.count())
    val graph = Graph.fromEdgeTuples(edgeRdd, 1L)
    graph
  }

  def generateRandomArray[T:ClassTag](oriArray: Array[T]): Array[T] = {
    val size = oriArray.size
    val result = new Array[T](size)
    for (i <- 0 until size) {
      result(i) = oriArray(i)
    }
    val random = new Random()
    for (i <- 0 until size){
      val randPos = random.nextInt(size)
      val tmp = result(i)
      result(i) = result(randPos)
      result(randPos) = tmp
    }
    result
  }

  def generateSelfLoopRdd[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): RDD[(Long, Long)] = {
    val partSelfLoopRdd = graph.edges.map(e => (e.srcId.toLong, e.dstId.toLong))
      .filter(e => e._1 == e._2)
      .groupBy(e => e._1)
      .map(e => (e._1, e._2.size))
    val result = graph.vertices.leftJoin(partSelfLoopRdd) {
      (vid, vdata, nbrsOpt) => nbrsOpt.getOrElse(0)
    }
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
    }.distinct()
    edgeRdd
  }

  def generateNeighCommRdd[VD: ClassTag, ED: ClassTag](nodeRdd: RDD[(Long, Long)],
                                                       graph: Graph[VD,ED],
                                                       n2cRdd: RDD[(Long, Long)],
                                                       sc: SparkContext): RDD[(Long,Long)] = {
    val edgeRdd = loadMultiEdgeRdd(graph)

    // 邻居所属的comm
    val nodeCommRdd = edgeRdd.leftOuterJoin(nodeRdd)
                              .map(e => (e._2._1, 1))
                              .leftOuterJoin(n2cRdd)
                              .map(e => (e._1, e._2._2.getOrElse(0L)))

    val neighCommRdd = nodeCommRdd.leftOuterJoin(graph.degrees)
                                    .map(e => (e._2._1, e._2._2.getOrElse(0).toLong))
                                    .reduceByKey(_+_)

    neighCommRdd
  }

  // TODO unfinished
  def modularityGain[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                                 nodeRdd: RDD[(Long, Long)],
                                                 n2cRdd: RDD[(Long, Long)],
                                                 totRdd: RDD[(Long, Long)],
                                                 neighCommRdd: RDD[(Long, Long)]): Long = {
    val edgeRdd = loadMultiEdgeRdd(graph)
    val nodeDegree = (graph.degrees.join(nodeRdd).map(e => e._2._1).collect())(0).toDouble

    val neighArray = edgeRdd.join(nodeRdd).map(e => e._2._1).collect()
    for (i <- 0 until neighArray.size) {
      val neigh = neighArray(i)
      neigh
    }
    return 0L
  }

  // TODO unfinished
  def reCommunity[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], sc: SparkContext): Boolean = {
    var movement = 0
    var n2cRdd = graph.degrees.map(e => (e._1.toLong, e._1.toLong))
    var totRdd = graph.degrees.map(e => (e._1.toLong, e._2.toLong))
    val selfLoopRdd = generateSelfLoopRdd(graph).cache()
    var inRdd = selfLoopRdd

    val vertexArray = graph.vertices.map(e => e._1.toLong).collect()
    val randomArray = generateRandomArray(vertexArray)

    for (i <- 1 to randomArray.size) {
      val node = randomArray(i)
      val nodeRdd = changeIntoRdd(node, node, sc).cache()

      // update tot
      val commDegreeRdd = nodeRdd.join(n2cRdd).join(graph.degrees).map(e => (e._2._1._2, e._2._2.toLong))
      totRdd = totRdd.leftOuterJoin(commDegreeRdd).map{
        e => (e._1, e._2._1 - e._2._2.getOrElse(0L))
      }

      // update in
      // if 这里的commNeighRdd是空的，会有问题吗？在第一轮的时候。
      // 在c++实现时，是加了一个 （当前comm，0）的设定
      val n2cRddTmp = n2cRdd.cache()
      // 记录每个neighbor所属的community
      val neighCommRdd = generateNeighCommRdd(nodeRdd, graph, n2cRddTmp, sc).cache()
      // 记录这个comm的weight总值
      val commNeighRdd = nodeRdd.join(n2cRdd)
                                  .map(e => (e._2._2, e._1))
                                  .join(neighCommRdd)
                                  .map(e => (e._1, e._2._2))
      val commSelfLoopRdd = nodeRdd.join(selfLoopRdd).join(n2cRdd).map(e => (e._2._2, e._2._1._2))
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

      val n2cRddTmp2 = n2cRdd
      val totRddTmp = totRdd
      val bestComm = modularityGain(graph, nodeRdd, n2cRddTmp2, totRddTmp, neighCommRdd)

      insert()

      val oriComm = (nodeRdd.join(n2cRddTmp2).map(e => e._2._2).collect())(0)
      if (bestComm != oriComm) {
        movement += 1
      }

      nodeRdd.unpersist()
      n2cRddTmp.persist()
      neighCommRdd.unpersist()
    }

    val newModularity = calcModularity()

    selfLoopRdd.unpersist()
    return false
  }

  def reGraph[VD: ClassTag, ED: ClassTag](): Graph[VD,ED] = {
    return null
  }

  def calcModularity(): Double = {
    return 0.0
  }

  def process[VD: ClassTag, ED: ClassTag](oriGraph: Graph[VD, ED],
                                          sc: SparkContext,
                                          maxIter: Int = Integer.MAX_VALUE) {
    reCommunity(oriGraph, sc)
    var current = 0
    while(improvement && current < maxIter) {
      val graph = reGraph().cache()
      println("new Graph vertex count: " + graph.vertices.count() + "\tedge count: " + graph.edges.count())
      reCommunity(graph, sc)
      println("old modularity: " + oldModularity + "\tnew modularity: " + newModularity)
      current += 1
      graph.vertices.unpersist()
      graph.edges.unpersist()
    }
  }


  def main(args: Array[String]) {
    if (args.size < 4) {
      println("ERROR INPUT!")
      return
    }

    println("FastUnfolding begins...")
    return

    val mode = args(0)  // "local" or yarn-standalone
    val input = args(1) // input file of edge information
    val partitionNum = args(2).toInt  // partition number
    val sc = new SparkContext(mode, "FastUnfolding")

    val fs = FileSystem.get(new Configuration())
    if (fs.exists(new Path(args(3)))) fs.delete(new Path(args(3)), true)  // output file name

    val graph = initialization(input, partitionNum, sc)
    process(graph, sc)

    println("FastUnfolding ends...")
  }
}
