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

package org.apache.spark.mllib.expectation

import scala.util._

import breeze.linalg.{Vector => BV, DenseVector => BDV, sum}

import org.apache.spark.Logging
import org.apache.spark.mllib.clustering.{LDAComputingParams, LDAParams, Document}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

/**
 * Gibbs sampling from a given dataset and model.
 * @param data Dataset, such as corpus.
 * @param numOuterIterations Number of outer iteration.
 * @param numInnerIterations Number of inner iteration, used in each partition.
 * @param docTopicSmoothing Document-topic smoohing.
 * @param topicTermSmoothing Topic-term smoothing.
 */
class GibbsSampling(
    data: RDD[Document],
    numOuterIterations: Int,
    numInnerIterations: Int,
    docTopicSmoothing: Double,
    topicTermSmoothing: Double)
  extends Logging with Serializable {
  import GibbsSampling._

   /**
   * Main function of running a Gibbs sampling method. It contains two phases of total Gibbs
   * sampling: first is initialization, second is real sampling.
   */
  def runGibbsSampling(
      initParams: LDAComputingParams,
      data: RDD[Document] = data,
      numOuterIterations: Int = numOuterIterations,
      numInnerIterations: Int = numInnerIterations,
      docTopicSmoothing: Double = docTopicSmoothing,
      topicTermSmoothing: Double = topicTermSmoothing): LDAComputingParams = {

    val numTerms = initParams.topicTermCounts.head.size
    val numDocs = initParams.docCounts.size
    val numTopics = initParams.topicCounts.size

    logInfo(s"Degree of parallelism is ${data.partitions.length}.")

    val cpInterval = System.getProperty("spark.gibbsSampling.checkPointInterval", "10").toInt

    // construct topic assignment RDD
    logInfo("Start initialization")
    val (initialParams, initialAssignedTopics) = sampleTermAssignment(initParams, data)

    // Gibbs sampling
    val (param, _, _) = Iterator.iterate((initialParams, initialAssignedTopics, 0)) {
      case (lastParams, lastAssignedTopics, salt) =>
        logInfo("Start Gibbs sampling")

        val assignedTopicsAndParams = data.zip(lastAssignedTopics).mapPartitions { iterator =>
          val params = LDAComputingParams(numDocs, numTopics, numTerms)
          val rand = new Random(42 + salt * numOuterIterations)
          val assignedTopics = iterator.map { case (Document(docId, content), topics) =>
            content.zip(topics).map { case (term, topic) =>
              lastParams.dec(docId, term, topic)

              val assignedTopic = dropOneDistSampler(
                lastParams, docTopicSmoothing,
                topicTermSmoothing, numTopics, numTerms, term, docId, rand)

              lastParams.inc(docId, term, assignedTopic)
              params.inc(docId, term, assignedTopic)
              assignedTopic
            }
          }.toArray

          Seq((assignedTopics, params)).iterator
        }

        logInfo("Aggregate topic assignment and parameters")

        val assignedTopics = assignedTopicsAndParams.flatMap(_._1).cache()
        if (salt % cpInterval == 0) {
          assignedTopics.checkpoint()
        }
        val paramsRdd = assignedTopicsAndParams.map(_._2)
        val params = paramsRdd.reduce(_ addi _)
        lastAssignedTopics.unpersist()

        (params, assignedTopics, salt + 1)
    }.drop(1 + numOuterIterations).next()

    param
  }

  /**
   * Model matrix Phi and Theta are inferred via LDAParams.
   */
  def solvePhiAndTheta(
      params: LDAParams,
      docTopicSmoothing: Double = docTopicSmoothing,
      topicTermSmoothing: Double = topicTermSmoothing): (Array[Vector], Array[Vector]) = {
    val numTopics = params.topicCounts.size
    val numTerms = params.topicTermCounts.head.size

    val docCount = params.docCounts.toBreeze :+ (docTopicSmoothing * numTopics)
    val topicCount = params.topicCounts.toBreeze :+ (topicTermSmoothing * numTerms)
    val docTopicCount = params.docTopicCounts.map(vec => vec.toBreeze :+ docTopicSmoothing)
    val topicTermCount = params.topicTermCounts.map(vec => vec.toBreeze :+ topicTermSmoothing)

    var i = 0
    while (i < numTopics) {
      topicTermCount(i) :/= topicCount(i)
      i += 1
    }
    i = 0
    while (i < docCount.length) {
      docTopicCount(i) :/= docCount(i)
      i += 1
    }
    (topicTermCount.map(vec => Vectors.fromBreeze(vec)),
      docTopicCount.map(vec => Vectors.fromBreeze(vec)))
  }
}

object GibbsSampling extends Logging {

  /**
   * Initial step of Gibbs sampling, which supports incremental LDA.
   */
  private def sampleTermAssignment(
      params: LDAComputingParams,
      data: RDD[Document]): (LDAComputingParams, RDD[Iterable[Int]]) = {
    val init = data.mapPartitionsWithIndex { case (index, iterator) =>
      val rand = new Random(42 + index)
      val docTopics = params.docTopicCounts(index).toBreeze
      val assignedTopics = iterator.map { case Document(docId, content) =>
        content.map { term =>
          val topicTerms = Vectors.dense(params.topicTermCounts.map(vec => vec(term))).toBreeze
          val dist = docTopics :* topicTerms
          if (dist.norm(2) == 0) {
            val topic = uniformDistSampler(rand, dist.size)
            params.inc(docId, term, topic)
            topic
          } else {
            multinomialDistSampler(rand, dist)
          }
        }
      }.toArray

      Seq((assignedTopics, params)).iterator
    }

    val initialAssignedTopics = init.flatMap(_._1).cache()
    val temp = init.map(_._2).collect()
    val initialParams = temp.reduce(_ addi _)
    (initialParams, initialAssignedTopics)
  }

  /**
   * A uniform distribution sampler, which is only used for initialization.
   */
  private def uniformDistSampler(rand: Random, dimension: Int): Int = rand.nextInt(dimension)

  /**
   * A multinomial distribution sampler, using roulette method to sample an Int back.
   */
  private[mllib] def multinomialDistSampler(rand: Random, dist: BV[Double]): Int = {
    val roulette = rand.nextDouble()

    assert(sum[BV[Double], Double](dist) != 0.0)
    dist :/= sum[BV[Double], Double](dist)

    def loop(index: Int, accum: Double): Int = {
      if(index == dist.length) return dist.length - 1
      val sum = accum + dist(index)
      if (sum >= roulette) index else loop(index + 1, sum)
    }

    loop(0, 0.0)
  }

  /**
   * This function used for computing the new distribution after drop one from current document,
   * which is a really essential part of Gibbs sampling for LDA, you can refer to the paper:
   * <I>Parameter estimation for text analysis</I>
   */
  private def dropOneDistSampler(
      params: LDAParams,
      docTopicSmoothing: Double,
      topicTermSmoothing: Double,
      numTopics: Int,
      numTerms: Int,
      termIdx: Int,
      docIdx: Int,
      rand: Random): Int = {
    val topicThisTerm = BDV.zeros[Double](numTopics)
    val topicThisDoc = BDV.zeros[Double](numTopics)
    val fraction = params.topicCounts.toBreeze :+ (numTerms * topicTermSmoothing)
    topicThisTerm := Vectors.dense(params.topicTermCounts.map(vec => vec(termIdx))).toBreeze
    topicThisDoc := params.docTopicCounts(docIdx).toBreeze
    topicThisTerm :+= topicTermSmoothing
    topicThisDoc :+= docTopicSmoothing
    topicThisTerm :/= fraction
    topicThisTerm :+= topicThisDoc
    topicThisTerm :/= sum[BDV[Double], Double](topicThisTerm)
    multinomialDistSampler(rand, topicThisTerm)
  }

  /**
   * Perplexity is a kind of evaluation method of LDA. Usually it is used on unseen data. But here
   * we use it for current documents, which is also OK. If using it on unseen data, you must do an
   * iteration of Gibbs sampling before calling this. Small perplexity means good result.
   */
  def perplexity(data: RDD[Document], phi: Array[Vector], theta: Array[Vector]): Double = {
    val (termProb, totalNum) = data.flatMap { case Document(docId, content) =>
      val currentTheta = BDV.zeros[Double](phi.head.size)
      var col = 0
      var row = 0
      while (col < phi.head.size) {
        row = 0
        while (row < phi.length) {
          currentTheta(col) += phi(row)(col) * theta(docId)(row)
          row += 1
        }
        col += 1
      }
      content.map(x => (math.log(currentTheta(x)), 1))
    }.reduce { (lhs, rhs) =>
      (lhs._1 + rhs._1, lhs._2 + rhs._2)
    }
    math.exp(-1 * termProb / totalNum)
  }
}
