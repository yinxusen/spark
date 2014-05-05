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

import java.util.Random

import breeze.linalg.{DenseVector => BDV, sum}

import org.apache.spark.mllib.clustering.LocalLDAModel

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.TermInDoc
import org.apache.spark.mllib.linalg.{Vector, Vectors}

object GibbsSampling extends Logging {

   /**
   * Main function of running a Gibbs sampling method. It contains two phases of total Gibbs
   * sampling: first is initialization, second is real sampling.
   */
  def runGibbsSampling(
      docCounts: Map[Int, Int],
      topicCounts: BDV[Int],
      docTopics: Array[BDV[Int]],
      termTopic: BDV[Int],
      topicAssigns: Array[BDV[Int]],
      elementIds: Array[Int],
      counts: Array[Int],
      docTopicSmoothing: Double,
      topicTermSmoothing: Double,
      numTerms: Int,
      numIterations: Int): Unit = {
    val numDocs = counts.length
    var i = 0
    var doc = 0
    var term = 0
    while (i < numIterations) {
      doc = 0
      while (doc < numDocs) {
        val currentDocCount = counts(doc)
        term = 0
        while (term < currentDocCount) {
          val lastTopic = topicAssigns(doc)(term)
          docTopics(doc)(lastTopic) -= 1
          termTopic(lastTopic) -= 1
          topicCounts(lastTopic) -= 1
          val chosenTopic = dropOneDistSampler(
            docTopics(doc),
            termTopic,
            docCounts(elementIds(doc)),
            topicCounts(lastTopic),
            topicTermSmoothing,
            docTopicSmoothing,
            numTerms,
            new Random())
          docTopics(doc)(chosenTopic) += 1
          termTopic(chosenTopic) += 1
          topicCounts(chosenTopic) += 1
          topicAssigns(doc)(term) = chosenTopic
          term += 1
        }
        doc += 1
      }
      i += 1
    }
  }

  /**
   * This function used for computing the new distribution after drop one from current document,
   * which is a really essential part of Gibbs sampling for LDA, you can refer to the paper:
   * <I>Parameter estimation for text analysis</I>
   */
  def dropOneDistSampler(
      docTopic: BDV[Int],
      termTopic: BDV[Int],
      docCount: Int,
      topicCount: Int,
      topicTermSmoothing: Double,
      docTopicSmoothing: Double,
      numTerms: Int,
      rand: Random): Int = {
    val numTopics = docTopic.size
    val topicThisTerm = BDV.zeros[Double](numTopics)
    var i = 0
    while (i < numTopics) {
      topicThisTerm(i) = ((termTopic(i) + topicTermSmoothing) / (topicCount + (numTerms * topicTermSmoothing))) *
          ((docTopic(i) + docTopicSmoothing) / (docCount + (numTopics * docTopicSmoothing) - 1))
      i += 1
    }
    GibbsSampling.multinomialDistSampler(rand, topicThisTerm)
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

  /**
   * Initial step of Gibbs sampling, which supports incremental LDA.
   */
  private def sampleTermAssignment(markovChain: Array[Array[Int]], params: LocalLDAModel): (LocalLDAModel, Array[Array[Int]]) = {
    val newParams = params.clone()
    val rand = new Random(42)
    val initialChosenTopics = markovChain.zipWithIndex.map { case (content, docId) =>
      val docTopics = params.docTopicCounts(docId)
      if (docTopics.norm(2) == 0) {
        content.map { term =>
          val topic = uniformDistSampler(rand, params.topicCounts.size)
          newParams.update(docId, term, topic, 1)
          topic
        }
      } else {
        content.map { term =>
          val topicTerms = new BDV[Double](params.topicTermCounts.map(_(term)))
          val dist = docTopics :* topicTerms
          multinomialDistSampler(rand, dist)
        }
      }
    }

    (newParams, initialChosenTopics)
  }

  /**
   * A uniform distribution sampler, which is only used for initialization.
   */
  private def uniformDistSampler(rand: Random, dimension: Int): Int = rand.nextInt(dimension)

  /**
   * A multinomial distribution sampler, using roulette method to sample an Int back.
   */
  def multinomialDistSampler(rand: Random, dist: BDV[Double]): Int = {
    val roulette = rand.nextDouble()

    dist :/= sum[BDV[Double], Double](dist)

    def loop(index: Int, accum: Double): Int = {
      if(index == dist.length) return dist.length - 1
      val sum = accum + dist(index)
      if (sum >= roulette) index else loop(index + 1, sum)
    }

    loop(0, 0.0)
  }

  /**
   * Perplexity is a kind of evaluation method of LDA. Usually it is used on unseen data. But here
   * we use it for current documents, which is also OK. If using it on unseen data, you must do an
   * iteration of Gibbs sampling before calling this. Small perplexity means good result.
   */
  def perplexity(data: RDD[TermInDoc], phi: Array[Vector], theta: Array[Vector]): Double = {
    val (termProb, totalNum) = data.flatMap { case TermInDoc(docId, content) =>
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
