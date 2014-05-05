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

import org.apache.spark.Logging

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
}
