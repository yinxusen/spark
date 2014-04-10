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

package org.apache.spark.mllib.clustering


import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import breeze.stats.distributions.Poisson
import breeze.linalg.{DenseVector => BDV}
import scala.util._
import org.apache.spark.mllib.expectation.GibbsSampling._
import org.apache.spark.mllib.expectation.GibbsSampling

class LDASuite extends FunSuite with BeforeAndAfterAll {
  import LDASuite._
  @transient private var sc: SparkContext = _

  override def beforeAll() {
    sc = new SparkContext("local", "LDA test")
  }

  override def afterAll() {
    sc.stop()
    System.clearProperty("spark.driver.port")
  }

  test("LDA || Gibbs sampling") {
    val checkPointDir = System.getProperty("spark.gibbsSampling.checkPointDir", "/tmp/lda")
    sc.setCheckpointDir(checkPointDir)
    val model = generateRandomLDAModel(numTopics, numTerms)
    val corpus = sampleCorpus(model, numDocs, numTerms, numTopics)
    val data = sc.parallelize(corpus, 2)
    val computedModel = GibbsSampling.runGibbsSampling(data, numOuterIterations, numInnerIterations, numTerms, numDocs, numTopics, docTopicSmoothing, topicTermSmoothing)
    val (phi, theta) = GibbsSampling.solvePhiAndTheta(computedModel, numTopics, numTerms, docTopicSmoothing, topicTermSmoothing)
  }
}

object LDASuite {

  val numTopics = 10
  val numTerms = 1000
  val numDocs = 500
  val expectedDocLength = 300
  val docTopicSmoothing = 0.01
  val topicTermSmoothing = 0.01
  val numOuterIterations = 2
  val numInnerIterations = 1

  /**
   * Generate a random LDA model, i.e. the topic-term matrix.
   */
  def generateRandomLDAModel(numTopics: Int, numTerms: Int): Array[BDV[Double]] = {
    val model = new Array[BDV[Double]](numTopics)
    val width = numTerms * 1.0 / numTopics
    var topic = 0
    var i = 0
    while (topic < numTopics) {
      val topicCentroid = width * (topic + 1)
      model(topic) = BDV.zeros[Double](numTerms)
      i = 0
      while (i < numTerms) {
        // treat the term list as a circle, so the distance between the first one and the last one is 1, not n-1.
        val distance = Math.abs(topicCentroid - i) % (numTerms / 2)
        // Possibility is decay along with distance
        model(topic)(i) = 1.0 / (1 + Math.abs(distance))
        i += 1
      }
      topic += 1
    }
    model
  }

  /**
   * Sample one document given the topic-term matrix
   */
  def ldaSampler(model: Array[BDV[Double]], topicDist: BDV[Double], numTermsPerDoc: Int): Array[Int] = {
    val samples = new Array[Int](numTermsPerDoc)
    val rand = new Random()
    (0 until numTermsPerDoc).foreach { i =>
      samples(i) = multinomialDistSampler(
        rand,
        model(multinomialDistSampler(rand, topicDist))
      )
    }
    samples
  }

  /**
   * Sample corpus (many documents) from a given topic-term matrix
   */
  def sampleCorpus(model: Array[BDV[Double]], numDocs: Int, numTerms: Int, numTopics: Int): Array[Document] = {
    (0 until numDocs).map { i =>
      val numTermsPerDoc = Poisson.distribution(expectedDocLength).sample()
      val numTopicsPerDoc = Random.nextInt(numTopics / 2) + 1
      val topicDist = BDV.zeros[Double](numTopics)
      (0 until numTopicsPerDoc).foreach { _ =>
        topicDist(Random.nextInt(numTopics)) += 1
      }
      Document(i, ldaSampler(model, topicDist, numTermsPerDoc))
    }.toArray
  }
}
