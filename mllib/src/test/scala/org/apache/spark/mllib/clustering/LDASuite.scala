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
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import breeze.stats.distributions.{Multinomial, Poisson, Uniform}
import breeze.linalg.{sum, DenseVector, normalize}
import scala.util._
import org.apache.spark.mllib.expectation.GibbsSampling._

class LDASuite extends FunSuite with BeforeAndAfterAll {
  @transient private var sc: SparkContext = _

  override def beforeAll() {
    sc = new SparkContext("local", "test")
  }

  override def afterAll() {
    sc.stop()
    System.clearProperty("spark.driver.port")
  }

  test("LDA || data clean and text to vector") {
    val file = "./lda.test"

    val (data, wordMap, docMap) = MLUtils.loadCorpus(sc, file, 2)

    assert(wordMap.size == 297)
    assert(docMap.size == 5)
  }

  test("LDA || Gibbs sampling") {

  }


}

object LDASuite {

  val numTopics = 10
  val numTerms = 1000
  val numDocs = 500
  val docLength = 300

  val topicParameterSampler = new Uniform(0, numTerms-1)
  val topicSelectSampler = new Poisson(numTopics/2)
  val topics = (0 until numTopics).map {
    _ => new Multinomial(
      DenseVector(
        Poisson.distribution(topicParameterSampler.draw())
          .sample(numTerms)
          .toArray
          .map(_.toDouble))
    )
  }
  val terms = (0 until numTerms).toArray

  def sampleCorpus(numDocs: Int, numTerms: Int, numTopics: Int): Array[Document] = {
    (0 until numDocs).map { Document(_,
      (0 until docLength).map { _ =>
        val selectedTopic = topicSelectSampler.draw() % numTopics
        terms(topics(selectedTopic).draw())
      }.toArray)
    }.toArray
  }
}
