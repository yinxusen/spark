package org.apache.spark.mllib.clustering

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.expectation.GibbsSampling
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import breeze.linalg.{DenseVector => BDV}

trait LDAParams {
  def docCounts: Vector
  def topicCounts: Vector
  def docTopicCounts: Array[Vector]
  def topicTermCounts: Array[Vector]

  def inc(arg0: Int, arg1: Int, arg2: Int)
  def dec(arg0: Int, arg1: Int, arg2: Int)
}

case class LDAComputingParams(
    currDocCounts: BDV[Double],
    currTopicCounts: BDV[Double],
    currDocTopicCounts: Array[BDV[Double]],
    currTopicTermCounts: Array[BDV[Double]])
  extends Serializable with LDAParams {

  override def docCounts: Vector = Vectors.fromBreeze(currDocCounts)

  override def topicCounts: Vector = Vectors.fromBreeze(currTopicCounts)

  override def docTopicCounts: Array[Vector] = currDocTopicCounts.map(Vectors.fromBreeze(_))

  override def topicTermCounts: Array[Vector] = currTopicTermCounts.map(Vectors.fromBreeze(_))

  private def update(doc: Int, term: Int, topic: Int, value: Double) {
    currDocCounts(doc) += value
    currTopicCounts(topic) += value
    currDocTopicCounts(doc)(topic) += value
    currTopicTermCounts(topic)(term) += value
  }

  override def inc(doc: Int, term: Int, topic: Int) {
    update(doc, term, topic, +1)
  }

  override def dec(doc: Int, term: Int, topic: Int) {
    update(doc, term, topic, -1)
  }

  def addi(other: LDAComputingParams): LDAComputingParams = {
    if (currTopicTermCounts.length != 10) {
      println("error here")
    }
    currDocCounts :+= other.currDocCounts
    currTopicCounts :+= other.currTopicCounts
    var i = 0
    while (i < currDocTopicCounts.length) {
      currDocTopicCounts(i) :+= other.currDocTopicCounts(i)
      i += 1
    }

    i = 0
    while (i < currTopicTermCounts.length) {
      currTopicTermCounts(i) :+= other.currTopicTermCounts(i)
      i += 1
    }

    this
  }
}

object LDAComputingParams {
  def apply(numDocs: Int, numTopics: Int, numTerms: Int) = new LDAComputingParams(
    BDV.zeros[Double](numDocs),
    BDV.zeros[Double](numTopics),
    (0 until numDocs).map(_ => BDV.zeros[Double](numTopics)).toArray,
    (0 until numTopics).map(_ => BDV.zeros[Double](numTerms)).toArray
  )
}

class LDA private (
    var numTopics: Int,
    var docTopicSmoothing: Double,
    var topicTermSmoothing: Double,
    var numIteration: Int,
    var numDocs: Int,
    var numTerms: Int)
  extends Serializable with Logging
{
  def run(input: RDD[Document]): LDAParams = {
    new GibbsSampling(LDAComputingParams(numDocs, numTopics, numTerms)).runGibbsSampling(
      input,
      numIteration,
      1,
      numTerms,
      numDocs,
      numTopics,
      docTopicSmoothing,
      topicTermSmoothing)
  }
}

object LDA {

  def train(
      data: RDD[Document],
      numTopics: Int,
      docTopicSmoothing: Double,
      topicTermSmoothing: Double,
      numIterations: Int,
      numDocs: Int,
      numTerms: Int): (Array[Vector], Array[Vector]) = {
    val lda = new LDA(numTopics,
      docTopicSmoothing,
      topicTermSmoothing,
      numIterations,
      numDocs,
      numTerms)
    val model = lda.run(data)
    GibbsSampling.
      solvePhiAndTheta(model, numTopics, numTerms, docTopicSmoothing, topicTermSmoothing)
  }

  def main(args: Array[String]) {
    if (args.length != 5) {
      println("Usage: LDA <master> <input_dir> <k> <max_iterations> <mini-split>")
      System.exit(1)
    }

    val (master, inputDir, k, iters, minSplit) =
      (args(0), args(1), args(2).toInt, args(3).toInt, args(4).toInt)
    val checkPointDir = System.getProperty("spark.gibbsSampling.checkPointDir", "/tmp/lda")
    val sc = new SparkContext(master, "LDA")
    sc.setCheckpointDir(checkPointDir)
    val (data, wordMap, docMap) = MLUtils.loadCorpus(sc, inputDir, minSplit)
    val numDocs = docMap.size
    val numTerms = wordMap.size
    val (phi, theta) = LDA.train(data, k, 0.01, 0.01, iters, numDocs, numTerms)
    val pp = GibbsSampling.perplexity(data, phi, theta)
    // println(s"final model Phi is $phi")
    // println(s"final model Theta is $theta")
    println(s"final mode perplexity is $pp")
  }
}
