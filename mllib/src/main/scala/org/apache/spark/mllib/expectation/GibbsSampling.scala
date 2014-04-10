package org.apache.spark.mllib.expectation

import scala.util._

import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.mllib.clustering.{LDAComputingParams, LDAParams, Document}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import breeze.linalg.{Vector => BV, DenseVector => BDV, norm, sum}

class GibbsSampling(params: LDAParams) extends Logging with Serializable {
  import GibbsSampling._

   /**
   * Main function of running a Gibbs sampling method.
   * It contains two phases of total Gibbs sampling:
   * first is initialization, second is real sampling.
   */
  def runGibbsSampling(
      data: RDD[Document],
      numOuterIterations: Int,
      numInnerIterations: Int,
      numTerms: Int,
      numDocs: Int,
      numTopics: Int,
      docTopicSmoothing: Double,
      topicTermSmoothing: Double,
      initParams: LDAParams = params)
    : LDAParams =
  {
    // construct topic assignment RDD
    logInfo("Start initialization")

    val checkPointInterval = System
      .getProperty("spark.gibbsSampling.checkPointInterval", "10").toInt

    // Preprocessing
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

        val assignedTopics = assignedTopicsAndParams.flatMap(_._1).cache()
        if (salt % checkPointInterval == 0) {
          assignedTopics.checkpoint()
        }
        val paramsRdd = assignedTopicsAndParams.map(_._2)
        val params = paramsRdd.zip(assignedTopics).map(_._1).reduce(_ addi _)
        lastAssignedTopics.unpersist()

        (params, assignedTopics, salt + 1)
    }.drop(1 + numOuterIterations).next()

    param
  }
}

object GibbsSampling extends Logging {

  private def sampleTermAssignment(params: LDAParams, data: RDD[Document]): (LDAParams, RDD[Array[Int]]) = {
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
            multinomialDistSampler(rand, docTopics :* topicTerms)
          }
        }
      }.toArray

      Seq((assignedTopics, params)).iterator
    }

    val initialAssignedTopics = init.flatMap(_._1).cache()
    val temp = init.map(_._2).collect()
    val initialParams = temp.reduce(_.asInstanceOf[LDAComputingParams] addi _.asInstanceOf[LDAComputingParams])
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

    // assert(sum[BV[Double], Double](dist) == 1.0)

    def loop(index: Int, accum: Double): Int = {
      val sum = accum + dist(index)
      if (sum >= roulette) index else loop(index + 1, sum)
    }

    loop(0, 0.0)
  }

  /**
   * I use this function to compute the new distribution after drop one from current document.
   * This is a really essential part of Gibbs sampling for LDA, you can refer to the paper:
   * <I>Parameter estimation for text analysis</I>
   * In the end, I call multinomialDistSampler to sample a figure out.
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
   * We use LDAParams to infer parameters Phi and Theta.
   */
  def solvePhiAndTheta(
      params: LDAParams,
      numTopics: Int,
      numTerms: Int,
      docTopicSmoothing: Double,
      topicTermSmoothing: Double)
    : (Array[Vector], Array[Vector]) =
  {
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
   * Perplexity is a kind of evaluation method of LDA. Usually it is used on unseen data.
   * But here we use it for current documents, which is also OK.
   * If using it on unseen data, you must do an iteration of Gibbs sampling before calling this.
   * Small perplexity means good result.
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
