package org.apache.spark.mllib.expectation

import org.jblas.DoubleMatrix

import scala.util._

import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.mllib.clustering.{LDAComputingParams, LDAParams, Document}

class GibbsSampling

object GibbsSampling extends Logging {

  /**
   * A uniform distribution sampler, which is only used for initialization.
   */
  private def uniformDistSampler(rand: Random, dimension: Int): Int = rand.nextInt(dimension)

  /**
   * A multinomial distribution sampler, using roulette method to sample an Int back.
   */
  private[mllib] def multinomialDistSampler(rand: Random, dist: DoubleMatrix): Int = {
    val roulette = rand.nextDouble()

    def loop(index: Int, accum: Double): Int = {
      val sum = accum + dist.get(index)
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
      rand: Random)
    : Int =
  {
    val topicThisTerm = new DoubleMatrix(numTopics, 1)
    val topicThisDoc = new DoubleMatrix(numTopics, 1)
    val fraction = params.topicCounts.add(numTerms * topicTermSmoothing)
    params.topicTermCounts.getColumn(termIdx, topicThisTerm)
    params.docTopicCounts.getRow(docIdx, topicThisDoc)
    topicThisTerm.addi(topicTermSmoothing)
    topicThisDoc.addi(docTopicSmoothing)
    topicThisTerm.divi(fraction)
    topicThisTerm.muli(topicThisDoc)
    topicThisTerm.divi(topicThisTerm.sum)
    multinomialDistSampler(rand, topicThisTerm)
  }

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
      topicTermSmoothing: Double)
    : LDAParams =
  {
    // construct topic assignment RDD
    logInfo("Start initialization")

    val checkPointInterval = System
      .getProperty("spark.gibbsSampling.checkPointInterval", "10").toInt

    val init = data.mapPartitionsWithIndex { case (index, iterator) =>
      val rand = new Random(42 + index)
      val params = LDAComputingParams(numDocs, numTopics, numTerms)
      val assignedTopics = iterator.map { case Document(docId, content) =>
        content.map { term =>
          val topic = uniformDistSampler(rand, numTopics)
          params.inc(docId, term, topic)
          topic
        }
      }.toArray

      Seq((assignedTopics, params)).iterator
    }

    val initialAssignedTopics = init.flatMap(_._1).cache()
    val initialParams = init.map(_._2).reduce(_ addi _)

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

  /**
   * We use LDAParams to infer parameters Phi and Theta.
   */
  def solvePhiAndTheta(
      params: LDAParams,
      numTopics: Int,
      numTerms: Int,
      docTopicSmoothing: Double,
      topicTermSmoothing: Double)
    : (DoubleMatrix, DoubleMatrix) =
  {
    val docCount = params.docCounts.add(docTopicSmoothing * numTopics)
    val topicCount = params.topicCounts.add(topicTermSmoothing * numTerms)
    val docTopicCount = params.docTopicCounts.add(docTopicSmoothing)
    val topicTermCount = params.topicTermCounts.add(topicTermSmoothing)
    (topicTermCount.divColumnVector(topicCount), docTopicCount.divColumnVector(docCount))
  }

  /**
   * Perplexity is a kind of evaluation method of LDA. Usually it is used on unseen data.
   * But here we use it for current documents, which is also OK.
   * If using it on unseen data, you must do an iteration of Gibbs sampling before calling this.
   * Small perplexity means good result.
   */
  def perplexity(data: RDD[Document], phi: DoubleMatrix, theta: DoubleMatrix): Double = {
    val (termProb, totalNum) = data.flatMap { case Document(docId, content) =>
      val currentTheta = theta.getRow(docId).mmul(phi)
      content.map(x => (math.log(currentTheta.get(x)), 1))
    }.reduce { (left, right) =>
      (left._1 + right._1, left._2 + right._2)
    }
    math.exp(-1 * termProb / totalNum)
  }
}
