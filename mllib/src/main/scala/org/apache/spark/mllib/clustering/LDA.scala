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

import java.util.Random

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV}
import scala.collection.mutable.{ArrayBuffer, BitSet}
import scala.util.Sorting

import org.apache.spark._
import org.apache.spark.mllib.expectation.GibbsSampling
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.collection.mutable
import org.apache.spark.storage.StorageLevel


case class LDAParams (
    docCounts: Vector,
    topicCounts: Vector,
    docTopicCounts: Array[Vector],
    topicTermCounts: Array[Vector])
  extends Serializable {

  def update(docId: Int, term: Int, topic: Int, inc: Int) = {
    docCounts.toBreeze(docId) += inc
    topicCounts.toBreeze(topic) += inc
    docTopicCounts(docId).toBreeze(topic) += inc
    topicTermCounts(topic).toBreeze(term) += inc
    this
  }

  def merge(other: LDAParams) = {
    docCounts.toBreeze += other.docCounts.toBreeze
    topicCounts.toBreeze += other.topicCounts.toBreeze

    var i = 0
    while (i < docTopicCounts.length) {
      docTopicCounts(i).toBreeze += other.docTopicCounts(i).toBreeze
      i += 1
    }

    i = 0
    while (i < topicTermCounts.length) {
      topicTermCounts(i).toBreeze += other.topicTermCounts(i).toBreeze
      i += 1
    }
    this
  }

  /**
   * This function used for computing the new distribution after drop one from current document,
   * which is a really essential part of Gibbs sampling for LDA, you can refer to the paper:
   * <I>Parameter estimation for text analysis</I>
   */
  def dropOneDistSampler(
      docTopicSmoothing: Double,
      topicTermSmoothing: Double,
      termId: Int,
      docId: Int,
      rand: Random): Int = {
    val (numTopics, numTerms) = (topicCounts.size, topicTermCounts.head.size)
    val topicThisTerm = BDV.zeros[Double](numTopics)
    var i = 0
    while (i < numTopics) {
      topicThisTerm(i) =
        ((topicTermCounts(i)(termId) + topicTermSmoothing)
          / (topicCounts(i) + (numTerms * topicTermSmoothing))
        ) + (docTopicCounts(docId)(i) + docTopicSmoothing)
      i += 1
    }
    GibbsSampling.multinomialDistSampler(rand, topicThisTerm)
  }
}

object LDAParams {
  implicit val ldaParamsAP = new LDAParamsAccumulableParam

  def apply(numDocs: Int, numTopics: Int, numTerms: Int) = new LDAParams(
    Vectors.fromBreeze(BDV.zeros[Double](numDocs)),
    Vectors.fromBreeze(BDV.zeros[Double](numTopics)),
    Array(0 until numDocs: _*).map(_ => Vectors.fromBreeze(BDV.zeros[Double](numTopics))),
    Array(0 until numTopics: _*).map(_ => Vectors.fromBreeze(BDV.zeros[Double](numTerms))))
}

class LDAParamsAccumulableParam extends AccumulableParam[LDAParams, (Int, Int, Int, Int)] {
  def addAccumulator(r: LDAParams, t: (Int, Int, Int, Int)) = {
    val (docId, term, topic, inc) = t
    r.update(docId, term, topic, inc)
  }

  def addInPlace(r1: LDAParams, r2: LDAParams): LDAParams = r1.merge(r2)

  def zero(initialValue: LDAParams): LDAParams = initialValue
}

private[clustering] case class OutLinkBlock(elementIds: Array[Int], shouldSend: Array[mutable.BitSet])

private[clustering] case class InLinkBlock(elementIds: Array[Int], termsInBlock: Array[Array[(Array[Int], Array[Int])]])

class LDA private (
    var numTopics: Int,
    var docTopicSmoothing: Double,
    var topicTermSmoothing: Double,
    var numIteration: Int,
    var numDocs: Int,
    var numTerms: Int,
    var numBlocks: Int,
    val sc: SparkContext)
  extends Serializable with Logging {

  // topic assign is termId, Array[(docId, termTimes, assign vector)]
  def updateBlock(
      termTopics: Array[(Int, Array[Double])],
      docTopics: Array[(Int, Array[Double])],
      topicAssign: Array[(Int, Array[(Int, Int, BDV[Int])])]):
  Iterator[(
    Array[(Int, Array[Double])],
    Array[(Int, Array[Double])],
    Array[(Int, Array[(Int, Int, BDV[Int])])])] = {
    ???
  }

  def run(documents: RDD[TermInDoc]): LDAModel = {

    val numBlocks = if (this.numBlocks == -1) {
      math.max(sc.defaultParallelism, documents.partitions.size / 2)
    } else {
      this.numBlocks
    }

    val partitioner = new HashPartitioner(numBlocks)

    val ratingsByUserBlock = documents.map{ doc => (doc.docId % numBlocks, doc) }
    val ratingsByProductBlock = documents.map{ doc =>
      (doc.termId % numBlocks, TermInDoc(doc.termId, doc.docId, doc.counts))
    }

    val (userInLinks, userOutLinks) = makeLinkRDDs(numBlocks, ratingsByUserBlock)
    val (productInLinks, productOutLinks) = makeLinkRDDs(numBlocks, ratingsByProductBlock)

    // keep the same partition
    val docTopicCounts = input.map { case (docId, terms) => (docId, BDV.zeros[Double](numTopics)) }

    val termTopicCounts = sc.parallelize((0 until numTerms).map((_, BDV.zeros[Double](numTopics))))

    val a = input.join(docTopicCounts).mapPartitions { iter =>
      iter.flatMap { case (docId, ((terms, topicAssigns), termCounts)) =>
        terms.activeIterator.zipWithIndex.flatMap { case ((termId, times), id) =>
          (0 until times).map { i =>
            (termId, (docId, topicAssigns, id * times + i))
          }
        }
      }
    }

    // partition w.r.t. term frequency here
    // and compute according to terms

    a.groupByKey(new HashPartitioner(1)).join(termTopicCounts).mapPartitions { iter =>

    }
    ???
  }

  /**
   * Make the out-links table for a block of the users (or products) dataset given the list of
   * (user, product, rating) values for the users in that block (or the opposite for products).
   */
  private def makeOutLinkBlock(numBlocks: Int, documents: Array[TermInDoc]): OutLinkBlock = {
    val docIds = documents.map(_.docId).distinct.sorted
    val numDocs = docIds.length
    val docIdToPos = docIds.zipWithIndex.toMap
    val shouldSend = Array.fill(numDocs)(new mutable.BitSet(numBlocks))
    for (term <- documents) {
      shouldSend(docIdToPos(term.docId))(term.termId % numBlocks) = true
    }
    OutLinkBlock(docIds, shouldSend)
  }

  /**
   * Make the in-links table for a block of the users (or products) dataset given a list of
   * (user, product, rating) values for the users in that block (or the opposite for products).
   */
  private def makeInLinkBlock(numBlocks: Int, documents: Array[TermInDoc]): InLinkBlock = {
    val docIds = documents.map(_.docId).distinct.sorted
    val docIdToPos = docIds.zipWithIndex.toMap

    val blockDocuments = Array.fill(numBlocks)(new ArrayBuffer[TermInDoc])
    for (term <- documents) {
      blockDocuments(term.termId % numBlocks) += term
    }

    val documentsForBlock = new Array[Array[(Array[Int], Array[Int])]](numBlocks)
    for (termBlock <- 0 until numBlocks) {
      val groupedDocuments = blockDocuments(termBlock).groupBy(_.termId).toArray
      val ordering = new Ordering[(Int, ArrayBuffer[TermInDoc])] {
        def compare(a: (Int, ArrayBuffer[TermInDoc]), b: (Int, ArrayBuffer[TermInDoc])): Int = {
          a._1 - b._1
        }
      }
      Sorting.quickSort(groupedDocuments)(ordering)
      documentsForBlock(termBlock) = groupedDocuments.map { case (_, docs) =>
        (docs.view.map(d => docIdToPos(d.docId)).toArray, docs.view.map(_.counts).toArray)
      }
    }

    InLinkBlock(docIds, documentsForBlock)
  }

  /**
   * Make RDDs of InLinkBlocks and OutLinkBlocks given an RDD of (blockId, (u, p, r)) values for
   * the users (or (blockId, (p, u, r)) for the products). We create these simultaneously to avoid
   * having to shuffle the (blockId, (u, p, r)) RDD twice, or to cache it.
   */
  private def makeLinkRDDs(numBlocks: Int, ratings: RDD[(Int, TermInDoc)])
    : (RDD[(Int, InLinkBlock)], RDD[(Int, OutLinkBlock)]) =
  {
    val grouped = ratings.partitionBy(new HashPartitioner(numBlocks))
    val links = grouped.mapPartitionsWithIndex((blockId, elements) => {
      val ratings = elements.map{_._2}.toArray
      val inLinkBlock = makeInLinkBlock(numBlocks, ratings)
      val outLinkBlock = makeOutLinkBlock(numBlocks, ratings)
      Iterator.single((blockId, (inLinkBlock, outLinkBlock)))
    }, true)
    val inLinks = links.mapValues(_._1)
    val outLinks = links.mapValues(_._2)
    inLinks.persist(StorageLevel.MEMORY_AND_DISK)
    outLinks.persist(StorageLevel.MEMORY_AND_DISK)
    (inLinks, outLinks)
  }

}

/*
object LDA extends Logging {

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
    val (trainer, model) = lda.run(data)
    trainer.solvePhiAndTheta(model)
  }

  def main(args: Array[String]) {
    if (args.length != 5) {
      println("Usage: LDA <master> <input_dir> <k> <max_iterations> <mini-split>")
      System.exit(1)
    }

    val (master, inputDir, k, iters, minSplit) =
      (args(0), args(1), args(2).toInt, args(3).toInt, args(4).toInt)
    val checkPointDir = System.getProperty("spark.gibbsSampling.checkPointDir", "/tmp/lda-cp")
    val sc = new SparkContext(master, "LDA")
    sc.setCheckpointDir(checkPointDir)
    val (data, wordMap, docMap) = MLUtils.loadCorpus(sc, inputDir, minSplit)
    val numDocs = docMap.size
    val numTerms = wordMap.size

    val (phi, theta) = LDA.train(data, k, 0.01, 0.01, iters, numDocs, numTerms)
    val pp = GibbsSampling.perplexity(data, phi, theta)
    println(s"final mode perplexity is $pp")
  }
}
*/
