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

import breeze.linalg.{DenseVector => BDV}
import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting
import org.apache.spark._
import org.apache.spark.mllib.expectation.GibbsSampling
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.collection.mutable
import org.apache.spark.storage.StorageLevel
import org.apache.spark.broadcast.Broadcast

private[clustering] case class OutLinkBlock(
    elementIds: Array[Int],
    shouldSend: Array[mutable.BitSet])

private[clustering] case class InLinkBlock(
    elementIds: Array[Int],
    termsInBlock: Array[Array[TermsAndCountsPerDoc]])

private[clustering] case class TopicAssign(
    elementIds: Array[Int],
    topicsInBlock: Array[Array[TermsAndTopicAssignsPerDoc]])

class LDA private (
    var numTopics: Int,
    var docTopicSmoothing: Double,
    var topicTermSmoothing: Double,
    var numIteration: Int,
    var numDocs: Int,
    var numTerms: Int,
    var numBlocks: Int,
    val sc: SparkContext,
    var seed: Long = System.nanoTime())
  extends Serializable with Logging {

  def run(documents: RDD[TermInDoc]) {

    val numBlocks = if (this.numBlocks == -1) {
      math.max(sc.defaultParallelism, documents.partitions.size / 2)
    } else {
      this.numBlocks
    }

    val partitioner = new HashPartitioner(numBlocks)

    val docCounts = documents.map { case TermInDoc(docId, termId, counts) =>
      (docId, counts)
    }.groupByKey(partitioner).mapValues(_.sum).collect().toMap

    val bDocCounts = sc.broadcast(docCounts)

    val documentsByUserBlock = documents.map{ doc => (doc.docId % numBlocks, doc) }
    val documentsByProductBlock = documents.map{ doc =>
      (doc.termId % numBlocks, TermInDoc(doc.termId, doc.docId, doc.counts))
    }

    val (docInLinks, docOutLinks) = makeLinkRDDs(numBlocks, documentsByUserBlock)
    val (_, termOutLinks) = makeLinkRDDs(numBlocks, documentsByProductBlock)

    // initialization
    var (topicCounts, docTopics, termTopics, topicAssignment) =
      initialization(numTopics, docOutLinks, docInLinks, termOutLinks)

    // gibbs sampling
    for (i <- 0 until numIteration) {
      (topicCounts, docTopics, termTopics, topicAssignment) =
        updateFeatures(
          bDocCounts,
          topicCounts,
          docTopics,
          termTopics,
          topicAssignment,
          termOutLinks,
          docInLinks,
          partitioner)
    }
  }

  private def initialization(
      numTopics: Int,
      docOutLinks: RDD[(Int, OutLinkBlock)],
      docInLinks: RDD[(Int, InLinkBlock)],
      termOutLinks: RDD[(Int, OutLinkBlock)]):
  (BDV[Int], RDD[(Int, Array[BDV[Int]])], RDD[(Int, Array[BDV[Int]])], RDD[(Int, TopicAssign)]) = {

    // Initialize user and product factors randomly, but use a deterministic seed for each
    // partition so that fault recovery works
    val seedGen = new Random(seed)
    val seed2 = seedGen.nextInt()

    // Hash an integer to propagate random bits at all positions, similar to java.util.HashTable
    def hash(x: Int): Int = {
      val r = x ^ (x >>> 20) ^ (x >>> 12)
      r ^ (r >>> 7) ^ (r >>> 4)
    }

    val topicCounts = BDV.zeros[Int](numTopics)

    val docTopics = docOutLinks.mapPartitionsWithIndex { (index, itr) =>
      itr.map { case (x, y) =>
        (x, y.elementIds.map(_ => BDV.zeros[Int](numTopics)))
      }
    }

    // maybe there is a better way to house topic assignment
    // and there should other ways to setup the topic assignment
    // say, new randomTopicAssign with some incremental parameters.
    val topicAssignment = docInLinks.mapPartitionsWithIndex { (index, itr) =>
      val rand = new Random(hash(seed2 ^ index))
      itr.map { case (x, y) =>
        (x, TopicAssign(y.elementIds, y.termsInBlock.map { _.map {
          case TermsAndCountsPerDoc(termIds, counts) =>
            TermsAndTopicAssignsPerDoc(
              termIds,
              counts.map(dimension => randomTopicAssign(dimension, numTopics, rand)))
        }}))
      }
    }

    val termTopics = termOutLinks.mapPartitionsWithIndex { (index, itr) =>
      itr.map { case (x, y) =>
        (x, y.elementIds.map(_ => BDV.zeros[Int](numTopics)))
      }
    }
    (topicCounts, docTopics, termTopics, topicAssignment)
  }
  /**
   * Make the out-links table for a block of the users (or termTopics) dataset given the list of
   * (user, product, rating) values for the users in that block (or the opposite for termTopics).
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
   * Make the in-links table for a block of the users (or termTopics) dataset given a list of
   * (user, product, rating) values for the users in that block (or the opposite for termTopics).
   */
  private def makeInLinkBlock(numBlocks: Int, documents: Array[TermInDoc]): InLinkBlock = {
    val docIds = documents.map(_.docId).distinct.sorted
    val docIdToPos = docIds.zipWithIndex.toMap

    val blockDocuments = Array.fill(numBlocks)(new ArrayBuffer[TermInDoc])
    for (term <- documents) {
      blockDocuments(term.termId % numBlocks) += term
    }

    val documentsForBlock = new Array[Array[TermsAndCountsPerDoc]](numBlocks)
    for (termBlock <- 0 until numBlocks) {
      val groupedDocuments = blockDocuments(termBlock).groupBy(_.termId).toArray
      val ordering = new Ordering[(Int, ArrayBuffer[TermInDoc])] {
        def compare(a: (Int, ArrayBuffer[TermInDoc]), b: (Int, ArrayBuffer[TermInDoc])): Int = {
          a._1 - b._1
        }
      }
      Sorting.quickSort(groupedDocuments)(ordering)
      documentsForBlock(termBlock) = groupedDocuments.map { case (_, docs) =>
        TermsAndCountsPerDoc(
          docs.view.map(d => docIdToPos(d.docId)).toArray,
          docs.view.map(_.counts).toArray)
      }
    }

    InLinkBlock(docIds, documentsForBlock)
  }

  /**
   * Make RDDs of InLinkBlocks and OutLinkBlocks given an RDD of (blockId, (u, p, r)) values for
   * the users (or (blockId, (p, u, r)) for the termTopics). We create these simultaneously to avoid
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
    }, preservesPartitioning = true)
    val inLinks = links.mapValues(_._1)
    val outLinks = links.mapValues(_._2)
    inLinks.persist(StorageLevel.MEMORY_AND_DISK)
    outLinks.persist(StorageLevel.MEMORY_AND_DISK)
    (inLinks, outLinks)
  }

   /**
   * Compute the user feature vectors given the current termTopics (or vice-versa). This first joins
   * the termTopics with their out-links to generate a set of messages to each destination block
   * (specifically, the features for the termTopics that user block cares about), then groups these
   * by destination and joins them with the in-link info to figure out how to update each user.
   * It returns an RDD of new feature vectors for each user block.
   */
  private def updateFeatures (
      bDocCounts: Broadcast[Map[Int, Int]],
      topicCounts: BDV[Int],
      docTopics: RDD[(Int, Array[BDV[Int]])],
      termTopics: RDD[(Int, Array[BDV[Int]])],
      topicAssignment: RDD[(Int, TopicAssign)],
      termOutLinks: RDD[(Int, OutLinkBlock)],
      docInLinks: RDD[(Int, InLinkBlock)],
      partitioner: Partitioner):
   (BDV[Int], RDD[(Int, Array[BDV[Int]])], RDD[(Int, Array[BDV[Int]])], RDD[(Int, TopicAssign)]) = {
    val numBlocks = termTopics.partitions.size
    val ret = termOutLinks.join(termTopics).flatMap { case (bid, (outLinkBlock, factors)) =>
      val toSend = Array.fill(numBlocks)((new ArrayBuffer[Int], new ArrayBuffer[BDV[Int]]))
      for (t <- 0 until outLinkBlock.elementIds.length; docBlock <- 0 until numBlocks) {
        if (outLinkBlock.shouldSend(t)(docBlock)) {
          toSend(docBlock)._1 += t
          toSend(docBlock)._2 += factors(t)
        }
      }
      toSend.zipWithIndex.map{ case (buf, idx) => (idx, (bid, buf._1.toArray, buf._2.toArray)) }
    }.groupByKey(partitioner)
      .join(docInLinks.join(topicAssignment).join(docTopics))
      .mapValues { case (termTopicMessages, ((inLinkBlock, topicAssign), docTopicMessages)) =>
      updateBlock(
        bDocCounts.getValue(),
        topicCounts,
        docTopicMessages,
        termTopicMessages.toSeq.sortBy(_._1),
        inLinkBlock,
        topicAssign)
    }
    val newTopicCounts =
      ret.map(_._2._1).fold(BDV.zeros[Int](numTopics))(_ + _) - (topicCounts :* (numBlocks - 1))
    val newDocTopics = ret.map(x => (x._1, x._2._2))
    val newTopicAssignment = ret.map(x => (x._1, x._2._4))
    val newTermTopics = ret.map(_._2._3)
      .flatMap(x => x.seq)
      .groupByKey(partitioner)
      .join(termTopics)
      .map { case (block, (itr, mat)) =>
        val tmp = itr.flatMap { case (ids, vectors) =>
          for (i <- 0 until ids.length) yield (ids(i), vectors(i))
        }.groupBy(_._1).map { case (id, vectors) =>
          val numVectors = vectors.size
          (id, (numVectors, vectors.reduce(_._2 + _._2)))
        }
        for ((id, (numVectors, vector)) <- tmp) {
          var i = 0
          while (i < vector.length) {
            mat(id)(i) = vector(i) - (mat(id)(i) * (numVectors - 1))
            i += 1
          }
        }
        (block, mat)
      }
    (newTopicCounts.asInstanceOf[BDV[Int]], newDocTopics, newTermTopics, newTopicAssignment)
  }

  // return value is (docTopics, termTopics, topicAssigns)
  // here is a local gibbs sampling
  private def updateBlock(
      docCounts: Map[Int, Int],
      topicCounts: BDV[Int],
      docTopics: Array[BDV[Int]],
      manyTermTopics: Seq[(Int, Array[Int], Array[BDV[Int]])],
      data: InLinkBlock,
      topicAssign: TopicAssign):
  (BDV[Int], Array[BDV[Int]], Seq[(Int, (Array[Int], Array[BDV[Int]]))], TopicAssign) = {
    val from = manyTermTopics.map(_._1)
    val blockTermIds = manyTermTopics.map(_._2)
    val blockTermTopics = manyTermTopics.map(_._3)
    val numBlocks = blockTermIds.length
    for (block <- 0 until numBlocks) {
      for (term <- 0 until blockTermTopics(block).length) {
        val currentTermTopic = blockTermTopics(block)(term)
        val TermsAndCountsPerDoc(_, currentCounts) = data.termsInBlock(block)(term)
        val TermsAndTopicAssignsPerDoc(_, currentTopicAssigns) =
          topicAssign.topicsInBlock(block)(term)
        // gibbs sampling for this subset of docs and terms
        GibbsSampling.runGibbsSampling(
          docCounts,
          topicCounts,
          docTopics,
          currentTermTopic,
          currentTopicAssigns,
          data.elementIds,
          currentCounts,
          docTopicSmoothing,
          topicTermSmoothing,
          numTerms,
          1)
      }
    }

    val termTopicsRetMessage = from.zip(blockTermIds.zip(blockTermTopics))
    // topic counts, doc topics, term topics, topic assigns.
    (topicCounts, docTopics, termTopicsRetMessage, topicAssign)
  }

  /**
   * Make a random factor vector with the given random.
   */
  private def randomTopicAssign(dimension: Int, numTopics: Int, rand: Random): BDV[Int] = {
    BDV.fill[Int](dimension)(rand.nextInt(numTopics))
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
