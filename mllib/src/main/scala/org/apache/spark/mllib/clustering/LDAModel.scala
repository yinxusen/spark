package org.apache.spark.mllib.clustering

import org.apache.spark.rdd.RDD
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV}
import java.util.Random
import org.apache.spark.mllib.expectation.GibbsSampling

case class TermInDoc(docId: Int, termId: Int, counts: Int)

case class TermsAndCountsPerDoc(termIds: Array[Int], counts: Array[Int])

case class TermsAndTopicAssignsPerDoc(termIds: Array[Int], topicAssigns: Array[BDV[Int]])

class LocalLDAModel private (
    var numTopics: Int,
    var docCounts: BDV[Double],
    var topicCounts: BDV[Double],
    var docTopicCounts: Array[BDV[Double]],
    var topicTermCounts: Array[BDV[Double]],
    var docTopicSmoothing: Double,
    var topicTermSmoothing: Double) {

  def this(nDoc: Int, nTerm: Int, nt: Int, dts: Double, tts: Double) = {
    this(nt, BDV.zeros[Double](nDoc), BDV.zeros[Double](nt),
    Array.range(0, nDoc).map(_ => BDV.zeros[Double](nt)),
    Array.range(0, nt).map(_ => BDV.zeros[Double](nTerm)),
    dts, tts)
  }

  override def clone(): LocalLDAModel = {
    new LocalLDAModel(numTopics, BDV.zeros[Double](docCounts.length), BDV.zeros[Double](topicCounts.length),
    Array.range(0, docTopicCounts.length).map(_ => BDV.zeros[Double](numTopics)),
    Array.range(0, topicTermCounts.length).map(_ => BDV.zeros[Double](topicTermCounts.head.length)),
    docTopicSmoothing, topicTermSmoothing)
  }

  def update(docId: Int, term: Int, topic: Int, inc: Int) = {
    docCounts(docId) += inc
    topicCounts(topic) += inc
    docTopicCounts(docId)(topic) += inc
    topicTermCounts(topic)(term) += inc
    this
  }
}