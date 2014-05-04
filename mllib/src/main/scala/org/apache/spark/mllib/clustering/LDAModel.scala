package org.apache.spark.mllib.clustering

import org.apache.spark.rdd.RDD
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV}

case class TermInDoc(docId: Int, termId: Int, counts: Int)

case class TermsAndCountsPerDoc(termIds: Array[Int], counts: Array[Int])

case class TermsAndTopicAssignsPerDoc(termIds: Array[Int], topicAssigns: Array[BDV[Double]])

class LDAModel(
    val numTopics: Int,
    val docTopicCounts: RDD[(Int, Array[Double])],
    val termTopicCounts: RDD[(Int, Array[Double])]) {

}