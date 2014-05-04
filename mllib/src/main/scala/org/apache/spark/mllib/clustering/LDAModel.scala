package org.apache.spark.mllib.clustering

import org.apache.spark.rdd.RDD
import breeze.linalg.{SparseVector => BSV}

case class TermInDoc(docId: Int, termId: Int, counts: Int)

class LDAModel(
    val numTopics: Int,
    val docTopicCounts: RDD[(Int, Array[Double])],
    val termTopicCounts: RDD[(Int, Array[Double])]) {

}