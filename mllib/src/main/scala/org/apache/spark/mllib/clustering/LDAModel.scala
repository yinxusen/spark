package org.apache.spark.mllib.clustering

import breeze.linalg.{DenseVector => BDV}

case class TermInDoc(docId: Int, termId: Int, counts: Int)

case class TermsAndCountsPerDoc(termIds: Array[Int], counts: Array[Int])

case class TermsAndTopicAssignsPerDoc(termIds: Array[Int], topicAssigns: Array[BDV[Int]])
