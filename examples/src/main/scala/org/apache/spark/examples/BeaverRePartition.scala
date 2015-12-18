package org.apache.spark.examples

import org.apache.spark.{SparkContext, SparkConf}

object BeaverRePartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BeaverRePartition").setMaster("local[1]").set("com.beaver.ShuffleOutputPartitions", "500")
    val sc = new SparkContext(conf)

    val twentyKArray = (0 until 20000).toArray.map(x => (x % 1000, x))
    sc.parallelize(twentyKArray, 2).groupByKey().saveAsTextFile("/Users/panda/data/tmp/test-1")
    sc.stop()
  }
}
