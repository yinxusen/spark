package org.apache.spark.mllib.util

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkContext, Logging}


/**
 * 查找互相关注的关系。
 *
 * 输入为一个文本文件，每行的格式为 userId1, userId2, userId3,..., userIdN，表示 userId1 关注了 userId2, userId3, ..., userIdN
 * 输出为一个文本文件，每行格式为 userId1,userId2，表示这两个用户互相关注了。
 *
 * 算法：把每一行变成一个 ((userIdA,userIdB), 1L)的KV序列，userId小的放在前边，key 是 (userIdA,userIdB)。然后统计个数，
 * 如果key对应的值大于或等于2,说明是是互相关注。
 *
 *
 */
object Mutual extends Logging {

  val SEPERATOR="\t"

  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println("Usage: Mutual <master> <input> <output>")
      System.exit(-1)
    }

    val start = System.currentTimeMillis()
    val sc = new SparkContext(args(0), "Mutual", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))

    val lines = sc.textFile(args(1))

    val pairs = lines.flatMap { line =>
      val (Array(user), followings) = line.split(SEPERATOR).map(_.toLong).splitAt(1)
      followings.map ((following: Long) => (user.min(following), user.max(following)) -> 1)
    }

    // 出现两次以上，说明是双向关注
    val result = pairs.reduceByKey(_ + _).filter(_._2 > 1)
    result.keys.map(_.productIterator.mkString(",")).saveAsTextFile(args(2))

    val end = System.currentTimeMillis()
    logInfo("Time elapsed: " + (end-start) + "ms")
    sc.stop()
    System.exit(0)
  }
}
