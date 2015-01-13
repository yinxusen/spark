package org.apache.spark.newml.util

import java.io.PrintWriter
import scala.util.Random

import org.apache.spark.newml.linalg._

object LassoGenerator {
  val randomFeatures: (Int, Int) => Array[Double] = (n, seed) => {
    val rnd = new Random(seed)
    val rnd2 = new Random(seed)
    var init = rnd.nextDouble()
    Array(0 until n: _*).map { i =>
      if (rnd2.nextDouble() < 0.9) {
        init = rnd.nextDouble()
      } else {
        init = init * 0.9 + rnd.nextDouble() * 0.1
      }
      init
    }
  }

  val randomWeights: (Int, Int) => Array[Double] = (n, seed) => {
    val rnd = new Random(seed)
    Array(0 until n: _*).map { i =>
      rnd.nextDouble()
    }
  }

  def get(fName: String, numSamples: Int, numFeatures: Int, seed: Int = 47) {
    val out = new PrintWriter(fName)
    val weights = Vectors.dense(randomWeights(numFeatures, seed))
    (0 until numSamples).map { case i =>
      val features = Vectors.dense(randomFeatures(numFeatures, seed))
      val y = weights.toBreeze.dot(features.toBreeze)
      out.println("%s,%s".format(y, features))
    }
    out.close()
  }

  def main(args: Array[String]) {
    val numFeatures = Array(10 * 1000000, 50 * 1000000, 100 * 1000000)
    val numSamples = 50 * 1000
    numFeatures.map { case i =>
      val file = s"/home/sen/data/lasso-$numSamples-$i"
      get(file, numSamples, i)
    }
  }
}
