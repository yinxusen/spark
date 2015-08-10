package org.apache.spark.ml.tuning.bandit

/**
 * Created by panda on 8/10/15.
 */
abstract class RangeSlicer[T](val name: String, val paramType: String) {
  def getSamples(numVal: Int): Array[T]
}

class IntegerRangeSlicer(
    override val name: String,
    val minVal: Int,
    val maxVal: Int,
    val initVal: Int = None) extends RangeSlicer[Int](name, _) {

  override val paramType: String = "integer"

  override def getSamples(numVal: Int): Array[Int] = {
    RangeSlicer.evenlySamplingFromRange(minVal.toDouble, maxVal.toDouble, numVal).map(_.toInt)
  }
}

class DoubleRangeSlicer(
    override val name: String,
    val minVal: Double,
    val maxVal: Double,
    val scale: String = "log",
    val initVal: Double = None) extends RangeSlicer[Double](name, _) {

  override val paramType: String = "continuous"

  override def getSamples(numVal: Int): Array[Double] = {
    if (this.scale == "log") {
      val minExponent = math.log10(minVal)
      val maxExponent = math.log10(maxVal)
      RangeSlicer.evenlySamplingFromRange(minExponent, maxExponent, numVal)
        .map(x => math.pow(10, x))
    } else {
      RangeSlicer.evenlySamplingFromRange(minVal, maxVal, numVal)
    }
  }
}

object RangeSlicer {
  def evenlySamplingFromRange(minVal: Double, maxVal: Double, numVal: Int): Array[Double] = {
    val stepSize = (maxVal - minVal) * 1.0 / numVal
    val result = Array.fill[Double](numVal)(0)
    var i = 0
    while (i < numVal) {
      result(i) = minVal + i * stepSize
      i += 1
    }
    result
  }
}
