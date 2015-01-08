package org.apache.spark.newml.model

import org.apache.spark.newml.linalg._

case class VectorModel (
    weights: Vector,
    intercept: Double) {
  protected def predictPoint(
      dataMatrix: Vector,
      weightMatrix: Vector,
      intercept: Double): Double = {
    weightMatrix.toBreeze.dot(dataMatrix.toBreeze) + intercept
  }
}