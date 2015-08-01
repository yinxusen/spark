package org.apache.spark.ml.tuning.bandit

import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.DataFrame

/**
 * Created by panda on 7/31/15.
 */
class Arm[M <: Model[M]](var data: DataFrame,
    var modelType: String,
    var estimator: Estimator[M],
    var model: M,
    val downSamplingFactor: Double = 1,
    var results: (Double, Double, Double) = null,
    var numPulls: Int = 0,
    var numEvals: Int = 0,
    val armValueAtInf: Double = Double.PositiveInfinity,
    val abridgedHistoryValCompute: Boolean = false,
    var abridgedHistoryValX: Array[Double] = Array.empty,
    var abridgedHistoryValY: Array[Double] = Array.empty,
    val abridgedHistoryValAlpha: Double = 1.2) {

  def reset(): Unit = {
    this.results = null
    this.numPulls = 0
    this.numEvals = 0
    // TODO reset model parameters here.
  }

  def pullArm(): Unit = {
    this.numPulls += 1
    // TODO some settings of estimator, i.e. the downsampling factor
    model = this.estimator.fit(data)
  }

  def getResults(forceRecompute: Boolean = false, partition: String = None): (Double, Double, Double) = {
    ???
  }

  def stripArm(): Unit = {
    this.data = null
    this.estimator = null
    this.abridgedHistoryValX = Array.empty
    this.abridgedHistoryValY = Array.empty
  }

  def trainToCompletion(maxIter: Int): Unit = {
    this.reset()
    while (this.numPulls < maxIter) {
      this.pullArm()
      if (this.abridgedHistoryValCompute) {
        // TODO fill in the procedure
      }
    }
  }
}

object Arms {
  // def generateArms(modelFamilies: String, data: DataFrame, numArmsPerParameter: Int)
}
