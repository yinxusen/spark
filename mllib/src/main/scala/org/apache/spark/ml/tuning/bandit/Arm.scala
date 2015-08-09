package org.apache.spark.ml.tuning.bandit

import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasMaxIter
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.DataFrame

/**
 * Created by panda on 7/31/15.
 */

class Arm[M <: Model[M]](val estimator: PartialEstimator[M], val params: ParamMap, evaluator: Evaluator, stepsPerPulling: Int) {

  /**
   * Pull an arm once time, given a dataset, then return the error in the step.
   */
  def pullArm(data: DataFrame, initModel: M, stepsPerPulling: Int = this.stepsPerPulling): (M, Double) = {
    val partialModel = this.estimator.fit(data, initModel, stepsPerPulling)
    val score = this.evaluator.evaluate(partialModel.transform(data))
    (partialModel, score)
  }
}

object Arms {
  // def generateArms(modelFamilies: String, data: DataFrame, numArmsPerParameter: Int)
}
