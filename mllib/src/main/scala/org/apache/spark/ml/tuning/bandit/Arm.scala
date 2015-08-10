package org.apache.spark.ml.tuning.bandit

import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.{DoubleParam, IntParam, ParamMap}
import org.apache.spark.ml.param.shared.HasMaxIter
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

/**
 * Created by panda on 7/31/15.
 */

class Arm[M <: Model[M]](
    val estimator: PartialEstimator[M],
    evaluator: Evaluator,
    stepsPerPulling: Int) {

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
  def generateArms(
      modelFamilies: Array[ModelFamily],
      data: DataFrame,
      numArmsPerParameter: Int): Map[(String, String), Arm] = {
    val arms = new mutable.HashMap[(String, String), Arm]()
    for (modelFamily <- modelFamilies) {
      val numParamsToTune = modelFamily.paramList.size
      val numArmsForModelFamily = numParamsToTune * numArmsPerParameter
      val hyperParameterPoints = (0 until numArmsForModelFamily).map { index =>
        val paramMap = new ParamMap()
        modelFamily.paramList.map {
          case parameter@(_: IntParam) =>
            paramMap.put(parameter, 1)
          case parameter@(_: DoubleParam) =>
            paramMap.put(parameter, 1.0)
          case _ =>
          // TODO refine the code
        }
        paramMap
      }.toArray
      modelFamily.createArms(hyperParameterPoints, data, arms)
    }
    arms.toMap
  }
}
