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
    var data: Dataset,
    var model: M,
    var results: (Double, Double, Double),
    var numPulls: Int,
    var numEvals: Int,
    var abridgedHistoryValCompute: Boolean = false,
    var abridgedHistoryValX: Array[Double] = Array.empty[Double],
    var abridgedHistoryValY: Array[Double] = Array.empty[Double],
    var abridgedHistoryValAlpha: Double = 1.2,
    val modelType: String,
    val estimator: PartialEstimator[M],
    val downSamplingFactor: Double = 1,
    val evaluator: Evaluator,
    val stepsPerPulling: Int) {

  def reset(): this.type = {
    this.results = null
    this.numPulls = 0
    this.numEvals = 0
    // TODO making default model for every kind of model.
    // this.model.
    this
  }

  def stripArm(): Unit = {
    this.data = null
    // TODO define null model
    // this.model = None
    this.abridgedHistoryValX = Array.empty
    this.abridgedHistoryValY = Array.empty
  }

  def pullArm(): Unit = {
    this.numPulls += 1
    val partialModel = this.estimator.setDownSamplingFactor(downSamplingFactor)
      .fit(this.data.trainingSet, this.model, this.stepsPerPulling)
    this.model = partialModel
  }

  def trainToCompletion(maxIter: Double): Unit = {

  }

  def getResults(forceRecompute: Boolean = true, partition: String = None): (Double, Double, Double) = {
    ???
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
          case parameter@(_: IntParamSampler) =>
            val param = new IntParam("from arm", parameter.name, "arm generated parameter")
            paramMap.put(param, parameter.getOneRandomSample)
          case parameter@(_: DoubleParamSampler) =>
            val param = new DoubleParam("from arm", parameter.name, "arm generated parameter")
            paramMap.put(param, parameter.getOneRandomSample)
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
