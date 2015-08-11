package org.apache.spark.ml.tuning.bandit

import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.{DoubleParam, IntParam, ParamMap}
import org.apache.spark.ml.param.shared.HasMaxIter
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by panda on 7/31/15.
 */

class Arm[M <: Model[M]](
    var data: Dataset,
    var model: M,
    var results: Array[Double] = Array.empty,
    var numPulls: Int,
    var numEvals: Int,
    var abridgedHistoryValCompute: Boolean = false,
    var abridgedHistoryValX: Array[Int] = Array.empty,
    var abridgedHistoryValY: Array[Double] = Array.empty,
    var abridgedHistoryValAlpha: Double = 1.2,
    val modelType: String,
    val estimator: PartialEstimator[M],
    val downSamplingFactor: Double = 1,
    // TODO, remember to set all parameters for evaluator, i.e. label column and score column
    val evaluator: Evaluator,
    val stepsPerPulling: Int) {

  def reset(): this.type = {
    this.results = Array.empty
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
    this.reset()
    val valXArrayBuffer = new ArrayBuffer[Int]()
    val valYArrayBuffer = new ArrayBuffer[Double]()
    while (this.numPulls < maxIter) {
      this.pullArm()
      if (this.abridgedHistoryValCompute) {
        if (this.abridgedHistoryValX.size == 0 || this.numPulls > valXArrayBuffer.last * this.abridgedHistoryValAlpha) {
          valXArrayBuffer.append(this.numPulls)
          val error = this.getResults(true, Some("validation"))(1)
          valYArrayBuffer.append(error)
        }
      }
    }
    this.abridgedHistoryValX = valXArrayBuffer.toArray
    this.abridgedHistoryValY = valYArrayBuffer.toArray
  }

  def getResults(forceRecompute: Boolean = true, partition: Option[String] = None): Array[Double] = {
    if (this.results == Array.empty || forceRecompute) {
      this.numEvals += 1
      if (partition == None || this.results == Array.empty) {
        this.results = Array(evaluator.evaluate(model.transform(data.trainingSet)),
          evaluator.evaluate(model.transform(data.validationSet)),
          evaluator.evaluate(model.transform(data.testSet)))
      } else if (partition == Some("train")) {
        this.results(0) = evaluator.evaluate(model.transform(data.trainingSet))
      } else if (partition == Some("validation")) {
        this.results(1) = evaluator.evaluate(model.transform(data.validationSet))
      } else if (partition == Some("test")) {
        this.results(2) = evaluator.evaluate(model.transform(data.testSet))
      } else {
        // TODO
      }
    }
    this.results
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
