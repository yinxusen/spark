package org.apache.spark.ml.tuning.bandit

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{LinearRegressionModel, LinearRegression}
import org.apache.spark.util.IntParam

import scala.collection.mutable

import org.apache.spark.ml.param.{DoubleParam, ParamMap, Params, Param}
import org.apache.spark.sql.DataFrame

/**
 * Created by panda on 8/1/15.
 */
abstract class ModelFamily(val name: String, val paramList: Array[ParamSampler[_]]) {
  def createArm(initData: Dataset, params: ParamMap): Arm

  def addArm(hp: ParamMap, arms: mutable.Map[(String, String), Arm], arm: Arm): Unit = {
    arms += ((this.name, hp.toString) -> arm)
  }

  def createArms(
      hpPoints: Array[ParamMap],
      initData: Dataset,
      arms: mutable.Map[(String, String), Arm]): mutable.Map[(String, String), Arm] = {
    for (hp <- hpPoints) {
      this.addArm(hp, arms, this.createArm(initData, hp))
    }
    arms
  }
}

class LinRegressionModelFamily(
    override val name: String,
    override val paramList: Array[ParamSampler[_]])
  extends ModelFamily(name, paramList) {

  override def createArm(initData: Dataset, params: ParamMap): Arm[_] = {
    val linearRidgeRegression= new LinearRidgeRegression()
      .setDownSamplingFactor(0.1).setStepsPerPulling(1).copy(params)
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
    new Arm[LinearRidgeRegressionModel](
      initData, None, 0, 0, "linear ridge regression", linearRidgeRegression, evaluator)
  }
}