package org.apache.spark.ml.tuning.bandit

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.util.IntParam

import scala.collection.mutable

import org.apache.spark.ml.param.{DoubleParam, ParamMap, Params}
import org.apache.spark.sql.DataFrame

/**
 * Created by panda on 8/1/15.
 */
abstract class ModelFamily(val name: String, val paramList: Array[ParamMap]) {
  def createArm(initData: DataFrame, params: ParamMap): Arm

  def addArm(hp: ParamMap, arms: mutable.Map[(String, String), Arm], arm: Arm): Unit = {
    arms += ((this.name, hp.toString) -> arm)
  }

  def createArms(hpPoints: Array[ParamMap],
    initData: DataFrame,
    arms: mutable.Map[(String, String), Arm]): mutable.Map[(String, String), Arm] = {
    for (hp <- hpPoints) {
      this.addArm(hp, arms, this.createArm(initData, hp))
    }
    arms
  }
}

class LinRegressionModelFamily(override val name: String, override val paramList: Array[ParamMap], val linRegressionModel: LinearRegression)
  extends ModelFamily(name, paramList) {
  override def createArm(initData: DataFrame, params: ParamMap): Arm = {
    val reg = params.get(linRegressionModel.regParam)
  }
}