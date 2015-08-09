package org.apache.spark.ml.tuning.bandit

import org.apache.spark.ml.Estimator
import org.apache.spark.ml.Model
import org.apache.spark.ml.param.{ParamMap, ParamPair}
import org.apache.spark.sql.DataFrame

import scala.annotation.varargs

/**
 * Created by panda on 8/2/15.
 */
abstract class PartialEstimator[M <: Model[M]] extends Estimator[M] {

  def fit(dataset: DataFrame, initModel: M, paramMap: ParamMap, steps: Int = 1): M = {
    copy(paramMap).fit(dataset, initModel, steps)
  }

  def fit(dataset: DataFrame, initModel: M, steps: Int = 1): M

  def fit(dataset: DataFrame, initModel: M, paramMaps: Array[ParamMap], steps: Int = 1): Seq[M] = {
    paramMaps.map(fit(dataset, initModel, _, steps))
  }

  override def copy(extra: ParamMap): PartialEstimator[M]
}
