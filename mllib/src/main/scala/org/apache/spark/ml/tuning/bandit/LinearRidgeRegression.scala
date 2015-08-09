package org.apache.spark.ml.tuning.bandit

import org.apache.spark.ml.param.shared.{HasOutputCol, HasInputCol}
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.ml.param.{IntParam, ParamMap, DoubleParam, Params}
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.{RidgeRegressionWithSGD, LabeledPoint}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, StructType}

/**
 * Created by panda on 8/10/15.
 */

trait LinearRidgeRegressionBase
  extends Params with HasInputCol with HasOutputCol with HasStepControl {

  val regularizer: DoubleParam = new DoubleParam(this, "regularizer", "regularization parameter")

  setDefault(regularizer -> 0.1)

  def getRegularizer: Double = $(regularizer)

  /**
   * Validate and transform the input schema.
   */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), new VectorUDT)
    schema
  }
}

class LinearRidgeRegression(override val uid: String)
  extends PartialEstimator[LinearRegressionModel] with LinearRidgeRegressionBase {

  def this() = this(Identifiable.randomUID("linear ridge regression"))

  def setRegularizer(value: Double): this.type = set(regularizer, value)

  def setStep(value: Int): this.type = set(step, value)

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
  override def fit(dataset: DataFrame): LinearRegressionModel = {
    ???
  }

  override def fit(
      dataset: DataFrame,
      initModel: LinearRegressionModel,
      steps: Int = 1): LinearRegressionModel = {
    val currentStep = $(step) + 1
    this.setStep(currentStep)
    val data = dataset.map { case Row(x: Vector, y: Double) => LabeledPoint(y, x)}
    val weight =
      LinearRidgeRegression.singleSGDStep(data, currentStep, initModel.weights, $(step), steps)
    new LinearRegressionModel(uid, weight, initModel.intercept)
  }

  override def copy(extra: ParamMap): LinearRidgeRegression = defaultCopy(extra)

}

object LinearRidgeRegression {
  def singleSGDStep(
      data: RDD[LabeledPoint],
      currentStep: Int,
      currentWeight: Vector,
      regularizer: Double,
      steps: Int): Vector = {
    val stepSize = 0.01 / math.sqrt(2 + currentStep)
    val newModel =
      RidgeRegressionWithSGD.train(data, steps, stepSize, regularizer, 0.1, currentWeight)
    newModel.weights
  }
}
