/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ml.api.r

import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel, NaiveBayes, NaiveBayesModel}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

private[r] object SparkRWrappers {
  def fitRModelFormula(
      value: String,
      df: DataFrame,
      family: String,
      lambda: Double,
      alpha: Double,
      standardize: Boolean,
      solver: String): PipelineModel = {
    val formula = new RFormula().setFormula(value)
    val estimator = family match {
      case "gaussian" => new LinearRegression()
        .setRegParam(lambda)
        .setElasticNetParam(alpha)
        .setFitIntercept(formula.hasIntercept)
        .setStandardization(standardize)
        .setSolver(solver)
      case "binomial" => new LogisticRegression()
        .setRegParam(lambda)
        .setElasticNetParam(alpha)
        .setFitIntercept(formula.hasIntercept)
        .setStandardization(standardize)
    }
    val pipeline = new Pipeline().setStages(Array(formula, estimator))
    pipeline.fit(df)
  }

  def fitNaiveBayes(
      value: String,
      df: DataFrame,
      lambda: Double,
      modelType: String): PipelineModel = {

    // Transform data with RFormula
    val formula = new RFormula().setFormula(value)
    val fModel = formula.fit(df)
    val rawLabels = fModel.getOriginalLabels

    val naiveBayes = new NaiveBayes().setSmoothing(lambda).setModelType(modelType)
    val rawLabelsIndexer = new IndexToString()
      .setInputCol(naiveBayes.getLabelCol).setOutputCol("rawLabelsPrediction")

    if (fModel.getOriginalLabels.isDefined) {
      // String labels have already been re-indexed by RFormula.
      val stages: Array[PipelineStage] =
        Array(fModel, naiveBayes, rawLabelsIndexer.setLabels(rawLabels.get))
      new Pipeline().setStages(stages).fit(df)
    } else {
      // Re-index numerical labels for NaiveBayes since it assumes labels are indices.
      val labelIndexer = new StringIndexer().setInputCol(fModel.getLabelCol).fit(df)
      val stages: Array[PipelineStage] =
        Array(
          labelIndexer,
          fModel,
          naiveBayes.setLabelCol(labelIndexer.getOutputCol),
          rawLabelsIndexer.setLabels(labelIndexer.labels))
      new Pipeline().setStages(stages).fit(df)
    }
  }

  def isNaiveBayesModel(model: PipelineModel): Boolean = {
    model.stages.length >= 2 &&
      model.stages(model.stages.length - 2).isInstanceOf[NaiveBayesModel] &&
      model.stages.last.isInstanceOf[IndexToString]
  }

  def fitKMeans(
      df: DataFrame,
      initMode: String,
      maxIter: Double,
      k: Double,
      columns: Array[String]): PipelineModel = {
    val assembler = new VectorAssembler().setInputCols(columns)
    val kMeans = new KMeans()
      .setInitMode(initMode)
      .setMaxIter(maxIter.toInt)
      .setK(k.toInt)
      .setFeaturesCol(assembler.getOutputCol)
    val pipeline = new Pipeline().setStages(Array(assembler, kMeans))
    pipeline.fit(df)
  }

  def getModelCoefficients(model: PipelineModel): Array[Double] = {
    model.stages.last match {
      case m: LinearRegressionModel =>
        val coefficientStandardErrorsR = Array(m.summary.coefficientStandardErrors.last) ++
          m.summary.coefficientStandardErrors.dropRight(1)
        val tValuesR = Array(m.summary.tValues.last) ++ m.summary.tValues.dropRight(1)
        val pValuesR = Array(m.summary.pValues.last) ++ m.summary.pValues.dropRight(1)
        if (m.getFitIntercept) {
          Array(m.intercept) ++ m.coefficients.toArray ++ coefficientStandardErrorsR ++
            tValuesR ++ pValuesR
        } else {
          m.coefficients.toArray ++ coefficientStandardErrorsR ++ tValuesR ++ pValuesR
        }
      case m: LogisticRegressionModel =>
        if (m.getFitIntercept) {
          Array(m.intercept) ++ m.coefficients.toArray
        } else {
          m.coefficients.toArray
        }
      case m: KMeansModel =>
        m.clusterCenters.flatMap(_.toArray)
      case _ if isNaiveBayesModel(model) => Array() // A dummy result to prevent unmatched error.
    }
  }

  def getModelDevianceResiduals(model: PipelineModel): Array[Double] = {
    model.stages.last match {
      case m: LinearRegressionModel =>
        m.summary.devianceResiduals
      case m: LogisticRegressionModel =>
        throw new UnsupportedOperationException(
          "No deviance residuals available for LogisticRegressionModel")
    }
  }

  def getKMeansModelSize(model: PipelineModel): Array[Int] = {
    model.stages.last match {
      case m: KMeansModel => Array(m.getK) ++ m.summary.size
      case other => throw new UnsupportedOperationException(
        s"KMeansModel required but ${other.getClass.getSimpleName} found.")
    }
  }

  def getKMeansCluster(model: PipelineModel, method: String): DataFrame = {
    model.stages.last match {
      case m: KMeansModel =>
        if (method == "centers") {
          // Drop the assembled vector for easy-print to R side.
          m.summary.predictions.drop(m.summary.featuresCol)
        } else if (method == "classes") {
          m.summary.cluster
        } else {
          throw new UnsupportedOperationException(
            s"Method (centers or classes) required but $method found.")
        }
      case other => throw new UnsupportedOperationException(
        s"KMeansModel required but ${other.getClass.getSimpleName} found.")
    }
  }

  /**
   * Extract labels' names for NaiveBayesModel.
   */
  def getNaiveBayesLabels(model: PipelineModel): Array[String] = {
    assert(isNaiveBayesModel(model),
      s"NaiveBayesModel required but ${model.stages.last.getClass.getSimpleName} found.")
    model.stages.last.asInstanceOf[IndexToString].getLabels
  }

  def getNaiveBayesPi(model: PipelineModel): Array[Double] = {
    assert(isNaiveBayesModel(model),
      s"NaiveBayesModel required but ${model.stages.last.getClass.getSimpleName} found.")
    model.stages(model.stages.length - 2).asInstanceOf[NaiveBayesModel].pi.toArray.map(math.exp)
  }

  def getNaiveBayesTheta(model: PipelineModel): Array[Double] = {
    assert(isNaiveBayesModel(model),
      s"NaiveBayesModel required but ${model.stages.last.getClass.getSimpleName} found.")
    model.stages(model.stages.length - 2).asInstanceOf[NaiveBayesModel].theta.toArray.map(math.exp)
  }

 def getModelFeatures(model: PipelineModel): Array[String] = {
    model.stages.last match {
      case m: LinearRegressionModel =>
        val attrs = AttributeGroup.fromStructField(
          m.summary.predictions.schema(m.summary.featuresCol))
        if (m.getFitIntercept) {
          Array("(Intercept)") ++ attrs.attributes.get.map(_.name.get)
        } else {
          attrs.attributes.get.map(_.name.get)
        }
      case m: LogisticRegressionModel =>
        val attrs = AttributeGroup.fromStructField(
          m.summary.predictions.schema(m.summary.featuresCol))
        if (m.getFitIntercept) {
          Array("(Intercept)") ++ attrs.attributes.get.map(_.name.get)
        } else {
          attrs.attributes.get.map(_.name.get)
        }
      case m: KMeansModel =>
        val attrs = AttributeGroup.fromStructField(
          m.summary.predictions.schema(m.summary.featuresCol))
        attrs.attributes.get.map(_.name.get)
      case _ if isNaiveBayesModel(model) =>
        model.stages(model.stages.length - 2).asInstanceOf[NaiveBayesModel].getFeatureNames
    }
  }

  def getModelName(model: PipelineModel): String = {
    model.stages.last match {
      case m: LinearRegressionModel => "LinearRegressionModel"
      case m: LogisticRegressionModel => "LogisticRegressionModel"
      case m: KMeansModel => "KMeansModel"
      case _ if isNaiveBayesModel(model) => "NaiveBayesModel"
    }
  }
}
