package org.apache.spark.mllib.tree.configuration

import org.apache.spark.mllib.tree.configuration.FeatureType.FeatureType

/**
 * Assume that all continuous features are Double, all categorical features are String.
 */
case class DataSchema(label: String, features: Array[String], featureTypes: Array[FeatureType]) {
  override def toString = {
    s"$label, ${features.mkString(", ")}"
  }

  def featuresString = {
    s"${features.mkString(", ")}"
  }
}

object DataSchema {
  /**
   * Default all types of features are continuous.
   */
  def apply(label: String, features: Array[String]): DataSchema = {
    DataSchema(label, features, features.map(_ => FeatureType.Continuous))
  }

  def apply(
      label: String,
      features: Array[String],
      isFeatureCategorical: Array[Boolean]): DataSchema = {
    DataSchema(label, features,
      isFeatureCategorical.map(b => if(b) FeatureType.Categorical else FeatureType.Continuous))
  }
}
