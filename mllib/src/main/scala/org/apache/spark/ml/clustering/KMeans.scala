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

package org.apache.spark.ml.clustering

import org.apache.hadoop.fs.Path
import org.json4s.DefaultFormats

import org.apache.spark.SparkException
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.{IntParam, Param, ParamMap, Params}
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.clustering.{KMeans => MLlibKMeans, KMeansModel => MLlibKMeansModel}
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{IntegerType, StructType}

/**
 * Common params for KMeans and KMeansModel
 */
private[clustering] trait KMeansParams extends Params with HasMaxIter with HasFeaturesCol
  with HasSeed with HasPredictionCol with HasTol with HasInitialModel[KMeansModel] {

  /**
   * Set the number of clusters to create (k). Must be > 1. Default: 2.
   * @group param
   */
  @Since("1.5.0")
  final val k = new IntParam(this, "k", "number of clusters to create", (x: Int) => x > 1)

  /** @group getParam */
  @Since("1.5.0")
  def getK: Int = $(k)

  /**
   * Param for the initialization algorithm. This can be either "random" to choose random points as
   * initial cluster centers, or "k-means||" to use a parallel variant of k-means++
   * (Bahmani et al., Scalable K-Means++, VLDB 2012). Default: k-means||.
   * @group expertParam
   */
  @Since("1.5.0")
  final val initMode = new Param[String](this, "initMode", "initialization algorithm",
    (value: String) => MLlibKMeans.validateInitMode(value))

  /** @group expertGetParam */
  @Since("1.5.0")
  def getInitMode: String = $(initMode)

  /**
   * Param for the number of steps for the k-means|| initialization mode. This is an advanced
   * setting -- the default of 5 is almost always enough. Must be > 0. Default: 5.
   * @group expertParam
   */
  @Since("1.5.0")
  final val initSteps = new IntParam(this, "initSteps", "number of steps for k-means||",
    (value: Int) => value > 0)

  /** @group expertGetParam */
  @Since("1.5.0")
  def getInitSteps: Int = $(initSteps)

  /**
   * Validates and transforms the input schema.
   * @param schema input schema
   * @return output schema
   */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    SchemaUtils.appendColumn(schema, $(predictionCol), IntegerType)
  }

  override def validateParams(): Unit = {
    super.validateParams()
    if (isSet(initialModel)) {
      val kOfInitialModel = $(initialModel).parentModel.clusterCenters.length
      require(kOfInitialModel == $(k),
        s"${$(k)} cluster centers required but $kOfInitialModel found in the initial model.")
    }
  }
}

/**
 * :: Experimental ::
 * Model fitted by KMeans.
 *
 * @param parentModel a model trained by spark.mllib.clustering.KMeans.
 */
@Since("1.5.0")
@Experimental
class KMeansModel private[ml] (
    @Since("1.5.0") override val uid: String,
    private[ml] val parentModel: MLlibKMeansModel)
  extends Model[KMeansModel] with KMeansParams with MLWritable {

  @Since("1.5.0")
  override def copy(extra: ParamMap): KMeansModel = {
    val copied = new KMeansModel(uid, parentModel)
    copyValues(copied, extra)
  }

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val predictUDF = udf((vector: Vector) => predict(vector))
    dataset.withColumn($(predictionCol), predictUDF(col($(featuresCol))))
  }

  @Since("1.5.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  private[clustering] def predict(features: Vector): Int = parentModel.predict(features)

  @Since("1.5.0")
  def clusterCenters: Array[Vector] = parentModel.clusterCenters

  /**
   * Return the K-means cost (sum of squared distances of points to their nearest center) for this
   * model on the given data.
   */
  // TODO: Replace the temp fix when we have proper evaluators defined for clustering.
  @Since("2.0.0")
  def computeCost(dataset: Dataset[_]): Double = {
    SchemaUtils.checkColumnType(dataset.schema, $(featuresCol), new VectorUDT)
    val data = dataset.select(col($(featuresCol))).rdd.map { case Row(point: Vector) => point }
    parentModel.computeCost(data)
  }

  @Since("1.6.0")
  override def write: MLWriter = new KMeansModel.KMeansModelWriter(this)

  override def hashCode(): Int = {
    (Array(this.getClass, uid) ++ clusterCenters)
      .foldLeft(17) { case (hash, obj) => hash * 31 + obj.hashCode() }
  }

  override def equals(other: Any): Boolean = other match {
    case that: KMeansModel =>
      this.uid == that.uid && this.clusterCenters.sameElements(that.clusterCenters)
    case _ => false
  }

  private var trainingSummary: Option[KMeansSummary] = None

  private[clustering] def setSummary(summary: KMeansSummary): this.type = {
    this.trainingSummary = Some(summary)
    this
  }

  /**
   * Return true if there exists summary of model.
   */
  @Since("2.0.0")
  def hasSummary: Boolean = trainingSummary.nonEmpty

  /**
   * Gets summary of model on training set. An exception is
   * thrown if `trainingSummary == None`.
   */
  @Since("2.0.0")
  def summary: KMeansSummary = trainingSummary.getOrElse {
    throw new SparkException(
      s"No training summary available for the ${this.getClass.getSimpleName}")
  }
}

@Since("1.6.0")
object KMeansModel extends MLReadable[KMeansModel] {

  @Since("1.6.0")
  override def read: MLReader[KMeansModel] = new KMeansModelReader

  @Since("1.6.0")
  override def load(path: String): KMeansModel = super.load(path)

  /** [[MLWriter]] instance for [[KMeansModel]] */
  private[KMeansModel] class KMeansModelWriter(instance: KMeansModel) extends MLWriter {
    import org.json4s.JsonDSL._

    private case class Data(clusterCenters: Array[Vector])

    override protected def saveImpl(path: String): Unit = {
      if (instance.isSet(instance.initialModel)) {
        val initialModelPath = new Path(path, "initialModel").toString
        val initialModel = instance.getInitialModel
        initialModel.save(initialModelPath)

        // Save metadata and Params
        DefaultParamsWriter.saveMetadata(instance, path, sc, Some("hasInitialModel" -> true))
      } else {
        // Save metadata and Params
        DefaultParamsWriter.saveMetadata(instance, path, sc, Some("hasInitialModel" -> false))
      }

      // Save model data: cluster centers
      val data = Data(instance.clusterCenters)
      val dataPath = new Path(path, "data").toString
      sqlContext.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class KMeansModelReader extends MLReader[KMeansModel] {
    implicit val format = DefaultFormats

    /** Checked against metadata when loading model */
    private val className = classOf[KMeansModel].getName

    override def load(path: String): KMeansModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString
      val data = sqlContext.read.parquet(dataPath).select("clusterCenters").head()
      val clusterCenters = data.getAs[Seq[Vector]](0).toArray
      val model = new KMeansModel(metadata.uid, new MLlibKMeansModel(clusterCenters))

      DefaultParamsReader.getAndSetParams(model, metadata)

      // Try to load initial model after version 2.0.0.
      if (metadata.sparkVersion.split("\\.").head.toInt >= 2) {
        val hasInitialModel = (metadata.metadata \ "hasInitialModel").extract[Boolean]
        if (hasInitialModel) {
          val initialModelPath = new Path(path, "initialModel").toString
          val initialModel = KMeansModel.load(initialModelPath)
          model.set(model.initialModel, initialModel)
        }
      }

      model
    }
  }
}

/**
 * :: Experimental ::
 * K-means clustering with support for k-means|| initialization proposed by Bahmani et al.
 *
 * @see [[http://dx.doi.org/10.14778/2180912.2180915 Bahmani et al., Scalable k-means++.]]
 */
@Since("1.5.0")
@Experimental
class KMeans @Since("1.5.0") (
    @Since("1.5.0") override val uid: String)
  extends Estimator[KMeansModel] with KMeansParams with MLWritable {

  setDefault(
    k -> 2,
    maxIter -> 20,
    initMode -> MLlibKMeans.K_MEANS_PARALLEL,
    initSteps -> 5,
    tol -> 1e-4)

  @Since("1.5.0")
  override def copy(extra: ParamMap): KMeans = defaultCopy(extra)

  @Since("1.5.0")
  def this() = this(Identifiable.randomUID("kmeans"))

  /** @group setParam */
  @Since("1.5.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setK(value: Int): this.type = set(k, value)

  /** @group expertSetParam */
  @Since("1.5.0")
  def setInitMode(value: String): this.type = set(initMode, value)

  /** @group expertSetParam */
  @Since("1.5.0")
  def setInitSteps(value: Int): this.type = set(initSteps, value)

  /** @group setParam */
  @Since("1.5.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /** @group setParam */
  @Since("1.5.0")
  def setTol(value: Double): this.type = set(tol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setSeed(value: Long): this.type = set(seed, value)

  /** @group setParam */
  @Since("2.0.0")
  def setInitialModel(value: KMeansModel): this.type = set(initialModel, value)

  /** @group setParam */
  @Since("2.0.0")
  def setInitialModel(value: Model[_]): this.type = {
    value match {
      case m: KMeansModel => setInitialModel(m)
      case other =>
        logWarning(s"KMeansModel required but ${other.getClass.getSimpleName} found.")
        this
    }
  }

  /** @group setParam */
  @Since("2.0.0")
  def setInitialModel(clusterCenters: Array[Vector]): this.type = {
    setInitialModel(new KMeansModel("initial model", new MLlibKMeansModel(clusterCenters)))
  }

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): KMeansModel = {
    val rdd = dataset.select(col($(featuresCol))).rdd.map { case Row(point: Vector) => point }

    val algo = new MLlibKMeans()
      .setK($(k))
      .setInitializationMode($(initMode))
      .setInitializationSteps($(initSteps))
      .setMaxIterations($(maxIter))
      .setSeed($(seed))
      .setEpsilon($(tol))

    if (isSet(initialModel)) {
      require(rdd.first().size == $(initialModel).clusterCenters.head.size, "mismatched dimension")
      algo.setInitialModel($(initialModel).parentModel)
    }

    val parentModel = algo.run(rdd)
    val model = copyValues(new KMeansModel(uid, parentModel).setParent(this))
    val summary = new KMeansSummary(
      model.transform(dataset), $(predictionCol), $(featuresCol), $(k))
    model.setSummary(summary)
  }

  @Since("1.5.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("2.0.0")
  override def write: MLWriter = new KMeans.KMeansWriter(this)
}

@Since("1.6.0")
object KMeans extends MLReadable[KMeans] {

  @Since("1.6.0")
  override def load(path: String): KMeans = super.load(path)

  @Since("2.0.0")
  override def read: MLReader[KMeans] = new KMeansReader

  /** [[MLWriter]] instance for [[KMeans]] */
  private[KMeans] class KMeansWriter(instance: KMeans) extends MLWriter {
    import org.json4s.JsonDSL._

    override protected def saveImpl(path: String): Unit = {
      if (instance.isSet(instance.initialModel)) {
        val initialModelPath = new Path(path, "initialModel").toString
        val initialModel = instance.getInitialModel
        initialModel.save(initialModelPath)

        // Save metadata and Params
        DefaultParamsWriter.saveMetadata(instance, path, sc, Some("hasInitialModel" -> true))
      } else {
        // Save metadata and Params
        DefaultParamsWriter.saveMetadata(instance, path, sc, Some("hasInitialModel" -> false))
      }
    }
  }

  private class KMeansReader extends MLReader[KMeans] {

    /** Checked against metadata when loading model */
    private val className = classOf[KMeans].getName

    override def load(path: String): KMeans = {
      implicit val format = DefaultFormats

      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val instance = new KMeans(metadata.uid)
      DefaultParamsReader.getAndSetParams(instance, metadata)

      // Try to load initial model after version 2.0.0.
      if (metadata.sparkVersion.split("\\.").head.toInt >= 2) {
        val hasInitialModel = (metadata.metadata \ "hasInitialModel").extract[Boolean]
        if (hasInitialModel) {
          val initialModelPath = new Path(path, "initialModel").toString
          val initialModel = KMeansModel.load(initialModelPath)
          instance.setInitialModel(initialModel)
        }
      }

      instance
    }
  }
}

/**
 * :: Experimental ::
 * Summary of KMeans.
 *
 * @param predictions  [[DataFrame]] produced by [[KMeansModel.transform()]]
 * @param predictionCol  Name for column of predicted clusters in `predictions`
 * @param featuresCol  Name for column of features in `predictions`
 * @param k  Number of clusters
 */
@Since("2.0.0")
@Experimental
class KMeansSummary private[clustering] (
    @Since("2.0.0") @transient val predictions: DataFrame,
    @Since("2.0.0") val predictionCol: String,
    @Since("2.0.0") val featuresCol: String,
    @Since("2.0.0") val k: Int) extends Serializable {

  /**
   * Cluster centers of the transformed data.
   */
  @Since("2.0.0")
  @transient lazy val cluster: DataFrame = predictions.select(predictionCol)

  /**
   * Size of (number of data points in) each cluster.
   */
  @Since("2.0.0")
  lazy val clusterSizes: Array[Long] = {
    val sizes = Array.fill[Long](k)(0)
    cluster.groupBy(predictionCol).count().select(predictionCol, "count").collect().foreach {
      case Row(cluster: Int, count: Long) => sizes(cluster) = count
    }
    sizes
  }

}
