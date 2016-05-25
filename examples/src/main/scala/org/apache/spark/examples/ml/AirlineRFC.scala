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

// scalastyle:off
package org.apache.spark.examples.ml

import scopt.OptionParser

import org.apache.spark.examples.mllib.AbstractParams
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{RFormula, StringIndexer, VectorAssembler}
import org.apache.spark.ml.param.ParamPair
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * An example runner for Random Forest with Airline Dataset. Run with
 * {{{
 * ./bin/run-example ml.AirlineRFC [options]
 * }}}
 * Decision Trees and ensembles can take a large amount of memory. If the run-example command
 * above fails, try running via spark-submit and specifying the amount of memory as at least 1g.
 * For local mode, run
 * {{{
 * ./bin/spark-submit --class org.apache.spark.examples.ml.AirlineRFC --driver-memory 1g
 *   [examples JAR path] [options]
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */

class DataIngestion (val data: DataFrame) {

  val splitCol = "isTrain"
  val labelCol = "dep_delayed_15min"
  val featureCols = data.columns.filterNot(Set(splitCol, labelCol).contains)

  /**
    * Cast String columns to Double columns to prevent recognizing continuous features as
    * categorical ones.
    *
    * @param col Column name
    * @param cols Column names
    * @return DataFrame with the same column names
    */
  def _castStringToDouble(col: String, cols: String*): DataFrame = {
    (Seq(col) ++ cols).foldLeft(data) { case (df, c) =>
      df.withColumn(c, df(c).cast(DoubleType))
    }
  }

  /**
    * Check the schema is right with all those columns.
    */
  def _checkSchema(): Unit = {
    assert(Array(splitCol, labelCol).map(data.columns.contains).reduce(_ && _),
      s"Your data does not contain $splitCol and $labelCol.")
  }

  /**
    * Provide the training and test set with all columns set.
    */
  def withStringIndexer: (DataFrame, DataFrame) = {
    _checkSchema()
    val df = _castStringToDouble("DepTime", "Distance")

    val si = new StringIndexer()
    val cols = df.columns.filterNot(Set(splitCol, "DepTime", "Distance").contains)

    val finalDF = cols.foldLeft(df) { case (aggregate, col) =>
      si.setInputCol(col).setOutputCol(s"si_$col").fit(aggregate).transform(aggregate)
    }

    val newFeatureCols =
      featureCols.map(s => if(Set("DepTime", "Distance").contains(s)) s else s"si_$s")
    val newLabelCol = s"si_$labelCol"

    val allData = new VectorAssembler()
      .setInputCols(newFeatureCols).setOutputCol("features").transform(finalDF)
      .select("features", newLabelCol, splitCol)
      .withColumn("label", col(newLabelCol)).drop(col(newLabelCol))

    (allData.where(col(splitCol)), allData.where(!col(splitCol)))
  }

  /**
    * Provide the training and test set with all columns set.
    */
  def withRFormula: (DataFrame, DataFrame) = {
    val allData = new RFormula().setFormula(s"$labelCol ~ . - $splitCol").fit(data).transform(data)
    (allData.where(col(splitCol)), allData.where(!col(splitCol)))
  }
}

object DataIngestion {
  def apply(session: SparkSession, trainPath: String, testPath: String): DataIngestion = {
    // Read CSV as Spark DataFrames
    val loader = session.read.format("com.databricks.spark.csv").option("header", "true")
    val trainDF = loader.load(trainPath)
    val testDF = loader.load(testPath)

    // Combine train, test temporarily
    val fullDF = trainDF.withColumn("isTrain", lit(true))
      .union(testDF.withColumn("isTrain", lit(false)))

    new DataIngestion(fullDF)
  }
}

object AirlineRFC {

  case class Params(
      input: String = null,
      testInput: String = null,
      algo: String = "classification",
      impurity: Seq[String] = Seq("gini"),
      maxDepth: Seq[Int] = Seq(5),
      maxBins: Seq[Int] = Seq(400),
      minInstancesPerNode: Seq[Int] = Seq(1),
      minInfoGain: Seq[Double] = Seq(0.0),
      numTrees: Seq[Int] = Seq(10),
      featureSubsetStrategy: Seq[String] = Seq("auto"),
      subsamplingRate: Seq[Double] = Seq(1.0),
      maxMemoryInMB: Int = 32,
      cacheNodeIds: Boolean = false,
      checkpointDir: Option[String] = None,
      checkpointInterval: Int = 10) extends AbstractParams[Params]

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("RandomForestExample") {
      head("RandomForestExample: an example random forest app.")
      opt[String]("algo")
        .text(s"algorithm (classification, regression), default: ${defaultParams.algo}")
        .action((x, c) => c.copy(algo = x))
      opt[Seq[String]]("impurity")
        .text(s"Impurity (gini, entropy, variance(only for regression))," +
          s" default ${defaultParams.impurity}")
        .action((x, c) => c.copy(impurity = x))
      opt[Seq[Double]]("subsamplingRate")
        .text(s"Subsampling rate, default ${defaultParams.subsamplingRate}")
        .action((x, c) => c.copy(subsamplingRate = x))
      opt[Seq[Int]]("maxDepth")
        .text(s"max depth of the tree, default: ${defaultParams.maxDepth}")
        .action((x, c) => c.copy(maxDepth = x))
      opt[Seq[Int]]("maxBins")
        .text(s"max number of bins, default: ${defaultParams.maxBins}")
        .action((x, c) => c.copy(maxBins = x))
      opt[Seq[Int]]("minInstancesPerNode")
        .text(s"min number of instances required at child nodes to create the parent split," +
        s" default: ${defaultParams.minInstancesPerNode}")
        .action((x, c) => c.copy(minInstancesPerNode = x))
      opt[Seq[Double]]("minInfoGain")
        .text(s"min info gain required to create a split, default: ${defaultParams.minInfoGain}")
        .action((x, c) => c.copy(minInfoGain = x))
      opt[Seq[Int]]("numTrees")
        .text(s"number of trees in ensemble, default: ${defaultParams.numTrees}")
        .action((x, c) => c.copy(numTrees = x))
      opt[Seq[String]]("featureSubsetStrategy")
        .text(s"number of features to use per node (supported:" +
        s" ${RandomForestClassifier.supportedFeatureSubsetStrategies.mkString(",")})," +
        s" default: ${defaultParams.numTrees}")
        .action((x, c) => c.copy(featureSubsetStrategy = x))
      opt[Boolean]("cacheNodeIds")
        .text(s"whether to use node Id cache during training, " +
        s"default: ${defaultParams.cacheNodeIds}")
        .action((x, c) => c.copy(cacheNodeIds = x))
      opt[Int]("maxMemoryInMB")
        .text(s"maximum memory to collect histogram in a task, default" +
          s" ${defaultParams.maxMemoryInMB}")
        .action((x, c) => c.copy(maxMemoryInMB = x))
      opt[String]("checkpointDir")
        .text(s"checkpoint directory where intermediate node Id caches will be stored, " +
        s"default: ${
          defaultParams.checkpointDir match {
            case Some(strVal) => strVal
            case None => "None"
          }
        }")
        .action((x, c) => c.copy(checkpointDir = Some(x)))
      opt[Int]("checkpointInterval")
        .text(s"how often to checkpoint the node Id cache, " +
        s"default: ${defaultParams.checkpointInterval}")
        .action((x, c) => c.copy(checkpointInterval = x))
      arg[String]("<input>")
        .text("input path to labeled examples")
        .required()
        .action((x, c) => c.copy(input = x))
      arg[String]("<testInput>")
        .text(s"input path to test dataset")
        .required()
        .action((x, c) => c.copy(testInput = x))
      checkConfig { params => success }
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params): Unit = {
    val spark = SparkSession
      .builder()
      .appName(s"RandomForestExample of Airline with $params")
      .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")
    params.checkpointDir.foreach(spark.sparkContext.setCheckpointDir)
    val algo = params.algo.toLowerCase

    println(s"RandomForestExample with parameters:\n$params")

    val (trainDF, testDF) =
      DataIngestion(spark, params.input, params.testInput).withStringIndexer

    val train = trainDF.coalesce(16).cache()
    val test = testDF.cache()

    println(s"xusen, Partitions of train set is ${train.rdd.partitions.size}")

    val rfc = new RandomForestClassifier()
    val metrics = new BinaryClassificationEvaluator().setMetricName("areaUnderROC")

    val grid = new ParamGridBuilder()
      .addGrid(rfc.maxBins, params.maxBins)
      .addGrid(rfc.maxDepth, params.maxDepth)
      .addGrid(rfc.impurity, params.impurity)
      .addGrid(rfc.featureSubsetStrategy, params.featureSubsetStrategy)
      .addGrid(rfc.minInstancesPerNode, params.minInstancesPerNode)
      .addGrid(rfc.numTrees, params.numTrees)
      .addGrid(rfc.subsamplingRate, params.subsamplingRate)
      .addGrid(rfc.minInfoGain, params.minInfoGain)
      .baseOn(ParamPair(rfc.maxMemoryInMB, params.maxMemoryInMB))
      .baseOn(ParamPair(rfc.cacheNodeIds, params.cacheNodeIds))
      .baseOn(ParamPair(rfc.checkpointInterval, params.checkpointInterval))
      .build()

    val cv = new CrossValidator()
      .setEstimator(rfc).setEvaluator(metrics).setEstimatorParamMaps(grid)

    val begin = System.nanoTime

    val model = cv.fit(train)

    val importance =
      model.bestModel.asInstanceOf[RandomForestClassificationModel].featureImportances

    val trees = model.bestModel.asInstanceOf[RandomForestClassificationModel].trees
    val weights = model.bestModel.asInstanceOf[RandomForestClassificationModel].treeWeights

    val elapsed = (System.nanoTime - begin) / 1e9
    println(s"Elapsed time for training is $elapsed")

    println(s"xusen, importance is $importance")
    println(s"xusen, depths of trees are\n${trees.map(_.depth).mkString("\n")}")
    println(s"xusen, nodes of trees are\n${trees.map(x => x.toOld.numNodes).mkString("\n")}")
    println(s"xusen, leaves of trees are\n${trees.map(x => x.toOld.numLeaves).mkString("\n")}")
    println(s"xusen, tree weights are ${weights.mkString(", ")}")

    val testBegin = System.nanoTime
    println(s"AUC is ${metrics.evaluate(model.transform(test))}")
    val testElapsed = (System.nanoTime - testBegin) / 1e9
    println(s"Elapsed time for testing is $testElapsed")

    spark.stop()
  }
}
// scalastyle:on
