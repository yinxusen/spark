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

import scala.collection.mutable

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.{DecisionTreeClassifier, RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{RFormula, StringIndexer}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.param.ParamPair
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DoubleType

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
  def apply(sqlCtx: SQLContext, trainPath: String, testPath: String): DataIngestion = {
    // Read CSV as Spark DataFrames
    val loader = sqlCtx.read.format("com.databricks.spark.csv").option("header", "true")
    val trainDF = loader.load(trainPath)
    val testDF = loader.load(testPath)

    // Combine train, test temporarily
    val fullDF = trainDF.withColumn("isTrain", lit(true))
      .union(testDF.withColumn("isTrain", lit(false)))

    new DataIngestion(fullDF)
  }
}

object AirlineDataToNumeric {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("RFCAirline").setMaster("local[8]")
      // .set("spark.executor.memory", "5g")
    val sc = new SparkContext(conf)
    sc.setLogLevel("INFO")
    val sqlContext = new SQLContext(sc)

    // Paths
    val path = if (args.length > 0) args(0) else "/Users/panda"
    val amount = if (args.length > 1) args(1) else "0.1"
    val origTrainPath = s"$path/data/airline/train-${amount}m.csv"
    val origTestPath = s"$path/data/airline/test.csv"
    val newTrainPath = s"$path/data/airline/spark1hot-train-${amount}m.parquet"
    val newTestPath = s"$path/data/airline/spark1hot-test-${amount}m.parquet"

    val (trainDF, testDF) =
      DataIngestion(sqlContext, origTrainPath, origTestPath).withStringIndexer

    val train = trainDF.coalesce(16).cache()
    val test = testDF.cache()

    println(s"xusen, Partitions of train set is ${train.rdd.partitions.size}")
    train.show()

    val rfc = new RandomForestClassifier()
    val metrics = new BinaryClassificationEvaluator().setMetricName("areaUnderROC")

    val grid = new ParamGridBuilder()
      .addGrid(rfc.maxBins, Array(400))
      .addGrid(rfc.maxDepth, Array(10))
      .addGrid(rfc.impurity, Array("gini"))
      // .addGrid(rfc.featureSubsetStrategy, Array("all"))
      // .addGrid(rfc.numTrees, Array(50, 100, 250, 500))
      .baseOn(ParamPair(rfc.numTrees, 1))
      // .baseOn(ParamPair(rfc.maxMemoryInMB, 32))
      .baseOn(ParamPair(rfc.cacheNodeIds, true))
      // .addGrid(rfc.subsamplingRate, Array(0.68))
      .build()

    val cv = new CrossValidator()
      .setEstimator(rfc).setEvaluator(metrics).setEstimatorParamMaps(grid)

    val begin = System.nanoTime

    val model = cv.fit(train)

    val importance =
      model.bestModel.asInstanceOf[RandomForestClassificationModel].featureImportances

    val trees = model.bestModel.asInstanceOf[RandomForestClassificationModel].trees
    val weights = model.bestModel.asInstanceOf[RandomForestClassificationModel].treeWeights

    println(s"xusen, importance is $importance")
    println(s"xusen, trees are\n${trees.map(_.toDebugString).mkString("\n")}")
    println(s"xusen, tree weights are ${weights}")

    val elapsed = (System.nanoTime - begin) / 1e9
    println(s"Elapsed time for training is $elapsed")

    val testBegin = System.nanoTime
    println(s"AUC is ${metrics.evaluate(model.transform(test))}")
    val testElapsed = (System.nanoTime - testBegin) / 1e9
    println(s"Elapsed time for testing is $testElapsed")

    sc.stop()
  }
}
// scalastyle:on
