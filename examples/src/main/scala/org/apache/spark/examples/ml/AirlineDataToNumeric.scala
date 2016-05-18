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

// scalastyle:off println
package org.apache.spark.examples.ml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.{DecisionTreeClassifier, RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{RFormula, StringIndexer}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.param.ParamPair
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{col, lit}

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

    // Read CSV as Spark DataFrames
    val loader = sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
    val trainDF = loader.load(origTrainPath)
    val testDF = loader.load(origTestPath)

    // Combine train, test temporarily
    val fullDF = trainDF.withColumn("isTrain", lit(true))
      .union(testDF.withColumn("isTrain", lit(false)))

    val si = new StringIndexer()
    val cols = fullDF.columns.filterNot(_ == "isTrain")
    var finalDF = fullDF
    val newCols = cols.map { colName =>
      val newColName = colName + "_numeric"
      finalDF = si.setInputCol(colName).setOutputCol(newColName).fit(finalDF).transform(finalDF)
      newColName
    }


    val newData = finalDF.select("isTrain", newCols: _*)

    val va = new VectorAssembler()
      .setInputCols(newCols.filterNot(_ == "dep_delayed_15min_numeric")).setOutputCol("features")

    val res = va.transform(newData).select("features", "dep_delayed_15min_numeric")
      .withColumn("label", col("dep_delayed_15min_numeric")).drop(col("dep_delayed_15min_numeric"))

    // Split back into train, test
    val finalTrainDF = res.where(col("isTrain")).coalesce(16)
    val finalTestDF = res.where(!col("isTrain"))


    val train = finalTrainDF.cache()
    val test = finalTestDF.cache()
    println(s"xusen, Partitions of train set is ${train.rdd.partitions.size}")

    train.show()

    val rfc = new RandomForestClassifier()
    val metrics = new BinaryClassificationEvaluator().setMetricName("areaUnderROC")

    val grid = new ParamGridBuilder()
      .addGrid(rfc.maxBins, Array(2000))
      .addGrid(rfc.maxDepth, Array(5))
      .addGrid(rfc.impurity, Array("gini"))
      // .addGrid(rfc.featureSubsetStrategy, Array("all"))
      // .addGrid(rfc.numTrees, Array(50, 100, 250, 500))
      .baseOn(ParamPair(rfc.numTrees, 500))
      .baseOn(ParamPair(rfc.maxMemoryInMB, 32))
      .addGrid(rfc.subsamplingRate, Array(1.0))
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
// scalastyle:on println
