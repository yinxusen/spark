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

package org.apache.spark.examples.ml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{col, lit}

object preProcessAirline {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PolynomialExpansionExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("OFF")
    val sqlContext = new SQLContext(sc)

    // Paths
    val origTrainPath = "/Users/panda/data/airline/train-1m.csv"
    val origTestPath = "/Users/panda/data/airline/test.csv"
    val newTrainPath = "/Users/panda/data/airline/spark1hot-train-10m.parquet"
    val newTestPath = "/Users/panda/data/airline/spark1hot-test-10m.parquet"

    // Read CSV as Spark DataFrames
    val loader = sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
    val trainDF = loader.load(origTrainPath)
    val testDF = loader.load(origTestPath)

    // Combine train, test temporarily
    val fullDF = trainDF.withColumn("isTrain", lit(true))
      .unionAll(testDF.withColumn("isTrain", lit(false)))

    val res = new RFormula().setFormula("dep_delayed_15min ~ .").fit(fullDF).transform(fullDF)


    // Split back into train, test
    val finalTrainDF = res.where(col("isTrain")).cache()
    val finalTestDF = res.where(!col("isTrain")).cache()

    // Save Spark DataFrames as Parquet
    // finalTrainDF.write.mode("overwrite").parquet(newTrainPath)
    // finalTestDF.write.mode("overwrite").parquet(newTestPath)

    finalTrainDF.show(10)
    finalTrainDF.show(10)

    // Train model
    val numTrees = 100
    val featureSubsetStrategy = "sqrt"
    val impurity = "entropy"
    val maxDepth = 20
    val maxBins = 100

    val now = System.nanoTime
    val model = new RandomForestClassifier()
      .setNumTrees(numTrees)
      .setFeatureSubsetStrategy(featureSubsetStrategy)
      .setImpurity(impurity)
      .setMaxDepth(maxDepth)
      .setMaxBins(maxBins)
      .fit(finalTrainDF)

    val elapsed = (System.nanoTime - now) / 1e9

    val auc = new BinaryClassificationEvaluator().setMetricName("areaUnderROC")
      .evaluate(model.transform(finalTestDF))

    // scalastyle:off println
    println(s"Elapsed time for training is $elapsed.")
    println(s"AUC is $auc.")
    // scalastyle:on println

    sc.stop()
  }
}
