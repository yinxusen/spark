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
import org.apache.spark.ml.classification.{DecisionTreeClassifier, RandomForestClassifier}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.param.{ParamMap, ParamPair}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{col, lit}

object AirlineDT {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RFCAirline").setMaster("local[8]")
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

    // Use RFormula to generate training data.
    val res = new RFormula().setFormula("dep_delayed_15min ~ .").fit(fullDF).transform(fullDF)

    // Split back into train, test
    val finalTrainDF = res.where(col("isTrain"))
    val finalTestDF = res.where(!col("isTrain"))

    // Save Spark DataFrames as Parquet
    finalTrainDF.write.mode("overwrite").parquet(newTrainPath)
    finalTestDF.write.mode("overwrite").parquet(newTestPath)


    val train = finalTrainDF.cache()
    val test = finalTestDF.cache()
    train.show(10)

    val dtc = new DecisionTreeClassifier()
    val metrics = new BinaryClassificationEvaluator().setMetricName("areaUnderROC")

    val grid = new ParamGridBuilder()
      .addGrid(dtc.cacheNodeIds, Array(true, false))
      .addGrid(dtc.maxBins, Array(200, 2000, 20000))
      .addGrid(dtc.maxDepth, Array(3, 5, 10))
      .addGrid(dtc.impurity, Array("entropy", "gini"))
      .baseOn(ParamPair(dtc.maxMemoryInMB, 2048))
      .build()

    val begin = System.nanoTime

    val cv = new CrossValidator()
      .setEstimator(dtc).setEstimatorParamMaps(grid).setEvaluator(metrics)

    val model = cv.fit(train)

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
