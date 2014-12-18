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

package org.apache.spark.mllib.tree

import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.{DataSchema, Algo, Strategy}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, SQLContext}
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkContext}

import scala.language.reflectiveCalls

/**
 * An example runner for decision trees and random forests. Run with
 * {{{
 * ./bin/run-example org.apache.spark.examples.mllib.DecisionTreeRunner [options]
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 *
 * Note: This script treats all features as real-valued (not categorical).
 *       To include categorical features, modify categoricalFeaturesInfo.
 */
object SecurityDecisionTree {

  object ImpurityType extends Enumeration {
    type ImpurityType = Value
    val Gini, Entropy, Variance = Value
  }

  import org.apache.spark.mllib.tree.SecurityDecisionTree.ImpurityType._

  case class Params(
      input: String = null,
      testInput: String = "",
      dataFormat: String = "libsvm",
      algo: Algo = Classification,
      maxDepth: Int = 5,
      impurity: ImpurityType = Gini,
      maxBins: Int = 32,
      minInstancesPerNode: Int = 1,
      minInfoGain: Double = 0.0,
      numTrees: Int = 1,
      featureSubsetStrategy: String = "auto",
      fracTest: Double = 0.2,
      useNodeIdCache: Boolean = false,
      checkpointDir: Option[String] = None,
      checkpointInterval: Int = 10)

  def main(args: Array[String]) {
    val params = Params()
    run(params)
  }

  /**
   * Load training and test data from files.
   * @param input  Path to input dataset.
   * @param testInput  Path to test dataset.
   * @param algo  Classification or Regression
   * @param fracTest  Fraction of input data to hold out for testing.  Ignored if testInput given.
   * @return  (training dataset, test dataset, number of classes),
   *          where the number of classes is inferred from data (and set to 0 for Regression)
   */
  private[mllib] def loadDatasets(
      sc: SparkContext,
      sqlCtx: SQLContext,
      input: SchemaRDD,
      table: String,
      schema: DataSchema,
      algo: Algo,
      fracTest: Double): (SchemaRDD, SchemaRDD, Map[Int, Int], Int) = {
    import sqlCtx._
    import schema._
    registerRDDAsTable(input, table)

    // ETL of label
    val (examples, classIndexMap, numClasses) = algo match {
      case Classification => {
        // classCounts: class --> # examples in class
        val classCounts = sql(s"SELECT $label, COUNT($label) FROM $table GROUP BY $label")
          .map(r => (r.getDouble(0), r.getLong(1))).collect().toMap
        val sortedClasses = classCounts.keys.toList.sorted
        val numClasses = classCounts.size
        // classIndexMap: class --> index in 0,...,numClasses-1
        val classIndexMap = {
          if (classCounts.keySet != Set(0.0, 1.0)) {
            sortedClasses.zipWithIndex.toMap
          } else {
            Map[Double, Int]()
          }
        }
        val reIndexLabel: Double => Double = label => classIndexMap(label).toDouble
        registerFunction("reIndexLabel", reIndexLabel)
        val examples = {
          if (classIndexMap.isEmpty) {
            input
          } else {
            sql(s"SELECT reIndexLabel($label), $featuresString FROM $table")
          }
        }
        val numExamples = examples.count()
        println(s"numClasses = $numClasses.")
        println(s"Per-class example fractions, counts:")
        println(s"Class\tFrac\tCount")
        sortedClasses.foreach { c =>
          val frac = classCounts(c) / numExamples.toDouble
          println(s"$c\t$frac\t${classCounts(c)}")
        }
        (examples, classIndexMap, numClasses)
      }
      case Regression =>
        (input, null, 0)
      case _ =>
        throw new IllegalArgumentException("Algo ${params.algo} not supported.")
    }
    // ETL of features
    ???
  }

  def run(params: Params) {

    val conf = new SparkConf().setAppName(s"DecisionTreeRunner with $params")
    val sc = new SparkContext(conf)
    val sqlCtx = new SQLContext(sc)

    println(s"DecisionTreeRunner with parameters:\n$params")

    // Load training and test data and cache it.
    val (training, test, numClasses) = loadDatasets(sc, params.input, params.dataFormat,
      params.testInput, params.algo, params.fracTest)

    val impurityCalculator = params.impurity match {
      case Gini => impurity.Gini
      case Entropy => impurity.Entropy
      case Variance => impurity.Variance
    }

    val strategy
      = new Strategy(
          algo = params.algo,
          impurity = impurityCalculator,
          maxDepth = params.maxDepth,
          maxBins = params.maxBins,
          numClasses = numClasses,
          minInstancesPerNode = params.minInstancesPerNode,
          minInfoGain = params.minInfoGain,
          useNodeIdCache = params.useNodeIdCache,
          checkpointDir = params.checkpointDir,
          checkpointInterval = params.checkpointInterval)

    if (params.numTrees == 1) {
      val startTime = System.nanoTime()
      val model = DecisionTree.train(training, strategy)
      val elapsedTime = (System.nanoTime() - startTime) / 1e9
      println(s"Training time: $elapsedTime seconds")
      if (model.numNodes < 20) {
        println(model.toDebugString) // Print full model.
      } else {
        println(model) // Print model summary.
      }
      if (params.algo == Classification) {
        val trainAccuracy =
          new MulticlassMetrics(training.map(lp => (model.predict(lp.features), lp.label)))
            .precision
        println(s"Train accuracy = $trainAccuracy")
        val testAccuracy =
          new MulticlassMetrics(test.map(lp => (model.predict(lp.features), lp.label))).precision
        println(s"Test accuracy = $testAccuracy")
      }
      if (params.algo == Regression) {
        val trainMSE = meanSquaredError(model, training)
        println(s"Train mean squared error = $trainMSE")
        val testMSE = meanSquaredError(model, test)
        println(s"Test mean squared error = $testMSE")
      }
    } else {
      val randomSeed = Utils.random.nextInt()
      if (params.algo == Classification) {
        val startTime = System.nanoTime()
        val model = RandomForest.trainClassifier(training, strategy, params.numTrees,
          params.featureSubsetStrategy, randomSeed)
        val elapsedTime = (System.nanoTime() - startTime) / 1e9
        println(s"Training time: $elapsedTime seconds")
        if (model.totalNumNodes < 30) {
          println(model.toDebugString) // Print full model.
        } else {
          println(model) // Print model summary.
        }
        val trainAccuracy =
          new MulticlassMetrics(training.map(lp => (model.predict(lp.features), lp.label)))
            .precision
        println(s"Train accuracy = $trainAccuracy")
        val testAccuracy =
          new MulticlassMetrics(test.map(lp => (model.predict(lp.features), lp.label))).precision
        println(s"Test accuracy = $testAccuracy")
      }
      if (params.algo == Regression) {
        val startTime = System.nanoTime()
        val model = RandomForest.trainRegressor(training, strategy, params.numTrees,
          params.featureSubsetStrategy, randomSeed)
        val elapsedTime = (System.nanoTime() - startTime) / 1e9
        println(s"Training time: $elapsedTime seconds")
        if (model.totalNumNodes < 30) {
          println(model.toDebugString) // Print full model.
        } else {
          println(model) // Print model summary.
        }
        val trainMSE = meanSquaredError(model, training)
        println(s"Train mean squared error = $trainMSE")
        val testMSE = meanSquaredError(model, test)
        println(s"Test mean squared error = $testMSE")
      }
    }

    sc.stop()
  }

  /**
   * Calculates the mean squared error for regression.
   */
  private[mllib] def meanSquaredError(
      model: { def predict(features: Vector): Double },
      data: RDD[LabeledPoint]): Double = {
    data.map { y =>
      val err = model.predict(y.features) - y.label
      err * err
    }.mean()
  }
}
