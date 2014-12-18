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
import org.apache.spark.mllib.tree.configuration.{FeatureType, DataSchema, Algo, Strategy}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, SQLContext}
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.parallel.mutable
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
      input: SchemaRDD = null,
      table: String = null,
      schema: DataSchema = null,
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

    var categoricalFeaturesInfo = Map[Int, Int]()

    val reLabeledTable = "RELABELEDTABLE"
    registerRDDAsTable(examples, reLabeledTable)
    // ETL of features
    val queryStr = (features zip featureTypes).zipWithIndex.map { case ((column, fType), i) =>
      fType match {
        case FeatureType.Continuous =>
          column
        case FeatureType.Categorical =>
          val classCounts = sql(s"SELECT $column, COUNT($column) FROM $reLabeledTable GROUP BY $column")
            .map(r => (r.getDouble(0), r.getLong(1))).collect().toMap
          val numClasses = classCounts.size
          categoricalFeaturesInfo = categoricalFeaturesInfo.+((i, numClasses))
          val sortedClasses = classCounts.keys.toList.sorted
          val classIndexMap = sortedClasses.zipWithIndex.toMap
          val reIndexLabel: Double => Double = label => classIndexMap(label).toDouble
          registerFunction(s"reIndex$column", reIndexLabel)
          s"reIndex$column($column)"
        case _ =>
          throw new IllegalArgumentException(s"Feature types $fType not supported.")
      }
    }

    val res = sql(s"SELECT $label, ${queryStr.mkString(", ")} FROM $reLabeledTable")
    val splits = res.randomSplit(Array(1.0 - fracTest, fracTest))
    val trainSet = splits(0)
    val testSet = splits(1)
    (trainSet.asInstanceOf[SchemaRDD], testSet.asInstanceOf[SchemaRDD], categoricalFeaturesInfo, numClasses)
  }

  def run(params: Params) {

    val conf = new SparkConf().setAppName(s"DecisionTreeRunner with $params")
    val sc = new SparkContext(conf)
    val sqlCtx = new SQLContext(sc)

    println(s"DecisionTreeRunner with parameters:\n$params")

    // Load training and test data and cache it.
    val (training, test, categoricalFeatureInfo, numClasses) = loadDatasets(
      sc, sqlCtx, params.input, params.table, params.schema, params.algo, params.fracTest)

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
          checkpointInterval = params.checkpointInterval,
          categoricalFeaturesInfo = categoricalFeatureInfo)

    if (params.numTrees == 1) {
      val startTime = System.nanoTime()
      val randomSeed = Utils.random.nextInt()
      val model = new RandomForest(strategy, params.numTrees, params.featureSubsetStrategy, randomSeed)
      model.run(training, params.schema.features, sqlCtx)
      val elapsedTime = (System.nanoTime() - startTime) / 1e9
      if (model.totalNumNodes < 30) {
        println(model.toDebugString) // Print full model.
      } else {
        println(model) // Print model summary.
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
