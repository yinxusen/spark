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

import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.FeatureType._
import org.apache.spark.mllib.tree.configuration.{FeatureType, DataSchema, Strategy}
import org.apache.spark.mllib.tree.model.{Split, DecisionTreeModel, Node}
import org.apache.spark.sql.{SchemaRDD, SQLContext}
import org.apache.spark.util.Utils
import org.apache.spark.SparkContext
import org.apache.spark.Logging

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
object SecurityDecisionTree extends Logging {

  object ImpurityType extends Enumeration {
    type ImpurityType = Value
    val Gini, Entropy, Variance = Value
  }

  import ImpurityType._

  case class Params(
      input: SchemaRDD = null,
      table: String = null,
      schema: DataSchema = null,
      algorithm: Algo = Classification,
      maxDepth: Int = 5,
      impurity: ImpurityType = Gini,
      maxBins: Int = 32,
      minInstancesPerNode: Int = 1,
      minInfoGain: Double = 0.0,
      numTrees: Int = 1,
      featureSubsetStrategy: String = "auto",
      fractionTest: Double = 0.2,
      useNodeIdCache: Boolean = false,
      checkpointDir: Option[String] = None,
      checkpointInterval: Int = 10
  )

  /**
   * Load training and test data from files.
   */
  private[mllib] def loadData(sqlCtx: SQLContext, input: SchemaRDD, tableName: String,
                              schema: DataSchema, algorithm: Algo, fractionTest: Double):
  (SchemaRDD, SchemaRDD, Map[Int, Int], Int, Map[Any, Int], Map[Int, Map[Any, Int]]) = {
    import sqlCtx._
    import schema._

    registerRDDAsTable(input, tableName)

    // ETL of label
    val (examples, labelClassIndexMap, numClasses) = algorithm match {
      case Classification =>
        logInfo(s"Counting classes info of $label in table $tableName")
        val classCounts = sql(s"SELECT $label, COUNT($label) FROM $tableName GROUP BY $label")
          .map(r => (r(0), r.getLong(1))).collect().toMap
        val classLabels = classCounts.keys.toList
        val numClasses = classCounts.size
        val classIndexMap = classLabels.zipWithIndex.toMap
        val reIndexLabel: Any => Double = label => classIndexMap(label).toDouble
        registerFunction("reIndexLabel", reIndexLabel)
        val examples =
          sql(s"SELECT reIndexLabel($label) as $label, $featuresString FROM $tableName")
        val numExamples = examples.count()
        println(s"numClasses = $numClasses.")
        println(s"Per-class example fractions, counts:")
        println(s"Class\tFraction\tCount")
        classLabels.foreach { c =>
          val fraction = classCounts(c) / numExamples.toDouble
          println(s"$c\t$fraction\t${classCounts(c)}")
        }
        (examples, classIndexMap, numClasses)
      case Regression =>
        (input, null, 0)
      case _ =>
        throw new IllegalArgumentException(s"Algorithm $algorithm not supported.")
    }

    var categoricalFeaturesInfo = Map[Int, Int]()
    var featureClassIndexMaps = Map[Int, Map[Any, Int]]()

    val relabeledTableName = s"reLabeled$tableName"
    registerRDDAsTable(examples, relabeledTableName)

    val intToDouble: (Int) => Double = (x) => x.toDouble
    registerFunction("IntToDouble", intToDouble)

    // ETL of features
    val queryStr = (features zip featureTypes).zipWithIndex.map { case ((column, fType), i) =>
      fType match {
        case FeatureType.Continuous =>
          s"IntToDouble($column) as $column"
        case FeatureType.Categorical =>
          logInfo(s"Counting classes info of $column in table $relabeledTableName")
          val classCounts =
            sql(s"SELECT $column, COUNT($column) FROM $relabeledTableName GROUP BY $column")
              .map(r => (r(0), r.getLong(1))).collect().toMap
          val numClasses = classCounts.size
          categoricalFeaturesInfo += ((i, numClasses))
          val classLabels = classCounts.keys.toList
          val classIndexMap = classLabels.zipWithIndex.toMap
          featureClassIndexMaps += ((i, classIndexMap))
          val reIndexLabel: Any => Double = label => classIndexMap(label).toDouble
          registerFunction(s"reIndex$column", reIndexLabel)
          s"reIndex$column($column) as $column"
        case _ =>
          throw new IllegalArgumentException(s"Feature types $fType not supported.")
      }
    }

    val preparedDataSql = s"SELECT $label, ${queryStr.mkString(", ")} FROM $relabeledTableName"
    logInfo(s"Query prepared dataset with SQL: \n$preparedDataSql")
    val res = sql(preparedDataSql)
    val splits = res.splitTrainAndTest(Array(1.0 - fractionTest, fractionTest))
    val trainSet = splits(0)
    val testSet = splits(1)
    (trainSet, testSet, categoricalFeaturesInfo, numClasses, labelClassIndexMap,
      featureClassIndexMaps)
  }

  def run(params: Params, sc: SparkContext, sqlCtx: SQLContext) {

    logInfo(s"Begin load data from ${params.table}")
    val (train, test, catFeatureInfo, numClasses, labelMap, featureMaps) = loadData(
      sqlCtx, params.input, params.table, params.schema, params.algorithm, params.fractionTest)
    logInfo(s"End load data from ${params.table}")

    val impurityCalculator = params.impurity match {
      case Gini => impurity.Gini
      case Entropy => impurity.Entropy
      case Variance => impurity.Variance
    }

    val strategy = new Strategy(
      algo = params.algorithm,
      impurity = impurityCalculator,
      maxDepth = params.maxDepth,
      maxBins = params.maxBins,
      numClasses = numClasses,
      minInstancesPerNode = params.minInstancesPerNode,
      minInfoGain = params.minInfoGain,
      useNodeIdCache = params.useNodeIdCache,
      checkpointDir = params.checkpointDir,
      checkpointInterval = params.checkpointInterval,
      categoricalFeaturesInfo = catFeatureInfo)

    if (params.numTrees == 1) {
      val startTime = System.nanoTime()
      val randomSeed = Utils.random.nextInt()
      val model = new RandomForest(
        strategy, params.numTrees, params.featureSubsetStrategy, randomSeed)
      logInfo(s"Begin training model with parameters: \n$params")
      val resModel = model.run(train, params.schema.label, params.schema.features, sqlCtx)
      val totalTime = (System.nanoTime() - startTime) / 1e9
      logInfo(s"End training model, total time cost: $totalTime")
      resModel.trees.foreach(t =>
        println(parseTreeToString(t, params.schema.features, labelMap, featureMaps)))
    } else { ??? }
    sc.stop()
  }

  private[mllib] def parseTreeToString(
      model: DecisionTreeModel,
      columnNames: Array[String],
      labelMap: Map[Any, Int],
      featureMaps: Map[Int, Map[Any, Int]]): String = {
    val header = model.toString
    header + "\n" + subtreeToString(model.topNode, labelMap, columnNames, featureMaps, 4)
  }

  private def reverseMap(ori: Map[Any, Int]): Map[Int, Any] = {
    ori.toList.map(x => (x._2, x._1)).toMap
  }

  private[mllib] def subtreeToString(
      node: Node,
      labelMap: Map[Any, Int],
      columnNames: Array[String],
      featureMaps: Map[Int, Map[Any, Int]],
      indentFactor: Int = 0): String = {

    val getLabel = reverseMap(labelMap)
    val getFeatures = featureMaps.mapValues(reverseMap)

    def splitToString(split: Split, left: Boolean): String = {
      split.featureType match {
        case Continuous => if (left) {
          s"(feature ${columnNames(split.feature)} <= ${split.threshold})"
        } else {
          s"(feature ${columnNames(split.feature)} > ${split.threshold})"
        }
        case Categorical => if (left) {
          s"(feature ${columnNames(split.feature)} in ${split.categories
            .map(x => getFeatures(split.feature)(x.toInt)).mkString("{",",","}")})"
        } else {
          s"(feature ${columnNames(split.feature)} not in ${split.categories
            .map(x => getFeatures(split.feature)(x.toInt)).mkString("{",",","}")})"
        }
      }
    }
    val prefix: String = " " * indentFactor
    if (node.isLeaf) {
      prefix + s"Predict: ${getLabel(node.predict.predict.toInt)}\n"
    } else {
      prefix + s"If ${splitToString(node.split.get, left=true)}\n" +
        subtreeToString(node.leftNode.get, labelMap, columnNames, featureMaps, indentFactor + 4) +
        prefix + s"Else ${splitToString(node.split.get, left=false)}\n" +
        subtreeToString(node.rightNode.get, labelMap, columnNames, featureMaps, indentFactor + 4)
    }
  }
}
