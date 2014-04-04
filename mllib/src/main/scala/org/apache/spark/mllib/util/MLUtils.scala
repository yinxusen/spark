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

package org.apache.spark.mllib.util

import breeze.linalg.{Vector => BV, SparseVector => BSV, squaredDistance => breezeSquaredDistance}

import org.apache.spark.annotation.Experimental
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

import org.apache.spark.mllib.clustering.Document
import breeze.util.Index
import chalk.text.tokenize.JavaWordTokenizer
import scala.collection.mutable.ArrayBuffer

/**
 * Helper methods to load, save and pre-process data used in ML Lib.
 */
object MLUtils {

  private[util] lazy val EPSILON = {
    var eps = 1.0
    while ((1.0 + (eps / 2.0)) != 1.0) {
      eps /= 2.0
    }
    eps
  }

  /**
   * Loads labeled data in the LIBSVM format into an RDD[LabeledPoint].
   * The LIBSVM format is a text-based format used by LIBSVM and LIBLINEAR.
   * Each line represents a labeled sparse feature vector using the following format:
   * {{{label index1:value1 index2:value2 ...}}}
   * where the indices are one-based and in ascending order.
   * This method parses each line into a [[org.apache.spark.mllib.regression.LabeledPoint]],
   * where the feature indices are converted to zero-based.
   *
   * @param sc Spark context
   * @param path file or directory path in any Hadoop-supported file system URI
   * @param labelParser parser for labels, default: 1.0 if label > 0.5 or 0.0 otherwise
   * @param numFeatures number of features, which will be determined from the input data if a
   *                    negative value is given. The default value is -1.
   * @param minSplits min number of partitions, default: sc.defaultMinSplits
   * @return labeled data stored as an RDD[LabeledPoint]
   */
  def loadLibSVMData(
      sc: SparkContext,
      path: String,
      labelParser: LabelParser,
      numFeatures: Int,
      minSplits: Int): RDD[LabeledPoint] = {
    val parsed = sc.textFile(path, minSplits)
      .map(_.trim)
      .filter(!_.isEmpty)
      .map(_.split(' '))
    // Determine number of features.
    val d = if (numFeatures >= 0) {
      numFeatures
    } else {
      parsed.map { items =>
        if (items.length > 1) {
          items.last.split(':')(0).toInt
        } else {
          0
        }
      }.reduce(math.max)
    }
    parsed.map { items =>
      val label = labelParser.parse(items.head)
      val (indices, values) = items.tail.map { item =>
        val indexAndValue = item.split(':')
        val index = indexAndValue(0).toInt - 1
        val value = indexAndValue(1).toDouble
        (index, value)
      }.unzip
      LabeledPoint(label, Vectors.sparse(d, indices.toArray, values.toArray))
    }
  }

  // Convenient methods for calling from Java.

  /**
   * Loads binary labeled data in the LIBSVM format into an RDD[LabeledPoint],
   * with number of features determined automatically and the default number of partitions.
   */
  def loadLibSVMData(sc: SparkContext, path: String): RDD[LabeledPoint] =
    loadLibSVMData(sc, path, BinaryLabelParser, -1, sc.defaultMinSplits)

  /**
   * Loads labeled data in the LIBSVM format into an RDD[LabeledPoint],
   * with the given label parser, number of features determined automatically,
   * and the default number of partitions.
   */
  def loadLibSVMData(
      sc: SparkContext,
      path: String,
      labelParser: LabelParser): RDD[LabeledPoint] =
    loadLibSVMData(sc, path, labelParser, -1, sc.defaultMinSplits)

  /**
   * Loads labeled data in the LIBSVM format into an RDD[LabeledPoint],
   * with the given label parser, number of features specified explicitly,
   * and the default number of partitions.
   */
  def loadLibSVMData(
      sc: SparkContext,
      path: String,
      labelParser: LabelParser,
      numFeatures: Int): RDD[LabeledPoint] =
    loadLibSVMData(sc, path, labelParser, numFeatures, sc.defaultMinSplits)

  /**
   * :: Experimental ::
   * Load labeled data from a file. The data format used here is
   * <L>, <f1> <f2> ...
   * where <f1>, <f2> are feature values in Double and <L> is the corresponding label as Double.
   *
   * @param sc SparkContext
   * @param dir Directory to the input data files.
   * @return An RDD of LabeledPoint. Each labeled point has two elements: the first element is
   *         the label, and the second element represents the feature values (an array of Double).
   */
  @Experimental
  def loadLabeledData(sc: SparkContext, dir: String): RDD[LabeledPoint] = {
    sc.textFile(dir).map { line =>
      val parts = line.split(',')
      val label = parts(0).toDouble
      val features = Vectors.dense(parts(1).trim().split(' ').map(_.toDouble))
      LabeledPoint(label, features)
    }
  }

  /**
   * :: Experimental ::
   * Save labeled data to a file. The data format used here is
   * <L>, <f1> <f2> ...
   * where <f1>, <f2> are feature values in Double and <L> is the corresponding label as Double.
   *
   * @param data An RDD of LabeledPoints containing data to be saved.
   * @param dir Directory to save the data.
   */
  @Experimental
  def saveLabeledData(data: RDD[LabeledPoint], dir: String) {
    val dataStr = data.map(x => x.label + "," + x.features.toArray.mkString(" "))
    dataStr.saveAsTextFile(dir)
  }

  /**
   * Returns the squared Euclidean distance between two vectors. The following formula will be used
   * if it does not introduce too much numerical error:
   * <pre>
   *   \|a - b\|_2^2 = \|a\|_2^2 + \|b\|_2^2 - 2 a^T b.
   * </pre>
   * When both vector norms are given, this is faster than computing the squared distance directly,
   * especially when one of the vectors is a sparse vector.
   *
   * @param v1 the first vector
   * @param norm1 the norm of the first vector, non-negative
   * @param v2 the second vector
   * @param norm2 the norm of the second vector, non-negative
   * @param precision desired relative precision for the squared distance
   * @return squared distance between v1 and v2 within the specified precision
   */
  private[mllib] def fastSquaredDistance(
      v1: BV[Double],
      norm1: Double,
      v2: BV[Double],
      norm2: Double,
      precision: Double = 1e-6): Double = {
    val n = v1.size
    require(v2.size == n)
    require(norm1 >= 0.0 && norm2 >= 0.0)
    val sumSquaredNorm = norm1 * norm1 + norm2 * norm2
    val normDiff = norm1 - norm2
    var sqDist = 0.0
    /*
     * The relative error is
     * <pre>
     * EPSILON * ( \|a\|_2^2 + \|b\\_2^2 + 2 |a^T b|) / ( \|a - b\|_2^2 ),
     * </pre>
     * which is bounded by
     * <pre>
     * 2.0 * EPSILON * ( \|a\|_2^2 + \|b\|_2^2 ) / ( (\|a\|_2 - \|b\|_2)^2 ).
     * </pre>
     * The bound doesn't need the inner product, so we can use it as a sufficient condition to
     * check quickly whether the inner product approach is accurate.
     */
    val precisionBound1 = 2.0 * EPSILON * sumSquaredNorm / (normDiff * normDiff + EPSILON)
    if (precisionBound1 < precision) {
      sqDist = sumSquaredNorm - 2.0 * v1.dot(v2)
    } else if (v1.isInstanceOf[BSV[Double]] || v2.isInstanceOf[BSV[Double]]) {
      val dot = v1.dot(v2)
      sqDist = math.max(sumSquaredNorm - 2.0 * dot, 0.0)
      val precisionBound2 = EPSILON * (sumSquaredNorm + 2.0 * math.abs(dot)) / (sqDist + EPSILON)
      if (precisionBound2 > precision) {
        sqDist = breezeSquaredDistance(v1, v2)
      }
    } else {
      sqDist = breezeSquaredDistance(v1, v2)
    }
    sqDist
  }

  def splitNameAndContent(nameAndContent: String) : (String, String) = {
    val pos = nameAndContent.indexOf(',')
    assert(pos != -1)
    nameAndContent.splitAt(pos)
  }

  def loadCorpus(
                  sc: SparkContext,
                  dir: String,
                  miniSplit: Int,
                  dirStopWords: String = "./english.stop.txt"):
  (RDD[Document], Index[String], Index[String]) = {

    val wordMap = Index[String]()
    val docMap = Index[String]()

    val almostData = sc.textFile(dir, miniSplit).cache()

    val stopWords = sc.textFile(dirStopWords, miniSplit).
      map(x => x.replaceAll("""(?m)\s+$""", "")).distinct.collect.toSet

    val broadcastStopWord = sc.broadcast(stopWords)

    almostData.map { line =>
      val (fileName, _) = splitNameAndContent(line)
      fileName
    }.distinct.collect.map(x => docMap.index(x))

    almostData.flatMap { line =>
      val (_, content) = splitNameAndContent(line)
      JavaWordTokenizer(content)
        .filter(x => x(0).isLetter && ! broadcastStopWord.value.contains(x))
    }.distinct.collect.map(x => wordMap.index(x))

    println(wordMap.size)
    println(docMap.size)

    val broadcastWordMap = sc.broadcast(wordMap)
    val broadcastDocMap = sc.broadcast(docMap)

    val data = almostData.map { line =>
      val splitVersion = splitNameAndContent(line)
      val fileIdx = broadcastDocMap.value.index(splitVersion._1)
      val content = new ArrayBuffer[Int]
      for (token <- JavaWordTokenizer(splitVersion._2)
           if (token(0).isLetter && ! broadcastStopWord.value.contains(token))) {
        content.append(broadcastWordMap.value.index(token))
      }
      Document(fileIdx, content.toArray)
    }
    (data, wordMap, docMap)
  }
}
