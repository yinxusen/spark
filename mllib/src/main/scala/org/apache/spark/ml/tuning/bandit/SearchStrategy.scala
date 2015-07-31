package org.apache.spark.ml.tuning.bandit

import scala.collection.mutable

import org.apache.spark.sql.DataFrame

/**
 * Created by panda on 7/31/15.
 */

case class Key(dataName: String, numArms: Int, maxIter: Int, trial: Int)

class SearchStrategy(val name: String, val allResults: mutable.Map[Key, Arm] = Map.empty) {

  def appendResults(dataName: String, trial: Int, arms: Array[Arm], maxIter: Int, numArms: Int) = {

  }

  def search(modelFamilies: String, maxIter: Int, data: DataFrame, arms: Map[Key, Arm]): Arm = {
    ???
  }
}

class StaticSearchStrategy(override val name: String, override val allResults: mutable.Map[Key, Arm]) extends SearchStrategy(name, allResults) {
  override def search(modelFamilies: String, maxIter: Int, data: DataFrame, arms: Map[Key, Arm]): Arm = {
    assert(arms.keys.size != 0, "ERROR: No arms!")
    val numArms = arms.keys.size
    var i = 0
    while (i  < maxIter) {
      arms.values(i % numArms).pullArm()
      i += 1
    }
  }
}

