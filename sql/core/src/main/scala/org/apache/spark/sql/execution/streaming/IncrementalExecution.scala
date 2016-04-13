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

package org.apache.spark.sql.execution.streaming

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan, SparkPlanner, UnaryNode}

/**
 * A variant of [[QueryExecution]] that allows the execution of the given [[LogicalPlan]]
 * plan incrementally. Possibly preserving state in between each execution.
 */
class IncrementalExecution(
    ctx: SQLContext,
    logicalPlan: LogicalPlan,
    checkpointLocation: String,
    currentBatchId: Long) extends QueryExecution(ctx, logicalPlan) {

  // TODO: make this always part of planning.
  val stateStrategy = sqlContext.sessionState.planner.StatefulAggregationStrategy :: Nil

  // Modified planner with stateful operations.
  override def planner: SparkPlanner =
    new SparkPlanner(
      sqlContext.sparkContext,
      sqlContext.conf,
      stateStrategy)

  /**
   * Records the current id for a given stateful operator in the query plan as the `state`
   * preperation walks the query plan.
   */
  private var operatorId = 0

  /** Locates save/restore pairs surrounding aggregation. */
  val state = new Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = plan transform {
      case StateStoreSave(keys, None,
             UnaryNode(agg,
               StateStoreRestore(keys2, None, child))) =>
        val stateId = OperatorStateId(checkpointLocation, operatorId, currentBatchId - 1)
        operatorId += 1

        StateStoreSave(
          keys,
          Some(stateId),
          agg.withNewChildren(
            StateStoreRestore(
              keys,
              Some(stateId),
              child) :: Nil))
    }
  }

  override def preparations: Seq[Rule[SparkPlan]] = state +: super.preparations
}
