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

package org.apache.spark.graphx.lib.community

class VertexData() extends Serializable{
  var degree: Long = 0
  var community: Long = 0
  var communityWeight: Long =  -1
  var neighDegree: Long = 0
  var neighCommunity: Long = -1
  var neighCommunityWeight: Long = -1

  def this(communityValue: Long) {
    this()
    community = communityValue
  }

  def this(other: VertexData) {
    this()
    degree = other.degree
    community = other.community
    communityWeight = other.communityWeight
    neighDegree = other.neighDegree
    neighCommunity = other.neighCommunity
    neighCommunityWeight = other.neighCommunityWeight
  }

  def setDegreeAndCommWeight(degreeValue: Long): VertexData = {
    degree = degreeValue
    communityWeight = degreeValue
    new VertexData(this)
  }

  def setCommAndCommWeight(commValue: Long, commWeightValue: Long): VertexData = {
    community = commValue
    communityWeight = commWeightValue
    new VertexData(this)
  }

  def setNeighbor(degreeValue: Long, commValue: Long, commWeightValue: Long): VertexData = {
    neighDegree = degreeValue
    neighCommunity = commValue
    neighCommunityWeight = commWeightValue
    new VertexData(this)
  }
}