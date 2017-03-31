/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package io.snappydata.spark.gemfire.connector.internal.rdd

import scala.collection.JavaConversions._

import com.gemstone.gemfire.cache.Region
import io.snappydata.spark.gemfire.connector.internal.DefaultGemFireConnectionManager

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

/**
  * An `RDD[ T, Option[V] ]` that represents the result of a left outer join
  * between `left` RDD[T] and the specified Geode Region[K, V].
  */
class GemFireOuterJoinRDD[T, K, V] private[connector]
(left: RDD[T],
    func: T => K,
    val regionPath: String
) extends RDD[(T, Option[V])](left.context, left.dependencies) {

  /** validate region existence when GeodeRDD object is created */
  validate()

  override def compute(split: Partition, context: TaskContext): Iterator[(T, Option[V])] = {
    val region = DefaultGemFireConnectionManager.getConnection.getRegionProxy[K, V](regionPath)
    if (func == null) computeWithoutFunc(split, context, region)
    else computeWithFunc(split, context, region)
  }

  /** T is (K1, V1), and K1 and K are the same type since there's no map function `func` */
  private def computeWithoutFunc(split: Partition, context: TaskContext, region: Region[K, V]): Iterator[(T, Option[V])] = {
    val leftPairs = left.iterator(split, context).toList.asInstanceOf[List[(K, _)]]
    val leftKeys = leftPairs.map { case (k, v) => k }.toSet
    // Note: get all will return (key, null) for non-exist entry
    val rightPairs = region.getAll(leftKeys)
    // rightPairs is a java.util.Map, not scala map, so need to convert map.get() to Option
    leftPairs.map { case (k, v) => ((k, v).asInstanceOf[T], Option(rightPairs.get(k))) }.toIterator
  }

  private def computeWithFunc(split: Partition, context: TaskContext, region: Region[K, V]): Iterator[(T, Option[V])] = {
    val leftPairs = left.iterator(split, context).toList.map(t => (t, func(t)))
    val leftKeys = leftPairs.map { case (t, k) => k }.toSet
    // Note: get all will return (key, null) for non-exist entry
    val rightPairs = region.getAll(leftKeys)
    // rightPairs is a java.util.Map, not scala map, so need to convert map.get() to Option
    leftPairs.map { case (t, k) => (t, Option(rightPairs.get(k))) }.toIterator
  }

  override protected def getPartitions: Array[Partition] = left.partitions

  /** Validate region, and make sure it exists. */
  private def validate(): Unit = DefaultGemFireConnectionManager.getConnection.validateRegion[K, V](regionPath)
}

