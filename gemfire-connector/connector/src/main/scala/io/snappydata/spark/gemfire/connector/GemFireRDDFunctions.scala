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
package io.snappydata.spark.gemfire.connector

import io.snappydata.spark.gemfire.connector.internal.rdd.{GemFireJoinRDD, GemFireOuterJoinRDD, GemFireRDDWriter}
import io.snappydata.spark.gemfire.connector.internal.DefaultGemFireConnectionManager

import org.apache.spark.Logging
import org.apache.spark.api.java.function.{Function, PairFunction}
import org.apache.spark.rdd.RDD

/**
  * Extra gemFire functions on non-Pair RDDs through an implicit conversion.
  * Import `org.apache.geode.spark.connector._` at the top of your program to
  * use these functions.
  */
class GemFireRDDFunctions[T](val rdd: RDD[T]) extends Serializable with Logging {

  /** This version of saveToGemFire(...) is just for Java API. */
  private[connector] def saveToGemFire[K, V](
      regionPath: String,
      func: PairFunction[T, K, V],
      opConf: Map[String, String]): Unit = {
    saveToGemFire[K, V](regionPath, func.call _, opConf)
  }

  /**
    * Save the non-pair RDD to Geode key-value store.
    *
    * @param regionPath the full path of region that the RDD is stored
    * @param func       the function that converts elements of RDD to key/value pairs
    * @param opConf     the optional parameters for this operation
    */
  def saveToGemFire[K, V](
      regionPath: String,
      func: T => (K, V),

      opConf: Map[String, String] = Map.empty): Unit = {
    val geodeConn = DefaultGemFireConnectionManager.getConnection
    geodeConn.validateRegion[K, V](regionPath)
    if (log.isDebugEnabled)
      logDebug(s"""Save RDD id=${rdd.id} to region $regionPath, partitions:\n  ${getRddPartitionsInfo(rdd)}""")
    else
      logInfo(s"""Save RDD id=${rdd.id} to region $regionPath""")
    val writer = new GemFireRDDWriter[T, K, V](regionPath, opConf)
    rdd.sparkContext.runJob(rdd, writer.write(func) _)
  }

  /** This version of joinGeodeRegion(...) is just for Java API. */
  private[connector] def joinGemFireRegion[K, V](
      regionPath: String, func: Function[T, K]): GemFireJoinRDD[T, K, V] = {
    joinGemFireRegion(regionPath, func.call _)
  }

  /**
    * Return an RDD containing all pairs of elements with matching keys in `this` RDD
    * and the Geode `Region[K, V]`. The join key from RDD element is generated by
    * `func(T) => K`, and the key from the Geode region is just the key of the
    * key/value pair.
    *
    * Each pair of elements of result RDD will be returned as a (t, v) tuple,
    * where (t) is in `this` RDD and (k, v) is in the Geode region.
    *
    * @param regionPath the region path of the Geode region
    * @param func       the function that generate region key from RDD element T
    * @tparam K the key type of the Geode region
    * @tparam V the value type of the Geode region
    * @return RDD[T, V]
    */
  def joinGemFireRegion[K, V](regionPath: String, func: T => K): GemFireJoinRDD[T, K, V] = {
    new GemFireJoinRDD[T, K, V](rdd, func, regionPath)
  }

  /** This version of outerJoinGeodeRegion(...) is just for Java API. */
  private[connector] def outerJoinGemFireRegion[K, V](
      regionPath: String, func: Function[T, K]): GemFireOuterJoinRDD[T, K, V] = {
    outerJoinGemFireRegion(regionPath, func.call _)
  }

  /**
    * Perform a left outer join of `this` RDD and the Geode `Region[K, V]`.
    * The join key from RDD element is generated by `func(T) => K`, and the
    * key from region is just the key of the key/value pair.
    *
    * For each element (t) in `this` RDD, the resulting RDD will either contain
    * all pairs (t, Some(v)) for v in the Geode region, or the pair
    * (t, None) if no element in the Geode region have key `func(t)`
    *
    * @param regionPath the region path of the Geode region
    * @param func       the function that generate region key from RDD element T
    * @tparam K the key type of the Geode region
    * @tparam V the value type of the Geode region
    * @return RDD[ T, Option[V] ]
    */
  def outerJoinGemFireRegion[K, V](regionPath: String, func: T => K): GemFireOuterJoinRDD[T, K, V] = {
    new GemFireOuterJoinRDD[T, K, V](rdd, func, regionPath)
  }


}


