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
package io.snappydata.spark.gemfire.connector.streaming

import io.snappydata.spark.gemfire.connector.internal.rdd.{GemFirePairRDDWriter, GemFireRDDWriter}
import io.snappydata.spark.gemfire.connector.internal.DefaultGemFireConnectionManager

import org.apache.spark.Logging
import org.apache.spark.api.java.function.PairFunction
import org.apache.spark.streaming.dstream.DStream

/**
  * Extra geode functions on DStream of non-pair elements through an implicit conversion.
  * Import `org.apache.geode.spark.connector.streaming._` at the top of your program to
  * use these functions.
  */
class GemFireDStreamFunctions[T](val dstream: DStream[T]) extends Serializable with Logging {

  /** this version of saveToGemFire is just for Java API */
  def saveToGemFire[K, V](
      regionPath: String,
      func: PairFunction[T, K, V],
      opConf: Map[String, String]): Unit = {
    saveToGemFire[K, V](regionPath, func.call _, opConf)
  }

  /**
    * Save the DStream of non-pair elements to Geode key-value store.
    *
    * @param regionPath the full path of region that the DStream is stored
    * @param func       the function that converts elements of the DStream to key/value pairs
    * @param opConf     the optional parameters for this operation
    */
  def saveToGemFire[K, V](
      regionPath: String,
      func: T => (K, V),
      opConf: Map[String, String] = Map.empty): Unit = {
    DefaultGemFireConnectionManager.getConnection.validateRegion[K, V](regionPath)
    val writer = new GemFireRDDWriter[T, K, V](regionPath, opConf)
    logInfo(s"""Save DStream region=$regionPath conn=${DefaultGemFireConnectionManager.locators.mkString(",")}""")
    dstream.foreachRDD(rdd => rdd.sparkContext.runJob(rdd, writer.write(func) _))
  }


}


/**
  * Extra geode functions on DStream of (key, value) pairs through an implicit conversion.
  * Import `org.apache.geode.spark.connector.streaming._` at the top of your program to
  * use these functions.
  */
class GemFirePairDStreamFunctions[K, V](val dstream: DStream[(K, V)]) extends Serializable with Logging {

  /**
    * Save the DStream of pairs to Geode key-value store without any conversion
    *
    * @param regionPath the full path of region that the DStream is stored
    * @param opConf     the optional parameters for this operation
    */
  def saveToGemFire(
      regionPath: String,
      opConf: Map[String, String] = Map.empty): Unit = {
    DefaultGemFireConnectionManager.getConnection.validateRegion[K, V](regionPath)
    val writer = new GemFirePairRDDWriter[K, V](regionPath, opConf)
    logInfo(s"""Save DStream region=$regionPath conn=${DefaultGemFireConnectionManager.locators.mkString(",")}""")
    dstream.foreachRDD(rdd => rdd.sparkContext.runJob(rdd, writer.write _))
  }


}
