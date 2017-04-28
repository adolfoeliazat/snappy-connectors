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

import scala.reflect.ClassTag

import io.snappydata.spark.gemfire.connector.internal.rdd.GemFireRegionRDD

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/** Provides Geode specific methods on `SparkContext` */
class GemFireSparkContextFunctions(@transient sc: SparkContext) extends Serializable {

  /**
    * Expose a Geode region as a GeodeRDD
    *
    * @param regionPath the full path of the region
    * @param opConf     use this to specify preferred partitioner
    *                   and its parameters. The implementation will use it if it's applicable
    */
  def gemfireRegion[K: ClassTag, V: ClassTag](
      regionPath: String,
      opConf: Map[String, String] = Map.empty): RDD[(K, V)] =
  GemFireRegionRDD.exposeRegion[K, V](sc, regionPath, opConf)

}
