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

import scala.reflect.ClassTag

import io.snappydata.spark.gemfire.connector.GemFireConnection
import io.snappydata.spark.gemfire.connector.internal.gemfirefunctions.shared.RegionMetadata

import org.apache.spark.{Logging, Partition, SparkContext}

/**
  * A GeodeRDD partitioner is used to partition the region into multiple RDD partitions.
  */
trait GemFireRDDPartitioner extends Serializable {

  def name: String

  /** the function that generates partitions */
  def partitions (conn: GemFireConnection, md: RegionMetadata,
      env: Map[String, String]): Array[Partition]
}

object GemFireRDDPartitioner extends Logging {

  /** To add new partitioner, just add it to the following list */
  final val partitioners: Map[String, GemFireRDDPartitioner] =
  List(OnePartitionPartitioner, ServerSplitsPartitioner).map(e => (e.name, e)).toMap
  val defaultReplicatedRegionPartitioner = OnePartitionPartitioner
  val defaultPartitionedRegionPartitioner = ServerSplitsPartitioner

  /**
    * Get a partitioner based on given name, a default partitioner will be returned if there's
    * no partitioner for the given name.
    */
  def apply(name: String = defaultPartitionedRegionPartitioner.name): GemFireRDDPartitioner = {
    val p = partitioners.get(name)
    if (p.isDefined) p.get
    else {
      logWarning(s"Invalid preferred partitioner name $name.")
      defaultPartitionedRegionPartitioner
    }
  }

}
