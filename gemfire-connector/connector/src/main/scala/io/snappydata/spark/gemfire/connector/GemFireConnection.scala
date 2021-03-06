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

import com.gemstone.gemfire.cache.Region
import com.gemstone.gemfire.cache.query.Query
import io.snappydata.spark.gemfire.connector.internal.rdd.GemFireRDDPartition
import io.snappydata.spark.gemfire.connector.internal.gemfirefunctions.shared.RegionMetadata
import org.apache.spark.sql.types.StructType

trait GemFireConnection {

  /**
    * Validate region existence and key/value type constraints, throw RuntimeException
    * if region does not exist or key and/or value type do(es) not match.
    *
    * @param regionPath the full path of region
    */
  def validateRegion[K, V](regionPath: String, gridName: Option[String]): Unit

  /**
    * Get Region proxy for the given region
    *
    * @param regionPath the full path of region
    */
  def getRegionProxy[K, V](regionPath: String, gridName: Option[String]): Region[K, V]

  /**
    * Retrieve region meta data for the given region.
    *
    * @param regionPath : the full path of the region
    * @return Some[RegionMetadata] if region exists, None otherwise
    */
  def getRegionMetadata[K, V](regionPath: String, gridName: Option[String]): Option[RegionMetadata]

  /**
    * Retrieve region data for the given region and bucket set
    *
    * @param regionPath  : the full path of the region
    * @param whereClause : the set of bucket IDs
    * @param split       : Geode RDD Partition instance
    */
  def getRegionData[K, V](regionPath: String, whereClause: Option[String],
      split: GemFireRDDPartition, keyLength: Int,
      schemaOpt: Option[StructType], gridName: Option[String]): Iterator[_]

  def executeQuery(regionPath: String, bucketSet: Set[Int],
      queryString: String, schemaOpt: Option[StructType], gridName: Option[String]): Object

  /**
    * Create a gemfire OQL query
    *
    * @param queryString Geode OQL query string
    */
  def getQuery(queryString: String): Query

  def getCount(regionPath: String, buckets: Set[Int],
      whereClause: Option[String], gridName: Option[String]): Long

  /** Close the connection */
  def close(): Unit
}


