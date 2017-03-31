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
package io.snappydata.spark.gemfire.connector.internal.oql

import scala.reflect.ClassTag

import io.snappydata.spark.gemfire.connector.internal.DefaultGemFireConnectionManager
import io.snappydata.spark.gemfire.connector.internal.rdd.{GemFireRDDPartition, ServerSplitsPartitioner}

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

/**
  * An RDD that provides the functionality that read the OQL query result
  *
  * @param sc          The SparkContext this RDD is associated with
  * @param queryString The OQL query string
  *
  */
class QueryRDD[T](@transient sc: SparkContext,
    queryString: String)
    (implicit ct: ClassTag[T])
    extends RDD[T](sc, Seq.empty) {

  override def getPartitions: Array[Partition] = {
    val conn = DefaultGemFireConnectionManager.getConnection
    val regionPath = getRegionPathFromQuery(queryString)
    val md = conn.getRegionMetadata(regionPath)
    md match {
      case Some(metadata) =>
        if (metadata.isPartitioned) {
          val splits = ServerSplitsPartitioner.partitions(conn, metadata, Map.empty)
          logInfo(s"QueryRDD.getPartitions():isPartitioned=true, partitions=${splits.mkString(",")}")
          splits
        }
        else {
          logInfo(s"QueryRDD.getPartitions():isPartitioned=false")
          Array[Partition](new GemFireRDDPartition(0, Set.empty))

        }
      case None => throw new RuntimeException(s"Region $regionPath metadata was not found.")
    }
  }

  private def getRegionPathFromQuery(queryString: String): String = {
    val r = QueryParser.parseOQL(queryString).get
    r match {
      case r: String =>
        val start = r.indexOf("/") + 1
        var end = r.indexOf(")")
        if (r.indexOf(".") > 0) end = math.min(r.indexOf("."), end)
        if (r.indexOf(",") > 0) end = math.min(r.indexOf(","), end)
        val regionPath = r.substring(start, end)
        regionPath
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val buckets = split.asInstanceOf[GemFireRDDPartition].bucketSet
    val regionPath = getRegionPathFromQuery(queryString)
    val result = DefaultGemFireConnectionManager.getConnection.executeQuery(regionPath, buckets, queryString)
    result match {
      case it: Iterator[T] =>
        logInfo(s"QueryRDD.compute():query=$queryString, partition=$split")
        it
      case _ =>
        throw new RuntimeException("Unexpected OQL result: " + result.toString)
    }
  }
}
