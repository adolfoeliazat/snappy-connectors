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
import scala.collection.immutable.SortedSet
import scala.collection.mutable
import scala.reflect.ClassTag

import io.snappydata.spark.gemfire.connector.internal.gemfirefunctions.shared.RegionMetadata
import io.snappydata.spark.gemfire.connector.{GemFireConnection,
NumberPartitionsPerServerPropKey, MaxBucketsPerPartitionKey, MaxBucketsPerPartitionDefault}

import org.apache.spark.{Partition, SparkContext}

/** This partitioner maps whole region to one GeodeRDDPartition */
object OnePartitionPartitioner extends GemFireRDDPartitioner {

  override val name = "OnePartition"

  override def partitions(conn: GemFireConnection, md: RegionMetadata, env: Map[String, String],
      sparkContext: Option[SparkContext]): Array[Partition] =
    Array[Partition](new GemFireRDDPartition(0, Set.empty))
}

/**
  * This partitioner maps whole region to N * M Geode RDD partitions, where M is the number of
  * Geode servers that contain the data for the given region. Th default value of N is 1.
  */
object ServerSplitsPartitioner extends GemFireRDDPartitioner {

  override val name = "ServerSplits"

  override def partitions(conn: GemFireConnection, md: RegionMetadata, env: Map[String, String],
      sparkContext: Option[SparkContext]): Array[Partition] = {
    if (md == null) throw new RuntimeException("RegionMetadata is null")
    val numberOfExecutors = sparkContext.map(
      org.apache.spark.sql.collection.Utils.getAllExecutorsMemoryStatus(_).size)
    /*
    val n = try {

      env.getOrElse(NumberPartitionsPerServerPropKey, "2").toInt
    } catch {
      case e: NumberFormatException => 2
    }
    */
    if (!md.isPartitioned || md.getServerBucketMap == null || md.getServerBucketMap.isEmpty) {
      Array[Partition](new GemFireRDDPartition(0, Set.empty))
    }
    else {
      val map = mapAsScalaMap(md.getServerBucketMap)
          .map { case (srv, set) => (srv, asScalaSet(set).map(_.toInt)) }.toList
          .map { case (srv, set) => (srv.getHostName, set) }
      doPartitions(map, md.getTotalBuckets, numberOfExecutors,
        env.getOrElse(MaxBucketsPerPartitionKey, MaxBucketsPerPartitionDefault.toString).toInt)
    }
  }

  /** Converts server to bucket ID set list to array of RDD partitions */
  def doPartitions(serverBucketMap: List[(String, mutable.Set[Int])],
      totalBuckets: Int, numberOfExecutors: Option[Int], maxBucketsPerPartition: Int)
  : Array[Partition] = {

    // method that calculates the group size for splitting "k" items into "g" groups
    def groupSize(k: Int, g: Int): Int = scala.math.ceil(k / g.toDouble).toInt

    // 1. convert list of server and bucket set pairs to a list of server and
    // sorted bucket set pairs
    val srvToSortedBucketSet = serverBucketMap.map { case (srv, set) => (srv, SortedSet[Int]() ++ set) }
    val minimumNumberOfpartitions = numberOfExecutors.map(scala.math.max(_,
      srvToSortedBucketSet.size))
    val totalPartitions = minimumNumberOfpartitions.map(x => {
      val temp = totalBuckets / x
      if(temp > maxBucketsPerPartition) {
        totalBuckets / maxBucketsPerPartition
      } else {
        x
      }
    }).getOrElse(totalBuckets / maxBucketsPerPartition )

    // 2. split bucket set of each server into n splits if possible, and server to Seq(server)
    val srvToSplitedBuckeSet = srvToSortedBucketSet.flatMap { case (host, set) =>
      if (set.isEmpty) Nil else set.grouped(groupSize(set.size, totalPartitions)).
          toList.map(s => (Seq(host), s))
    }

    // 3. calculate empty bucket IDs by removing all bucket sets of all
    // servers from the full bucket sets
    val emptyIDs = SortedSet[Int]() ++ ((0 until totalBuckets).toSet /: srvToSortedBucketSet) {
      case (s1, (k, s2)) => s1 &~ s2
    }

    // 4. distribute empty bucket IDs to all partitions evenly.
    //    The empty buckets do not contain data when partitions are created,
    // but they may contain data
    //    when RDD is materialized, so need to include those bucket IDs in the partitions.
    val srvToFinalBucketSet = if (emptyIDs.isEmpty) srvToSplitedBuckeSet
    else srvToSplitedBuckeSet.zipAll(
      emptyIDs.grouped(groupSize(emptyIDs.size, srvToSplitedBuckeSet.size)).
          toList, (Nil, Set.empty), Set.empty).map {
      case ((server, set1), set2) => (server, SortedSet[Int]() ++ set1 ++ set2)
    }
   
    // 5. create array of partitions w/ 0-based index
    (0 until srvToFinalBucketSet.size).toList.zip(srvToFinalBucketSet).map {
      case (i, (srv, set)) => {
      val temp = srv.filter(_ != null)
      new GemFireRDDPartition(i, set, if (temp.isEmpty) Nil else temp)
    }
    }.toArray
  }
}
