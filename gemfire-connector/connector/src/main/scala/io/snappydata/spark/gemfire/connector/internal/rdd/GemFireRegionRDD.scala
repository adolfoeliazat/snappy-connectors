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

import scala.collection.Seq
import scala.reflect.ClassTag

import io.snappydata.spark.gemfire.connector.PreferredPartitionerPropKey
import io.snappydata.spark.gemfire.connector.internal.DefaultGemFireConnectionManager
import io.snappydata.spark.gemfire.connector.internal.rdd.GemFireRDDPartitioner._

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

/**
  * This class exposes Geode region as a RDD.
  *
  * @param sc         the Spark Context
  * @param regionPath the full path of the region
  * @param opConf     the parameters for this operation, such as preferred partitioner.
  */
class GemFireRegionRDD[K, V] private[connector]
(@transient sc: SparkContext,
    val regionPath: String,
    val opConf: Map[String, String] = Map.empty,
    val whereClause: Option[String] = None
)(implicit ctk: ClassTag[K], ctv: ClassTag[V])
    extends RDD[(K, V)](sc, Seq.empty) {

  /** validate region existence when GeodeRDD object is created */
  validate()

  def kClassTag = ctk

  def vClassTag = ctv

  /** When where clause is specified, OQL query
    * `select key, value from /<region-path>.entries where <where clause> `
    * is used to filter the dataset.
    */
  def where(whereClause: Option[String]): GemFireRegionRDD[K, V] = {
    if (whereClause.isDefined) copy(whereClause = whereClause)
    else this
  }

  /** this version is for Java API that doesn't use scala.Option */
  def where(whereClause: String): GemFireRegionRDD[K, V] = {
    if (whereClause == null || whereClause.trim.isEmpty) this
    else copy(whereClause = Option(whereClause.trim))
  }

  /**
    * method `copy` is used by method `where` that creates new immutable
    * GeodeRDD instance based this instance.
    */
  private def copy(
      regionPath: String = regionPath,
      opConf: Map[String, String] = opConf,
      whereClause: Option[String] = None
  ): GemFireRegionRDD[K, V] = {

    require(sc != null,
      """RDD transformation requires a non-null SparkContext. Unfortunately
        |SparkContext in this GeodeRDD is null. This can happen after
        |GeodeRDD has been deserialized. SparkContext is not Serializable,
        |therefore it deserializes to null. RDD transformations are not allowed
        |inside lambdas used in other RDD transformations.""".stripMargin)

    new GemFireRegionRDD[K, V](sc, regionPath, opConf, whereClause)
  }

  /**
    * Use preferred partitioner generate partitions. `defaultReplicatedRegionPartitioner`
    * will be used if it's a replicated region.
    */
  override def getPartitions: Array[Partition] = {
    val conn = DefaultGemFireConnectionManager.getConnection
    val md = conn.getRegionMetadata[K, V](regionPath)
    md match {
      case None => throw new RuntimeException(s"region $regionPath was not found.")
      case Some(data) =>
        logInfo(s"""RDD id=${this.id} region=$regionPath conn=${DefaultGemFireConnectionManager.locators.mkString(",")}, env=$opConf""")
        val p = if (data.isPartitioned) preferredPartitioner else defaultReplicatedRegionPartitioner
        val splits = p.partitions[K, V](conn, data, opConf)
        logDebug(s"""RDD id=${this.id} region=$regionPath partitions=\n  ${splits.mkString("\n  ")}""")
        splits
    }
  }

  /**
    * Get preferred partitioner. return `defaultPartitionedRegionPartitioner` if none
    * preference is specified.
    */
  private def preferredPartitioner =
  GemFireRDDPartitioner(opConf.getOrElse(
    PreferredPartitionerPropKey, GemFireRDDPartitioner.defaultPartitionedRegionPartitioner.name))

  /**
    * provide preferred location(s) (host name(s)) of the given partition.
    * Only some partitioner implementation(s) provides this info, which is
    * useful when Spark cluster and Geode cluster share some hosts.
    */
  override def getPreferredLocations(split: Partition) =
  split.asInstanceOf[GemFireRDDPartition].locations

  /** materialize a RDD partition */
  override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = {
    val partition = split.asInstanceOf[GemFireRDDPartition]
    logDebug(s"compute RDD id=${this.id} partition $partition")
    DefaultGemFireConnectionManager.getConnection.getRegionData[K, V](regionPath, whereClause, partition)
    // new InterruptibleIterator(context, split.asInstanceOf[GeodeRDDPartition[K, V]].iterator)
  }

  /** Validate region, and make sure it exists. */
  private def validate(): Unit = DefaultGemFireConnectionManager.getConnection.validateRegion[K, V](regionPath)
}

object GemFireRegionRDD {

  def apply[K: ClassTag, V: ClassTag](sc: SparkContext, regionPath: String,
      opConf: Map[String, String] = Map.empty)
  : GemFireRegionRDD[K, V] =
    new GemFireRegionRDD[K, V](sc, regionPath, opConf)

}
