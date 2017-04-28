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
import io.snappydata.spark.gemfire.connector.internal.oql.QueryParser
import io.snappydata.spark.gemfire.connector.internal.rdd.GemFireRDDPartitioner._
import io.snappydata.spark.gemfire.connector.internal.rdd.behaviour.{ComputeLogic, ExposeRegion}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{Partition, SparkContext, TaskContext}

/**
  * This class exposes Geode region as a RDD.
  *
  * @param sc         the Spark Context
  * @param regionPath the full path of the region
  * @param opConf     the parameters for this operation, such as preferred partitioner.
  */
class GemFireRegionRDD[K, V, T]
(@transient val sc: SparkContext,
    val regionPath: Option[String],
    val computeLogicCreator: GemFireRegionRDD[K, V, T] => ComputeLogic[K, V, T],
    val opConf: Map[String, String] = Map.empty,
    val rowObjectLength: Option[Int],
    val whereClause: Option[String] = None,
    val oql: Option[String] = None
)(implicit ctk: ClassTag[K], ctv: ClassTag[V], ctr: ClassTag[T])
    extends RDD[T](sc, Seq.empty) {

  /** validate region existence when GeodeRDD object is created */
  validate()

  def kClassTag = ctk

  def vClassTag = ctv

  def tClassTag = ctr

  val isRowObject = classOf[Row].isAssignableFrom(vClassTag.runtimeClass)

  /** When where clause is specified, OQL query
    * `select key, value from /<region-path>.entries where <where clause> `
    * is used to filter the dataset.
    */
  def where(whereClause: Option[String]): GemFireRegionRDD[K, V, T] = {
    if (whereClause.isDefined) copy(whereClause = whereClause)
    else this
  }

  /** this version is for Java API that doesn't use scala.Option */
  def where(whereClause: String): GemFireRegionRDD[K, V, T] = {
    if (whereClause == null || whereClause.trim.isEmpty) this
    else copy(whereClause = Option(whereClause.trim))
  }

  override def compute(split: Partition, taskContext: TaskContext): scala.Iterator[T] = {
    val partition = split.asInstanceOf[GemFireRDDPartition]
    computeLogicCreator(this)(this, partition, taskContext)
  }

  protected def copy(
      regionPath: Option[String] = regionPath,
      opConf: Map[String, String] = opConf,
      whereClause: Option[String] = None): GemFireRegionRDD[K, V, T] = {
    require(sc != null,
      """RDD transformation requires a non-null SparkContext. Unfortunately
        |SparkContext in this GeodeRDD is null. This can happen after
        |GeodeRDD has been deserialized. SparkContext is not Serializable,
        |therefore it deserializes to null. RDD transformations are not allowed
        |inside lambdas used in other RDD transformations.""".stripMargin)

    new GemFireRegionRDD[K, V, T](sc, regionPath, computeLogicCreator, opConf, rowObjectLength,
      whereClause, oql)

  }


  /**
    * Use preferred partitioner generate partitions. `defaultReplicatedRegionPartitioner`
    * will be used if it's a replicated region.
    */
  override def getPartitions: Array[Partition] = {
    val conn = DefaultGemFireConnectionManager.getConnection
    val rgn = regionPath.getOrElse(oql.map(GemFireRegionRDD.getRegionPathFromQuery(_)).
        getOrElse(throw new IllegalStateException("Unknown region")))
    val md = conn.getRegionMetadata(rgn)
    md match {
      case None => throw new RuntimeException(s"region $regionPath was not found.")
      case Some(data) =>

        logInfo(
          s"""RDD  region=$regionPath
              |conn=${DefaultGemFireConnectionManager.locators.mkString(",")},
              | env=$opConf""".stripMargin)

        val p = if (data.isPartitioned) preferredPartitioner(opConf)
        else defaultReplicatedRegionPartitioner
        val splits = p.partitions(conn, data, opConf, Some(sparkContext))
        logDebug(s"""RDD  region=$regionPath partitions=\n  ${splits.mkString("\n  ")}""")
        splits
    }
  }


  /**
    * provide preferred location(s) (host name(s)) of the given partition.
    * Only some partitioner implementation(s) provides this info, which is
    * useful when Spark cluster and Geode cluster share some hosts.
    */
  override def getPreferredLocations(split: Partition): Seq[String] =
  split.asInstanceOf[GemFireRDDPartition].locations


  /** Validate region, and make sure it exists. */
  private def validate(): Unit = {
    val rgn = regionPath.getOrElse(oql.map(GemFireRegionRDD.getRegionPathFromQuery(_)).
        getOrElse(throw new IllegalStateException("Unknown region")))
    DefaultGemFireConnectionManager.
        getConnection.validateRegion[K, V](rgn)
  }


  /**
    * Get preferred partitioner. return `defaultPartitionedRegionPartitioner` if none
    * preference is specified.
    */
  private def preferredPartitioner(opConf: Map[String, String]) = GemFireRDDPartitioner(
    opConf.getOrElse(PreferredPartitionerPropKey,
      GemFireRDDPartitioner.defaultPartitionedRegionPartitioner.name))

}

object GemFireRegionRDD {

  def exposeRegion[K: ClassTag, V: ClassTag](sc: SparkContext, regionPath: String,
      opConf: Map[String, String] = Map.empty): GemFireRegionRDD[K, V, (K, V)] =
    new GemFireRegionRDD[K, V, (K, V)](sc, Some(regionPath),
      (rdd: GemFireRegionRDD[K, V, (K, V)]) => new ExposeRegion[K, V, (K, V)],
      opConf, None)

  def getRegionPathFromQuery(queryString: String): String = {
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
}
