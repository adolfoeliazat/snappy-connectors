/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.snappydata.spark.gemfire.connector.internal

import java.util.{List => JList, Set => JSet}

import com.gemstone.gemfire.cache.Region
import com.gemstone.gemfire.cache.client.{ClientCache, ClientRegionShortcut}
import com.gemstone.gemfire.cache.execute.{FunctionException, FunctionService}
import com.gemstone.gemfire.cache.query.Query
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import com.gemstone.gemfire.internal.cache.execute.InternalExecution
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdSingleResultCollector
import io.snappydata.spark.gemfire.connector.GemFireConnection
import io.snappydata.spark.gemfire.connector.internal.gemfirefunctions.{ConnectorStreamingResultCollector, CountResultCollector, DummyFunction}
import io.snappydata.spark.gemfire.connector.internal.oql.QueryResultCollector
import io.snappydata.spark.gemfire.connector.internal.rdd.GemFireRDDPartition
import io.snappydata.spark.gemfire.connector.internal.gemfirefunctions.shared.{ConnectorFunctionIDs, RegionMetadata}
import com.gemstone.gemfire.internal.cache.{GemFireCacheImpl, GemFireSparkConnectorCacheImpl}

import org.apache.spark.Logging


/**
  * Default GeodeConnection implementation. The instance of this should be
  * created by DefaultGeodeConnectionFactory
  *
  * @param locators pairs of host/port of locators
  *
  */
private[connector] class DefaultGemFireConnection(locators: Array[String])
    extends GemFireConnection with Logging {

  private val clientCache: ClientCache = GemFireCacheImpl.getInstance() // initClientCache()

  /** Register Geode functions to the Geode cluster */

  FunctionService.registerFunction(new DummyFunction {
    override def getId: String = ConnectorFunctionIDs.RetrieveRegionMetadataFunction_ID

    override def optimizeForWrite: Boolean = false
  })
  FunctionService.registerFunction(new DummyFunction {
    override def getId: String = ConnectorFunctionIDs.RetrieveRegionFunction_ID
  })

  FunctionService.registerFunction(new DummyFunction {
    override def getId: String = ConnectorFunctionIDs.RegionCountFunction_ID
  })

  /** close the clientCache */
  override def close(): Unit =
  if (!clientCache.isClosed) clientCache.close()

  /** ----------------------------------------- */
  /** implementation of GeodeConnection trait */
  /** ----------------------------------------- */

  override def getQuery(queryString: String): Query =
  clientCache.asInstanceOf[GemFireSparkConnectorCacheImpl].getRemoteGemFireQueryService.newQuery(queryString)

  override def validateRegion[K, V](regionPath: String): Unit = {
    val md = getRegionMetadata[K, V](regionPath)
    if (!md.isDefined) throw new RuntimeException(s"The region named $regionPath was not found")
  }

  override def getCount(regionPath: String, buckets: Set[Int],
      whereClause: Option[String]): Long = {
    val region = getRegionProxy(regionPath)
    val args: Array[String] = Array[String](whereClause.getOrElse(""))
    val rc = new CountResultCollector()
    import scala.collection.JavaConverters._
    val exec = FunctionService.onRegion(region).withArgs(args).withCollector(rc).
        asInstanceOf[InternalExecution].withFilter(buckets.map(Integer.valueOf).asJava)

    exec.execute(ConnectorFunctionIDs.RegionCountFunction_ID).getResult.asInstanceOf[Long]

  }

  def getRegionMetadata[K, V](regionPath: String): Option[RegionMetadata] = {
    import scala.collection.JavaConverters._
    val region = getRegionProxy[K, V](regionPath)
    val set0: JSet[Integer] = Set[Integer](0).asJava
    val exec = FunctionService.onRegion(region).asInstanceOf[InternalExecution].withFilter(set0)
    // exec.setWaitOnExceptionFlag(true)
    try {
      val collector = exec.execute(ConnectorFunctionIDs.RetrieveRegionMetadataFunction_ID)
      val r = collector.getResult.asInstanceOf[JList[RegionMetadata]]
      logDebug(r.get(0).toString)
      Some(r.get(0))
    } catch {
      case e: FunctionException =>
        if (e.getMessage.contains(s"The region named /$regionPath was not found")) None
        else throw e
    }
  }

  override def getRegionData[K, V](regionPath: String, whereClause: Option[String],
      split: GemFireRDDPartition, keyLength: Int): Iterator[_] = {
    val region = getRegionProxy[K, V](regionPath)
    val desc = s"""RDD($regionPath, "${whereClause.getOrElse("")}", ${split.index})"""
    val args: Array[String] = Array[String](whereClause.getOrElse(""), desc, keyLength.toString)
    import scala.collection.JavaConverters._

    def executeFunction[T](collector: ConnectorStreamingResultCollector[T]): Unit = {
      val exec = FunctionService.onRegion(region).withArgs(args).withCollector(collector).
          asInstanceOf[InternalExecution].withFilter(split.bucketSet.map(Integer.valueOf).asJava)
      // exec.setWaitOnExceptionFlag(true)
      exec.execute(ConnectorFunctionIDs.RetrieveRegionFunction_ID)
    }



    if (keyLength > 0) {
      val collector = new ConnectorStreamingResultCollector[Array[Object]](desc)
      executeFunction(collector)
      collector.getResult.map { objs: Array[Object] => (objs(0).asInstanceOf[K],
          objs(1).asInstanceOf[V])
      }
    } else {
      val collector = new ConnectorStreamingResultCollector[Object](desc)
      executeFunction(collector)
      collector.getResult
    }

  }

  def getRegionProxy[K, V](regionPath: String): Region[K, V] = {
    val region1: Region[K, V] = clientCache.getRegion(regionPath).asInstanceOf[Region[K, V]]
    if (region1 != null) region1
    else DefaultGemFireConnection.regionLock.synchronized {
      val region2 = clientCache.getRegion(regionPath).asInstanceOf[Region[K, V]]
      if (region2 != null) region2
      else clientCache.createClientRegionFactory[K, V](ClientRegionShortcut.PROXY).create(regionPath)
    }
  }

  override def executeQuery(regionPath: String, bucketSet: Set[Int],
      queryString: String, returnRaw: Boolean):
  AnyRef = {
    import scala.collection.JavaConverters._
    FunctionService.registerFunction(new DummyFunction {
      override def getId: String = ConnectorFunctionIDs.QueryFunction_ID
    })
    val collector = new QueryResultCollector
    val region = getRegionProxy(regionPath)
    val args: Array[String] = Array[String](queryString, bucketSet.toString, returnRaw.toString)
    val exec = FunctionService.onRegion(region).withCollector(collector).
        asInstanceOf[InternalExecution].withFilter(bucketSet.map(Integer.valueOf).asJava).
        withArgs(args)
    exec.execute(ConnectorFunctionIDs.QueryFunction_ID)
    collector.getResult
  }


}

private[connector] object DefaultGemFireConnection {
  /** a lock object only used by getRegionProxy...() */
  private val regionLock = new Object
}

/** The purpose of this class is making unit test DefaultGeodeConnectionManager easier
  * class DefaultGeodeConnectionFactory {
  * *
  * def newConnection(locators: Seq[(String, Int)], gemFireProps: Map[String, String] = Map.empty) =
  * new DefaultGeodeConnection(locators, gemFireProps)
  * *
  * }
  */
