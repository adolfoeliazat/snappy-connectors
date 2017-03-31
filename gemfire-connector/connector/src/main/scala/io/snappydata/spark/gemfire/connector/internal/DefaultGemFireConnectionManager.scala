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
package io.snappydata.spark.gemfire.connector.internal


import scala.collection.mutable

import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem
import io.snappydata.spark.gemfire.connector.GemFireConnection


object DefaultGemFireConnectionManager {

  val locators: Array[String] = {
    val locatorsStr = InternalDistributedSystem.getConnectedInstance.getConfig.getRemoteLocators
    locatorsStr.split(',')
  }
  /** connection cache, keyed by host:port pair */
  private[connector] val connections = mutable.Map[String, GemFireConnection]()

  /**
    * use locator host:port pair to lookup cached connection. create new connection
    * and add it to the cache `connections` if it does not exist.
    */
  def getConnection: GemFireConnection = {
    def getCachedConnection(locators: Array[String]): GemFireConnection = {
      val conns = locators.map(connections withDefaultValue null).filter(_ != null)
      if (conns.nonEmpty) conns(0) else null
    }

    val conn1 = getCachedConnection(locators)
    if (conn1 != null) conn1
    else connections.synchronized {
      val conn2 = getCachedConnection(locators)
      if (conn2 != null) conn2
      else {
        val conn3 = new DefaultGemFireConnection(locators)
        locators.foreach(elem => connections += (elem -> conn3))
        conn3
      }
    }
  }

  /**
    * Close the connection and remove it from connection cache.
    * Note: multiple entries may share the same connection, all those entries are removed.
    */
  def closeConnection(): Unit = {
    val conns = locators.map(connections withDefaultValue null).filter(_ != null)
    if (conns.nonEmpty) connections.synchronized {
      conns(0).close()
      connections.retain((k, v) => v != conns(0))
    }
  }
}
