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


import scala.annotation.meta.param

import io.snappydata.spark.gemfire.connector.internal.oql.OQLRelation
import io.snappydata.spark.gemfire.connector.internal.rdd.GemFireRegionRDD

import org.apache.spark.Logging
import org.apache.spark.sql.sources.connector.gemfire.{Constants, GemFireRelation}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Provide Geode OQL specific functions
  */
class GemFireSQLContextFunctions(@(transient @param) sqlContext: SQLContext) extends
    Serializable with Logging {

  /**
    * Expose a Geode OQL query result as a DataFrame
    *
    * @param query the OQL query string.
    */
  def gemfireOQL(query: String, opConf: Map[String, String] = Map.empty): DataFrame = {
    logInfo(s"OQL query = $query")

     val computeOQLCreator = (rdd: GemFireRegionRDD[Any, Any, Any]) => {
        GemFireRelation.computeForOQL[Any]
    }
    val options = if (opConf == null) Map.empty[String, String] else opConf
    val rdd = new GemFireRegionRDD[Any, Any, Any](sqlContext.sparkContext,
      None, computeOQLCreator, options, None, options.get(Constants.gridNameKey), None,
      Some(query), None)

    sqlContext.baseRelationToDataFrame(OQLRelation(rdd)(sqlContext))

  }


}
