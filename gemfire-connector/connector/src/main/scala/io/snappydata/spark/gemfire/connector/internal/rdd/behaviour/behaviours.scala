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
package io.snappydata.spark.gemfire.connector.internal.rdd.behaviour

import scala.reflect.ClassTag

import io.snappydata.spark.gemfire.connector.internal.DefaultGemFireConnectionManager
import io.snappydata.spark.gemfire.connector.internal.GemFireRow
import io.snappydata.spark.gemfire.connector.internal.rdd.{GemFireRDDPartition, GemFireRegionRDD}

import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.TaskContext



class ExposeRegion[K: ClassTag, V: ClassTag, T: ClassTag] extends ComputeLogic[K, V, T]{
  override def apply(rdd: GemFireRegionRDD[K, V, T], partition: GemFireRDDPartition,
      taskContext: TaskContext): Iterator[T] = {
    // logDebug(s"compute RDD id=${this.id} partition $partition")

    val iter = DefaultGemFireConnectionManager.getConnection.
        getRegionData[Any, Any](rdd.regionPath.get, rdd.whereClause, partition, 1, None).
        asInstanceOf[Iterator[(Any, Any)]]
    if (rdd.isRowObject) {
      iter.map{
        case (k, v) => (k, ExposeRegion.valueExtractor(
          v.asInstanceOf[GemFireRow].getArray).asInstanceOf[V]).
            asInstanceOf[T]
      }
    } else {
      iter.asInstanceOf[Iterator[T]]
    }
  }
}

object ExposeRegion {

  def valueExtractor(arr: Array[Any]) : GenericRow = new GenericRow(arr.map(x => {
    x match {
       case gfRow: GemFireRow => valueExtractor(gfRow.getArray)
       case _ => x
    }
  }))
}