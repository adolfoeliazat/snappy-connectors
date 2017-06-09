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

import scala.collection.mutable.ListBuffer

import com.gemstone.gemfire.cache.query.internal.StructImpl

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._

class SchemaBuilder[T](queryRDD: RDD[T]) extends Logging {

  val nullStructType = StructType(Nil)

  val typeMap: Map[Class[_], DataType] = Map(
    (classOf[java.lang.String], StringType),
    (classOf[java.lang.Integer], IntegerType),
    (classOf[java.lang.Short], ShortType),
    (classOf[java.lang.Long], LongType),
    (classOf[java.lang.Double], DoubleType),
    (classOf[java.lang.Float], FloatType),
    (classOf[java.lang.Boolean], BooleanType),
    (classOf[java.lang.Byte], ByteType),
    (classOf[java.util.Date], DateType),
    (classOf[java.lang.Object], nullStructType)
  )

  /**
    * Analyse QueryRDD to get the Spark schema
    *
    * @return The schema represented by Spark StructType
    */
  def toSparkSchema(): StructType = {
    val row = queryRDD.first()
    val tpe = row match {
      case r: StructImpl => constructFromStruct(r)
      case null => StructType(StructField("col1", NullType) :: Nil)
      case default =>
        val value = typeMap.getOrElse(default.getClass(), {
          try {
            Encoders.bean(default.getClass).schema
          } catch {
            case e: Exception => nullStructType
          }
        })
        StructType(StructField("col1", value) :: Nil)
    }
    logInfo(s"Schema: $tpe")
    tpe
  }

  def constructFromStruct(r: StructImpl): StructType = {
    val names = r.getFieldNames
    val values = r.getFieldValues
    val lb = new ListBuffer[StructField]()
    for (i <- 0 until names.length) {
      val name = names(i)
      val value = values(i)
      val dataType = value match {
        case null => NullType
        case default => {
          typeMap.getOrElse(default.getClass, {
            try {
              Encoders.bean(default.getClass).schema
            } catch {
              case e: Exception => nullStructType
            }
          })
        }
      }
      lb += StructField(name, dataType)
    }
    StructType(lb.toSeq)
  }
}
