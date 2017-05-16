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

package org.apache.spark.sql.sources.connector.gemfire




import io.snappydata.spark.gemfire.connector.internal.gemfirefunctions.shared.GemFireRow
import io.snappydata.spark.gemfire.connector.internal.gemfirefunctions.shared.SchemaMappings._

import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._


object GemFireRowHelper {
  val mapping = Map[AbstractDataType, Byte](StringType -> stringg,
    ShortType -> shortt,
    IntegerType -> intt,
    LongType -> longg, DoubleType -> doublee,
    ByteType -> bytee,
    FloatType -> floatt, BooleanType -> booll,
    BinaryType -> binary,
    DateType -> datee, TimestampType -> timestampp,
    StructType -> structtypee )

  def getSchemaCode(schema: StructType): Array[Byte] = schema.map(sf =>
    GemFireRowHelper.mapping.get(sf.dataType).getOrElse(unoptimizedtype)).toArray


  def convertNestedGemFireRowToRow(topLevelValueSchema: StructType,
      top: Array[Any], keyPartLength: Int): Array[Any] = {
      topLevelValueSchema.zipWithIndex.foreach {
        case (sf, i) => sf.dataType match {
          case st: StructType => {
            top(i) = convertObjectArrayToArrayAny(top(i).
                asInstanceOf[GemFireRow].getArray)
            top(i + keyPartLength) = new GenericRow(convertNestedGemFireRowToRow(st,
              top(i).asInstanceOf[Array[Any]], 0))
          }
          case _ =>
        }
      }
      top
  }

  def convertObjectArrayToArrayAny(array: Array[Object]) : Array[Any] = {
    array.map(x => x match {
      case z: java.lang.Short => z.shortValue()
      case z: java.lang.Integer => z.intValue()
      case z: java.lang.Float => z.floatValue()
      case z: java.lang.Long => z.longValue()
      case z: java.lang.Double => z.doubleValue()
      case z: java.lang.Boolean => z.booleanValue()
      case _ => x.asInstanceOf[Any]
    })
  }
}

