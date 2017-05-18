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

import java.io.DataInput
import java.sql.Timestamp

import com.gemstone.gemfire.DataSerializer
import io.snappydata.spark.gemfire.connector.internal.GemFireRow

import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._

object RowDeserializer {

  def readArrayDataWithoutTopSchema(dis: DataInput, schema: StructType,
      readNestedStruct: Boolean = true): Array[Any] = {
    val numLongs: Int = GemFireRow.getNumLongsForBitSet(schema.length)
    val mask = Array.fill[Long](numLongs)(dis.readLong())
    val bitset = scala.collection.immutable.BitSet.fromBitMaskNoCopy(mask)
    val deser = Array.ofDim[Any](schema.length)
    var i = 0
    schema.foreach(sf => {
      if (bitset(i)) {
        sf.dataType match {
          case  StringType => deser(i) = DataSerializer.readString(dis)
          case  ShortType => deser(i) = DataSerializer.readPrimitiveShort(dis)
          case  IntegerType => deser(i) = DataSerializer.readPrimitiveInt(dis)
          case  LongType => deser(i) = DataSerializer.readPrimitiveLong(dis)
          case  DoubleType => deser(i) = DataSerializer.readPrimitiveDouble(dis)
          case  ByteType => deser(i) = DataSerializer.readPrimitiveByte(dis)
          case  FloatType => deser(i) = DataSerializer.readPrimitiveFloat(dis)
          case  BinaryType => deser(i) = DataSerializer.readByteArray(dis)
          case  BooleanType => deser(i) = DataSerializer.readPrimitiveBoolean(dis)
          case  DateType => {
            val time = DataSerializer.readPrimitiveLong(dis)
            deser(i) = new java.sql.Date(time)
          }
          case _: TimestampType => {
            val time = DataSerializer.readPrimitiveLong(dis)
            val nano = DataSerializer.readPrimitiveInt(dis)
            val ts = new Timestamp(time)
            ts.setNanos(nano)
            deser(i) = ts
          }
          case _: StructType => {
            deser(i) = if (readNestedStruct) {
              // This case happens only if complete value is obtained ,
              // i.e GFRow itself is serialzied,
              // This is the case when query is on all columns of region value
              // & is the case of
              // fetching select * only & for sure region contains GFRow
              // & not domain object.
              // In this case it is safe to convert nested struct
              // into spark Row
              val gfRow = new GemFireRow()
              gfRow.fromData(dis)
              gfRow
            } else {
              DataSerializer.readObject(dis)
            }
          }
          // read unoptimized type
          case _ => deser(i) = DataSerializer.readObject(dis)
        }

      }
      i += 1
    }
    )
    deser
  }

}
