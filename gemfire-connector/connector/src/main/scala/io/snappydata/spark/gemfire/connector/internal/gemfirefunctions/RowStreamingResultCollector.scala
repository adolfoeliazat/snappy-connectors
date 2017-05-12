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
package io.snappydata.spark.gemfire.connector.internal.gemfirefunctions

import java.io.DataInputStream

import scala.reflect.ClassTag

import com.gemstone.gemfire.DataSerializer
import com.gemstone.gemfire.internal.ByteArrayDataInput
import com.gemstone.gemfire.internal.shared.Version
import io.snappydata.spark.gemfire.connector.internal.GemFireRow
import io.snappydata.spark.gemfire.connector.internal.gemfirefunctions.shared.StructStreamingResult._

import org.apache.spark.sql.sources.connector.gemfire.RowDeserializer
import org.apache.spark.sql.types.StructType

class RowStreamingResultCollector[T: ClassTag](desc: String, schema: StructType) extends
    ConnectorStreamingResultCollector[T](desc) {

  override def chunkToIterator(input: ByteArrayDataInput, rowSize: Int): Iterator[T] =
    new RowChunkIterator[T](input, rowSize)

  class RowChunkIterator[T](input: ByteArrayDataInput, rowSize: Int) extends
      ChunkIterator[T](input, rowSize) {

    private def readValue: Object = {
      val b = input.readByte()
      b match {
        case SER_DATA =>
          val arr: Array[Byte] = DataSerializer.readByteArray(input)
          tmpInput.initialize(arr, Version.CURRENT)
          DataSerializer.readObject(tmpInput).asInstanceOf[GemFireRow].getArray
        case UNSER_DATA =>
          RowDeserializer.readArrayData(new DataInputStream(input), schema)
        case BYTEARR_DATA =>
          DataSerializer.readByteArray(input).asInstanceOf[Object]
        case _ =>
          throw new RuntimeException(s"unknown data type $b")
      }
    }
  }

}

