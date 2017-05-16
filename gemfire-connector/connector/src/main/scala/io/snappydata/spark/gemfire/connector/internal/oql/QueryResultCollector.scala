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

import java.io.{DataInput, InputStream}
import java.util.concurrent.{LinkedBlockingDeque, TimeUnit}

import com.gemstone.gemfire.DataSerializer
import com.gemstone.gemfire.cache.execute.ResultCollector
import com.gemstone.gemfire.distributed.DistributedMember
import com.gemstone.gemfire.internal.ByteArrayDataInput
import com.gemstone.gemfire.internal.shared.Version

import org.apache.spark.sql.sources.connector.gemfire.RowDeserializer
import org.apache.spark.sql.types._


class QueryResultCollector(schemaOpt: Option[StructType]) extends
    ResultCollector[Array[Byte], Iterator[Any]] {

  private lazy val resultIterator = new Iterator[Any] {
    private var currentIterator = nextIterator

    def hasNext = {
      if (!currentIterator.hasNext && currentIterator != Iterator.empty) {
        currentIterator = nextIterator
      }
      currentIterator.hasNext
    }

    def next = currentIterator.next
  }
  private val queue = new LinkedBlockingDeque[Array[Byte]]()

  override def getResult: Iterator[Any] = resultIterator

  override def getResult(timeout: Long, unit: TimeUnit): Iterator[Any] =
    throw new UnsupportedOperationException

  override def addResult(memberID: DistributedMember, chunk: Array[Byte]): Unit =
    if (chunk != null && chunk.size > 0) {
      queue.add(chunk)
    }

  override def endResults: Unit = queue.add(Array.empty)

  override def clearResults: Unit = queue.clear

  val reader = if (schemaOpt.isDefined) {
    val schema = schemaOpt.get
    if(schema.size == 1) {
      (is: DataInput) => {
        val obj = DataSerializer.readObject(is).asInstanceOf[Any]
        obj match {
          case z: java.lang.Short => z.shortValue
          case z: java.lang.Integer => z.intValue
          case z: java.lang.Float => z.floatValue
          case z: java.lang.Long => z.longValue
          case z: java.lang.Double => z.doubleValue
          case z: java.lang.Boolean => z.booleanValue
          case _ => obj.asInstanceOf[Any]
        }
      }
    } else {
      (is: DataInput) => RowDeserializer.readArrayDataWithoutTopSchema(is, schema,
        false).asInstanceOf[Any]
    }
  } else {
    (is: DataInput) => DataSerializer.readObject(is).asInstanceOf[Any]
  }
  private def nextIterator: Iterator[Any] = {
    val chunk = queue.take
    if (chunk.isEmpty) {
      Iterator.empty
    }
    else {
      val input = new ByteArrayDataInput
      input.initialize(chunk, Version.CURRENT_GFE)

      new Iterator[Any] {
        override def hasNext: Boolean = input.available() > 0

        override def next: Any = reader(input)
      }
    }
  }

}
