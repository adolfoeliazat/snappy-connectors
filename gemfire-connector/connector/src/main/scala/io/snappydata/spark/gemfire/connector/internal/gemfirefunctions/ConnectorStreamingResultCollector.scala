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

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}

import scala.reflect.{ClassTag, classTag}

import com.gemstone.gemfire.DataSerializer
import com.gemstone.gemfire.cache.execute.ResultCollector
import com.gemstone.gemfire.cache.query.internal.types.StructTypeImpl
import com.gemstone.gemfire.cache.query.types.StructType
import com.gemstone.gemfire.distributed.DistributedMember
import com.gemstone.gemfire.internal.ByteArrayDataInput
import com.gemstone.gemfire.internal.shared.Version
import io.snappydata.spark.gemfire.connector.internal.gemfirefunctions.shared.StructStreamingResult._

/**
  * StructStreamingResultCollector and StructStreamingResultSender are paired
  * to transfer result of list of `com.gemstone.gemfire.cache.query.Struct`
  * from Geode server to Spark Connector (the client of Geode server)
  * in streaming, i.e., while sender sending the result, the collector can
  * start processing the arrived result without waiting for full result to
  * become available.
  */
class ConnectorStreamingResultCollector[T: ClassTag](desc: String) extends
    ResultCollector[Array[Byte], Iterator[T]] {

  private val isValOnly = !classTag[T].runtimeClass.isArray
  /**
    * Note: The data is sent in chunks, and each chunk contains multiple
    * records. So the result iterator is an iterator (I) of iterator (II),
    * i.e., go through each chunk (iterator (I)), and for each chunk, go
    * through each record (iterator (II)).
    */
  private lazy val resultIterator = new Iterator[T] {

    private var currentIterator: Iterator[T] = nextIterator()

    override def hasNext: Boolean = {
      if (!currentIterator.hasNext && currentIterator != Iterator.empty) {
        currentIterator = nextIterator()
      }
      currentIterator.hasNext
    }

    /** Note: make sure call `hasNext` first to adjust `currentIterator` */
    override def next(): T = currentIterator.next()
  }
  private val queue: BlockingQueue[Array[Byte]] = new LinkedBlockingQueue[Array[Byte]]()
  var structType: StructType = null

  /** ------------------------------------------ */
  /** ResultCollector interface implementations */

  /** the constructor that provide default `desc` (description) */
  def this() = this("StructStreamingResultCollector")

  /** ------------------------------------------ */

  override def getResult: Iterator[T] = resultIterator

  override def getResult(timeout: Long, unit: TimeUnit): Iterator[T] =
    throw new UnsupportedOperationException()

  /** addResult add non-empty byte array (chunk) to the queue */
  override def addResult(memberID: DistributedMember, chunk: Array[Byte]): Unit =
  if (chunk != null && chunk.size > 1) {
    this.queue.add(chunk)
    // println(s"""$desc receive from $memberID: ${chunk.mkString(" ")}""")
  }

  /** endResults add special `Array.empty` to the queue as marker of end of data */
  override def endResults(): Unit = this.queue.add(Array.empty)

  /** ------------------------------------------ */
  /** Internal methods               */

  override def clearResults(): Unit = this.queue.clear()

  /** ------------------------------------------ */

  def getResultType: StructType = {
    // trigger lazy resultIterator initialization if necessary
    if (!isValOnly && structType == null) resultIterator.hasNext
    structType
  }

  /** get the iterator for the next chunk of data */
  private def nextIterator(): Iterator[T] = {
    val chunk: Array[Byte] = queue.take
    if (chunk.isEmpty) {
      Iterator.empty
    } else {
      val input = new ByteArrayDataInput()
      input.initialize(chunk, Version.CURRENT_GFE)
      val chunkType = input.readByte()
      // println(s"chunk type $chunkType")
      chunkType match {
        case TYPE_CHUNK =>
          if (structType == null) {
            structType = DataSerializer.readObject(input).asInstanceOf[StructTypeImpl]
          }
          nextIterator()
        case DATA_CHUNK =>
          // require(structType != null && structType.getFieldNames.length > 0)
          if (!isValOnly && structType == null) structType = KeyValueType
          chunkToIterator(input, if (isValOnly) 0 else structType.getFieldNames.length)
        case ERROR_CHUNK =>
          val error = DataSerializer.readObject(input).asInstanceOf[Exception]
          errorPropagationIterator(error)
        case _ => throw new RuntimeException(s"unknown chunk type: $chunkType")
      }
    }
  }

  /** create a iterator that propagate sender's exception */
  private def errorPropagationIterator(ex: Exception) = new Iterator[T] {
    val re = new RuntimeException(ex)

    override def hasNext: Boolean = throw re

    override def next(): T = throw re
  }

  /** convert a chunk of data to an iterator */
  private def chunkToIterator(input: ByteArrayDataInput, rowSize: Int) = new Iterator[T] {
    override def hasNext: Boolean = input.available() > 0

    val tmpInput = new ByteArrayDataInput()

    override def next(): T = {

      if (rowSize > 0) {
        (0 until rowSize).map { ignore =>
          readObject
        }.toArray.asInstanceOf[T]
      } else {
        readObject.asInstanceOf[T]
      }
    }

    private def readObject: Object = {
      val b = input.readByte()
      b match {
        case SER_DATA =>
          val arr: Array[Byte] = DataSerializer.readByteArray(input)
          tmpInput.initialize(arr, Version.CURRENT)
          DataSerializer.readObject(tmpInput).asInstanceOf[Object]
        case UNSER_DATA =>
          DataSerializer.readObject(input).asInstanceOf[Object]
        case BYTEARR_DATA =>
          DataSerializer.readByteArray(input).asInstanceOf[Object]
        case _ =>
          throw new RuntimeException(s"unknown data type $b")
      }
    }
  }

}

