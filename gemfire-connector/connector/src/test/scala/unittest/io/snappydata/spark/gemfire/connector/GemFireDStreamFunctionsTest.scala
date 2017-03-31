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
package unittest.io.snappydata.spark.gemfire.connector

import scala.reflect.ClassTag

import com.gemstone.gemfire.cache.Region
import io.snappydata.spark.gemfire.connector.GemFireConnection
import org.mockito.Matchers.{any => mockAny, eq => mockEq}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSuite, Matchers}

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

class GemFireDStreamFunctionsTest extends FunSuite with Matchers with MockitoSugar {

  test("test GemFirePairDStreamFunctions Implicit") {
    import io.snappydata.spark.gemfire.connector.streaming._
    val mockDStream = mock[DStream[(Int, String)]]
    // the implicit make the following line valid
    val pairDStream: GemFirePairDStreamFunctions[Int, String] = mockDStream
    pairDStream shouldBe a[GemFirePairDStreamFunctions[_, _]]
  }

  test("test GemFireDStreamFunctions Implicit") {
    import io.snappydata.spark.gemfire.connector.streaming._
    val mockDStream = mock[DStream[String]]
    // the implicit make the following line valid
    val dstream: GemFireDStreamFunctions[String] = mockDStream
    dstream shouldBe a[GemFireDStreamFunctions[_]]
  }

  def createMocks[K, V](regionPath: String)
      (implicit kt: ClassTag[K], vt: ClassTag[V], m: Manifest[Region[K, V]])
  : (String, GemFireConnection, Region[K, V]) = {
    val mockConnection = mock[GemFireConnection]
    val mockRegion = mock[Region[K, V]]
    (regionPath, mockConnection, mockRegion)
  }

  test("test GemFirePairDStreamFunctions.saveToGemFire()") {
    import io.snappydata.spark.gemfire.connector.streaming._
    val (regionPath, mockConnection, mockRegion) = createMocks[String, String]("test")
    val mockDStream = mock[DStream[(String, String)]]
    mockDStream.saveToGemFire(regionPath)
    verify(mockConnection).validateRegion[String, String](regionPath)
    verify(mockDStream).foreachRDD(mockAny[(RDD[(String, String)]) => Unit])
  }

  test("test GemFireDStreamFunctions.saveToGemFire()") {
    import io.snappydata.spark.gemfire.connector.streaming._
    val (regionPath, mockConnection, mockRegion) = createMocks[String, Int]("test")
    val mockDStream = mock[DStream[String]]
    mockDStream.saveToGemFire[String, Int](regionPath, (s: String) => (s, s.length))
    verify(mockConnection).validateRegion[String, String](regionPath)
    verify(mockDStream).foreachRDD(mockAny[(RDD[String]) => Unit])
  }

}
