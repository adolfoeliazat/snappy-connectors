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

import io.snappydata.spark.gemfire.connector._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSuite, Matchers}

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

class ConnectorImplicitsTest extends FunSuite with Matchers with MockitoSugar {

  test("implicit map2Properties") {
    verifyProperties(Map.empty)
    verifyProperties(Map("One" -> "1", "Two" -> "2", "Three" -> "3"))
  }

  def verifyProperties(map: Map[String, String]): Unit = {
    val props: java.util.Properties = map
    assert(props.size() == map.size)
    map.foreach(p => assert(props.getProperty(p._1) == p._2))
  }

  test("Test Implicit SparkContext Conversion") {
    val mockSparkContext = mock[SparkContext]
    val gfscf: GemFireSparkContextFunctions = mockSparkContext
    assert(gfscf.isInstanceOf[GemFireSparkContextFunctions])
  }

  test("Test Implicit SQLContext Conversion") {
    val mockSQLContext = mock[SQLContext]
    val gfscf: GemFireSQLContextFunctions = mockSQLContext
    assert(gfscf.isInstanceOf[GemFireSQLContextFunctions])
  }
}
