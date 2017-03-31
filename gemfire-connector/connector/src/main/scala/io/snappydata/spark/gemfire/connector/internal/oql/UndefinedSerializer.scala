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

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.gemstone.gemfire.cache.query.QueryService
import com.gemstone.gemfire.cache.query.internal.Undefined

/**
  * This is the customized serializer to serialize QueryService.UNDEFINED,
  * i.e. com.gemstone.gemfire.cache.query.internal.Undefined, in order to
  * guarantee the singleton Undefined after its deserialization within Spark.
  */
class UndefinedSerializer extends Serializer[Undefined] {

  def write(kryo: Kryo, output: Output, u: Undefined) {
    //Only serialize a byte for Undefined
    output.writeByte(u.getDSFID)
  }

  def read(kryo: Kryo, input: Input, tpe: Class[Undefined]): Undefined = {
    //Read DSFID of Undefined
    input.readByte()
    QueryService.UNDEFINED match {
      case null => new Undefined
      case _ =>
        //Avoid calling Undefined constructor again.
        QueryService.UNDEFINED.asInstanceOf[Undefined]
    }
  }
}
