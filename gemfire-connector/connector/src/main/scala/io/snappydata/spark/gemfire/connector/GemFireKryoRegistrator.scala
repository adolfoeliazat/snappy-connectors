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
package io.snappydata.spark.gemfire.connector

import com.esotericsoftware.kryo.Kryo
import com.gemstone.gemfire.cache.query.internal.Undefined
import io.snappydata.spark.gemfire.connector.internal.oql.UndefinedSerializer

import org.apache.spark.serializer.KryoRegistrator

class GemFireKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kyro: Kryo): Unit = {
    kyro.addDefaultSerializer(classOf[Undefined], classOf[UndefinedSerializer])
  }
}
