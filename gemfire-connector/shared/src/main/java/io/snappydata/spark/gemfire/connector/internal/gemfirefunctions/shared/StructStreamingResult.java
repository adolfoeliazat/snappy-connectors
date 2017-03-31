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
package io.snappydata.spark.gemfire.connector.internal.gemfirefunctions.shared;


import com.gemstone.gemfire.cache.query.internal.types.ObjectTypeImpl;
import com.gemstone.gemfire.cache.query.internal.types.StructTypeImpl;
import com.gemstone.gemfire.cache.query.types.ObjectType;

public interface StructStreamingResult {
  byte TYPE_CHUNK = 0x30;
  byte DATA_CHUNK = 0x31;
  byte ERROR_CHUNK = 0x32;
  byte SER_DATA = 0x41;
  byte UNSER_DATA = 0x42;
  byte BYTEARR_DATA = 0x43;

  StructTypeImpl KeyValueType = new StructTypeImpl(new String[]{"key", "value"},
      new ObjectType[]{new ObjectTypeImpl(java.lang.Object.class), new ObjectTypeImpl(java.lang.Object.class)});
}
