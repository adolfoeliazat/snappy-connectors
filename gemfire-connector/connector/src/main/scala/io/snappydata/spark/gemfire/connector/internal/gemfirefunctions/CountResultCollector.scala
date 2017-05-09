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

import java.util.concurrent.TimeUnit

import com.gemstone.gemfire.cache.execute.{FunctionException, ResultCollector}
import com.gemstone.gemfire.distributed.DistributedMember

class CountResultCollector extends ResultCollector[Long, Long]{
  @volatile
  private var total = 0L

  def addResult(memberID: DistributedMember, partialResult: Long) {
    total += partialResult
  }

  @throws[FunctionException]
  def getResult: Long = total

  @throws[FunctionException]
  @throws[InterruptedException]
  def getResult(timeout: Long, unit: TimeUnit): Long = throw new AssertionError("getResult with " +
      "timeout not expected to be invoked for connector")

  def clearResults() {
    this.total = 0
  }

  def endResults() {}
}
