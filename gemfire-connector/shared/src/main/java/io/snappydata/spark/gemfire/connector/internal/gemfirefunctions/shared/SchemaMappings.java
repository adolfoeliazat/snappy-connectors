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


public interface SchemaMappings {
  byte stringg = 0;
  byte shortt = 1;
  byte intt = 2;
  byte longg = 3;
  byte doublee= 4;
  byte bytee = 5;
  byte floatt = 6;
  byte booll = 7;
  byte binary = 8;
  byte datee = 10;
  byte timestampp = 11;
  byte structtypee = 12;
  byte unoptimizedtype = 13;

  /*

  Unoptmized types include, BigDecimal,

   */
}
