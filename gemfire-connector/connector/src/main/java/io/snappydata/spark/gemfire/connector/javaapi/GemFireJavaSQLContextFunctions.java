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
package io.snappydata.spark.gemfire.connector.javaapi;

import io.snappydata.spark.gemfire.connector.GemFireSQLContextFunctions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;

/**
 * Java API wrapper over {@link org.apache.spark.sql.SQLContext} to provide Geode
 * OQL functionality.
 * <p>
 * <p></p>To obtain an instance of this wrapper, use one of the factory methods in {@link
 * GemFireJavaUtil} class.</p>
 */
public class GemFireJavaSQLContextFunctions {

  public final GemFireSQLContextFunctions scf;

  public GemFireJavaSQLContextFunctions(SQLContext sqlContext) {
    scf = new GemFireSQLContextFunctions(sqlContext);
  }

  public <T> Dataset gemfireOQL(String query) {
    Dataset df = scf.gemfireOQL(query);
    return df;
  }


}
