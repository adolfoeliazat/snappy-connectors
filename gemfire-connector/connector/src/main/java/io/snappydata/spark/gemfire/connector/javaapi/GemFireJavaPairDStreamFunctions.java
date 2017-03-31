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

import java.util.Properties;

import io.snappydata.spark.gemfire.connector.streaming.GemFirePairDStreamFunctions;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import static io.snappydata.spark.gemfire.connector.javaapi.JavaAPIHelper.emptyStrStrMap;
import static io.snappydata.spark.gemfire.connector.javaapi.JavaAPIHelper.propertiesToScalaMap;

/**
 * A Java API wrapper over {@link org.apache.spark.streaming.api.java.JavaPairDStream}
 * to provide Geode Spark Connector functionality.
 * <p>
 * <p>To obtain an instance of this wrapper, use one of the factory methods in {@link
 * GemFireJavaUtil} class.</p>
 */
public class GemFireJavaPairDStreamFunctions<K, V> {

  public final GemFirePairDStreamFunctions<K, V> dsf;

  public GemFireJavaPairDStreamFunctions(JavaPairDStream<K, V> ds) {
    this.dsf = new GemFirePairDStreamFunctions<K, V>(ds.dstream());
  }


  /**
   * Save the JavaPairDStream to Geode key-value store.
   *
   * @param regionPath the full path of region that the DStream is stored
   * @param opConf     the optional parameters for this operation
   */
  public void saveToGemFire(String regionPath, Properties opConf) {
    dsf.saveToGemFire(regionPath, propertiesToScalaMap(opConf));
  }

  /**
   * Save the JavaPairDStream to Geode key-value store.
   *
   * @param regionPath the full path of region that the DStream is stored
   */
  public void saveToGemFire(String regionPath) {
    dsf.saveToGemFire(regionPath, emptyStrStrMap());
  }

}
