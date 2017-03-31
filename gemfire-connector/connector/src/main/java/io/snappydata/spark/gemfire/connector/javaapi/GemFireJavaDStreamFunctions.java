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

import io.snappydata.spark.gemfire.connector.streaming.GemFireDStreamFunctions;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;

import static io.snappydata.spark.gemfire.connector.javaapi.JavaAPIHelper.emptyStrStrMap;
import static io.snappydata.spark.gemfire.connector.javaapi.JavaAPIHelper.propertiesToScalaMap;

/**
 * A Java API wrapper over {@link org.apache.spark.streaming.api.java.JavaDStream}
 * to provide Geode Spark Connector functionality.
 * <p>
 * <p>To obtain an instance of this wrapper, use one of the factory methods in {@link
 * GemFireJavaUtil} class.</p>
 */
public class GemFireJavaDStreamFunctions<T> {

  public final GemFireDStreamFunctions<T> dsf;

  public GemFireJavaDStreamFunctions(JavaDStream<T> ds) {
    this.dsf = new GemFireDStreamFunctions<T>(ds.dstream());
  }


  /**
   * Save the JavaDStream to Geode key-value store.
   *
   * @param regionPath the full path of region that the DStream is stored
   * @param func       the PairFunction that converts elements of JavaDStream to key/value pairs
   * @param opConf     the optional  parameters for this operation
   */
  public <K, V> void saveToGemFire(
      String regionPath, PairFunction<T, K, V> func, Properties opConf) {
    dsf.saveToGemFire(regionPath, func, propertiesToScalaMap(opConf));
  }


  /**
   * Save the JavaDStream to Geode key-value store.
   *
   * @param regionPath the full path of region that the DStream is stored
   * @param func       the PairFunction that converts elements of JavaDStream to key/value pairs
   */
  public <K, V> void saveToGemFire(
      String regionPath, PairFunction<T, K, V> func) {
    dsf.saveToGemFire(regionPath, func, emptyStrStrMap());
  }

}
