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

import io.snappydata.spark.gemfire.connector.internal.rdd.GemFireRegionRDD;
import io.snappydata.spark.gemfire.connector.internal.rdd.GemFireRegionRDD$;
import org.apache.spark.SparkContext;
import scala.reflect.ClassTag;

import static io.snappydata.spark.gemfire.connector.javaapi.JavaAPIHelper.fakeClassTag;
import static io.snappydata.spark.gemfire.connector.javaapi.JavaAPIHelper.propertiesToScalaMap;

/**
 * Java API wrapper over {@link org.apache.spark.SparkContext} to provide Geode
 * Connector functionality.
 * <p>
 * <p></p>To obtain an instance of this wrapper, use one of the factory methods in {@link
 * GemFireJavaUtil} class.</p>
 */
public class GemFireJavaSparkContextFunctions {

  public final SparkContext sc;

  public GemFireJavaSparkContextFunctions(SparkContext sc) {
    this.sc = sc;
  }

  /**
   * Expose a Geode region as a JavaPairRDD with default GeodeConnector.
   *
   * @param regionPath the full path of the region
   * @param opConf     the parameters for this operation, such as preferred partitioner.
   */
  public <K, V> GemFireJavaRegionRDD<K, V> gemfireRegion(String regionPath, Properties opConf) {

    ClassTag<K> kt = fakeClassTag();
    ClassTag<V> vt = fakeClassTag();

    GemFireRegionRDD<K, V, scala.Tuple2<K, V>> rdd = GemFireRegionRDD$.MODULE$.exposeRegion(
        sc, regionPath, propertiesToScalaMap(opConf), kt, vt);
    return new GemFireJavaRegionRDD<>(rdd);
  }

  /**
   * Expose a Geode region as a JavaPairRDD with default GeodeConnector and no preferred partitioner.
   *
   * @param regionPath the full path of the region
   */
  public <K, V> GemFireJavaRegionRDD<K, V> gemfireRegion(String regionPath) {

    return gemfireRegion(regionPath, new Properties());
  }


}
