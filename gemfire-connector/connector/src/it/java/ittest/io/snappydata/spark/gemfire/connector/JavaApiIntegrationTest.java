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
package ittest.io.snappydata.spark.gemfire.connector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import io.snappydata.spark.gemfire.connector.GemFireConnection;
import io.snappydata.spark.gemfire.connector.internal.DefaultGemFireConnectionManager$;
import io.snappydata.spark.gemfire.connector.javaapi.GemFireJavaRegionRDD;
import ittest.io.snappydata.spark.gemfire.connector.testkit.GemFireCluster$;
import ittest.io.snappydata.spark.gemfire.connector.testkit.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.scalatest.junit.JUnitSuite;
import scala.Option;
import scala.Some;
import scala.Tuple2;

import static io.snappydata.spark.gemfire.connector.javaapi.GemFireJavaUtil.RDDSaveBatchSizePropKey;
import static io.snappydata.spark.gemfire.connector.javaapi.GemFireJavaUtil.javaFunctions;
import static org.junit.Assert.assertTrue;

public class JavaApiIntegrationTest extends JUnitSuite {

  static JavaSparkContext jsc = null;
  static int numServers = 2;
  static int numObjects = 1000;
  static String regionPath = "pr_str_int_region";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // start gemfire cluster, and spark context
    Properties settings = new Properties();
    settings.setProperty(DistributionConfig.CACHE_XML_FILE_NAME,
        "src/it/resources/test-retrieve-regions.xml");
    settings.setProperty("num-of-servers", Integer.toString(numServers));
    int locatorPort = GemFireCluster$.MODULE$.start(settings);

    // start spark context in local mode
    Properties props = new Properties();
    props.put("log4j.logger.org.apache.spark", "INFO");
    props.put("log4j.logger.io.snappydata.spark.gemfire.connector", "DEBUG");
    IOUtils.configTestLog4j("ERROR", props);
    SparkConf conf = new SparkConf()
        .setAppName("RetrieveRegionIntegrationTest")
        .setMaster("local[2]")
        .set(DistributionConfig.REMOTE_LOCATORS_NAME, "localhost:" + locatorPort);
    // sc = new SparkContext(conf);
    jsc = new JavaSparkContext(conf);

  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    // stop connection, spark context, and gemfire cluster
    DefaultGemFireConnectionManager$.MODULE$.closeConnection();
    jsc.stop();
    GemFireCluster$.MODULE$.stop();
  }

  // --------------------------------------------------------------------------------------------
  //   utility methods
  // --------------------------------------------------------------------------------------------

  private <K, V> void matchMapAndPairList(Map<K, V> map, List<Tuple2<K, V>> list) {
    assertTrue("size mismatch \nmap: " + map.toString() + "\nlist: " + list.toString(), map.size() == list.size());
    for (Tuple2<K, V> p : list) {
      assertTrue("value mismatch: k=" + p._1() + " v1=" + p._2() + " v2=" + map.get(p._1()),
          p._2().equals(map.get(p._1())));
    }
  }

  private Region<String, Integer> prepareStrIntRegion(String regionPath, int start, int stop) {
    HashMap<String, Integer> entriesMap = new HashMap<>();
    for (int i = start; i < stop; i++) {
      entriesMap.put("k_" + i, i);
    }

    GemFireConnection conn = DefaultGemFireConnectionManager$.MODULE$.getConnection();
    Region<String, Integer> region = conn.getRegionProxy(regionPath);
    for (Object key : region.keySetOnServer()) {
      region.remove(key);
    }
    region.putAll(entriesMap);
    return region;
  }

  private JavaPairRDD<String, Integer> prepareStrIntJavaPairRDD(int start, int stop) {
    List<Tuple2<String, Integer>> data = new ArrayList<>();
    for (int i = start; i < stop; i++) {
      data.add(new Tuple2<>("k_" + i, i));
    }
    return jsc.parallelizePairs(data);
  }

  private JavaPairRDD<Integer, Integer> prepareIntIntJavaPairRDD(int start, int stop) {
    List<Tuple2<Integer, Integer>> data = new ArrayList<>();
    for (int i = start; i < stop; i++) {
      data.add(new Tuple2<>(i, i * 2));
    }
    return jsc.parallelizePairs(data);
  }

  private JavaRDD<Integer> prepareIntJavaRDD(int start, int stop) {
    List<Integer> data = new ArrayList<>();
    for (int i = start; i < stop; i++) {
      data.add(i);
    }
    return jsc.parallelize(data);
  }

  // --------------------------------------------------------------------------------------------
  //   JavaRDD.saveToGemFire
  // --------------------------------------------------------------------------------------------

  @Test
  public void testRDDSaveToGemFireWithDefaultConnConfAndOpConf() throws Exception {
    verifyRDDSaveToGemFire(true, true);
  }

  @Test
  public void testRDDSaveToGemFireWithDefaultConnConf() throws Exception {
    verifyRDDSaveToGemFire(true, false);
  }

  @Test
  public void testRDDSaveToGemFireWithConnConfAndOpConf() throws Exception {
    verifyRDDSaveToGemFire(false, true);
  }

  @Test
  public void testRDDSaveToGemFireWithConnConf() throws Exception {
    verifyRDDSaveToGemFire(false, false);
  }

  public void verifyRDDSaveToGemFire(boolean useDefaultConnConf, boolean useOpConf) throws Exception {
    Region<String, Integer> region = prepareStrIntRegion(regionPath, 0, 0);  // remove all entries
    JavaRDD<Integer> rdd1 = prepareIntJavaRDD(0, numObjects);

    PairFunction<Integer, String, Integer> func = new IntToStrIntPairFunction();
    Properties opConf = new Properties();
    opConf.put(RDDSaveBatchSizePropKey, "200");

    if (useDefaultConnConf) {
      if (useOpConf)
        javaFunctions(rdd1).saveToGemFire(regionPath, func, opConf);
      else
        javaFunctions(rdd1).saveToGemFire(regionPath, func);
    } else {
      if (useOpConf)
        javaFunctions(rdd1).saveToGemFire(regionPath, func, opConf);
      else
        javaFunctions(rdd1).saveToGemFire(regionPath, func);
    }

    Set<String> keys = region.keySetOnServer();
    Map<String, Integer> map = region.getAll(keys);

    List<Tuple2<String, Integer>> expectedList = new ArrayList<>();

    for (int i = 0; i < numObjects; i++) {
      expectedList.add(new Tuple2<>("k_" + i, i));
    }
    matchMapAndPairList(map, expectedList);
  }

  @Test
  public void testPairRDDSaveToGemFireWithDefaultConnConfAndOpConf() throws Exception {
    verifyPairRDDSaveToGemFire(true, true);
  }

  // --------------------------------------------------------------------------------------------
  //   JavaPairRDD.saveToGemFire
  // --------------------------------------------------------------------------------------------

  @Test
  public void testPairRDDSaveToGemFireWithDefaultConnConf() throws Exception {
    verifyPairRDDSaveToGemFire(true, false);
  }

  @Test
  public void testPairRDDSaveToGemFireWithConnConfAndOpConf() throws Exception {
    verifyPairRDDSaveToGemFire(false, true);
  }

  @Test
  public void testPairRDDSaveToGemFireWithConnConf() throws Exception {
    verifyPairRDDSaveToGemFire(false, false);
  }

  public void verifyPairRDDSaveToGemFire(boolean useDefaultConnConf, boolean useOpConf) throws Exception {
    Region<String, Integer> region = prepareStrIntRegion(regionPath, 0, 0);  // remove all entries
    JavaPairRDD<String, Integer> rdd1 = prepareStrIntJavaPairRDD(0, numObjects);
    Properties opConf = new Properties();
    opConf.put(RDDSaveBatchSizePropKey, "200");

    if (useDefaultConnConf) {
      if (useOpConf)
        javaFunctions(rdd1).saveToGemFire(regionPath, opConf);
      else
        javaFunctions(rdd1).saveToGemFire(regionPath);
    } else {
      if (useOpConf)
        javaFunctions(rdd1).saveToGemFire(regionPath, opConf);
      else
        javaFunctions(rdd1).saveToGemFire(regionPath);
    }

    Set<String> keys = region.keySetOnServer();
    Map<String, Integer> map = region.getAll(keys);

    List<Tuple2<String, Integer>> expectedList = new ArrayList<>();
    for (int i = 0; i < numObjects; i++) {
      expectedList.add(new Tuple2<>("k_" + i, i));
    }
    matchMapAndPairList(map, expectedList);
  }

  @Test
  public void testJavaSparkContextGemFireRegion() throws Exception {
    prepareStrIntRegion(regionPath, 0, numObjects);  // remove all entries
    Properties emptyProps = new Properties();
    GemFireJavaRegionRDD<String, Integer> rdd1 = javaFunctions(jsc).gemfireRegion(regionPath);
    GemFireJavaRegionRDD<String, Integer> rdd2 = javaFunctions(jsc).gemfireRegion(regionPath, emptyProps);
    GemFireJavaRegionRDD<String, Integer> rdd3 = javaFunctions(jsc).gemfireRegion(regionPath);
    GemFireJavaRegionRDD<String, Integer> rdd4 = javaFunctions(jsc).gemfireRegion(regionPath, emptyProps);
    GemFireJavaRegionRDD<String, Integer> rdd5 = rdd1.where("value.intValue() < 50");

    HashMap<String, Integer> expectedMap = new HashMap<>();
    for (int i = 0; i < numObjects; i++) {
      expectedMap.put("k_" + i, i);
    }

    matchMapAndPairList(expectedMap, rdd1.collect());
    matchMapAndPairList(expectedMap, rdd2.collect());
    matchMapAndPairList(expectedMap, rdd3.collect());
    matchMapAndPairList(expectedMap, rdd4.collect());

    HashMap<String, Integer> expectedMap2 = new HashMap<>();
    for (int i = 0; i < 50; i++) {
      expectedMap2.put("k_" + i, i);
    }

    matchMapAndPairList(expectedMap2, rdd5.collect());
  }

  // --------------------------------------------------------------------------------------------
  //   JavaSparkContext.gemfireRegion and where clause
  // --------------------------------------------------------------------------------------------

  @Test
  public void testPairRDDJoinWithSameKeyType() throws Exception {
    prepareStrIntRegion(regionPath, 0, numObjects);
    JavaPairRDD<String, Integer> rdd1 = prepareStrIntJavaPairRDD(-5, 10);

    JavaPairRDD<Tuple2<String, Integer>, Integer> rdd2a = javaFunctions(rdd1).joinGemFireRegion(regionPath);
    JavaPairRDD<Tuple2<String, Integer>, Integer> rdd2b = javaFunctions(rdd1).joinGemFireRegion(regionPath);
    // System.out.println("=== Result RDD =======\n" + rdd2a.collect() + "\n=========================");

    HashMap<Tuple2<String, Integer>, Integer> expectedMap = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      expectedMap.put(new Tuple2<>("k_" + i, i), i);
    }
    matchMapAndPairList(expectedMap, rdd2a.collect());
    matchMapAndPairList(expectedMap, rdd2b.collect());
  }

  // --------------------------------------------------------------------------------------------
  //   JavaPairRDD.joinGemFireRegion
  // --------------------------------------------------------------------------------------------

  @Test
  public void testPairRDDJoinWithDiffKeyType() throws Exception {
    prepareStrIntRegion(regionPath, 0, numObjects);
    JavaPairRDD<Integer, Integer> rdd1 = prepareIntIntJavaPairRDD(-5, 10);
    Function<Tuple2<Integer, Integer>, String> func = new IntIntPairToStrKeyFunction();

    JavaPairRDD<Tuple2<Integer, Integer>, Integer> rdd2a = javaFunctions(rdd1).joinGemFireRegion(regionPath, func);
    JavaPairRDD<Tuple2<Integer, Integer>, Integer> rdd2b = javaFunctions(rdd1).joinGemFireRegion(regionPath, func);
    //System.out.println("=== Result RDD =======\n" + rdd2a.collect() + "\n=========================");

    HashMap<Tuple2<Integer, Integer>, Integer> expectedMap = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      expectedMap.put(new Tuple2<>(i, i * 2), i);
    }
    matchMapAndPairList(expectedMap, rdd2a.collect());
    matchMapAndPairList(expectedMap, rdd2b.collect());
  }

  @Test
  public void testPairRDDOuterJoinWithSameKeyType() throws Exception {
    prepareStrIntRegion(regionPath, 0, numObjects);
    JavaPairRDD<String, Integer> rdd1 = prepareStrIntJavaPairRDD(-5, 10);

    JavaPairRDD<Tuple2<String, Integer>, Option<Integer>> rdd2a = javaFunctions(rdd1).outerJoinGemFireRegion(regionPath);
    JavaPairRDD<Tuple2<String, Integer>, Option<Integer>> rdd2b = javaFunctions(rdd1).outerJoinGemFireRegion(regionPath);
    //System.out.println("=== Result RDD =======\n" + rdd2a.collect() + "\n=========================");

    HashMap<Tuple2<String, Integer>, Option<Integer>> expectedMap = new HashMap<>();
    for (int i = -5; i < 10; i++) {
      if (i < 0)
        expectedMap.put(new Tuple2<>("k_" + i, i), Option.apply((Integer)null));
      else
        expectedMap.put(new Tuple2<>("k_" + i, i), Some.apply(i));
    }
    matchMapAndPairList(expectedMap, rdd2a.collect());
    matchMapAndPairList(expectedMap, rdd2b.collect());
  }

  @Test
  public void testPairRDDOuterJoinWithDiffKeyType() throws Exception {
    prepareStrIntRegion(regionPath, 0, numObjects);
    JavaPairRDD<Integer, Integer> rdd1 = prepareIntIntJavaPairRDD(-5, 10);
    Function<Tuple2<Integer, Integer>, String> func = new IntIntPairToStrKeyFunction();

    JavaPairRDD<Tuple2<Integer, Integer>, Option<Integer>> rdd2a = javaFunctions(rdd1).outerJoinGemFireRegion(regionPath, func);
    JavaPairRDD<Tuple2<Integer, Integer>, Option<Integer>> rdd2b = javaFunctions(rdd1).outerJoinGemFireRegion(regionPath, func);
    //System.out.println("=== Result RDD =======\n" + rdd2a.collect() + "\n=========================");

    HashMap<Tuple2<Integer, Integer>, Option<Integer>> expectedMap = new HashMap<>();
    for (int i = -5; i < 10; i++) {
      if (i < 0)
        expectedMap.put(new Tuple2<>(i, i * 2), Option.apply((Integer)null));
      else
        expectedMap.put(new Tuple2<>(i, i * 2), Some.apply(i));
    }
    matchMapAndPairList(expectedMap, rdd2a.collect());
    matchMapAndPairList(expectedMap, rdd2b.collect());
  }

  // --------------------------------------------------------------------------------------------
  //   JavaPairRDD.outerJoinGemFireRegion
  // --------------------------------------------------------------------------------------------

  @Test
  public void testRDDJoinWithSameKeyType() throws Exception {
    prepareStrIntRegion(regionPath, 0, numObjects);
    JavaRDD<Integer> rdd1 = prepareIntJavaRDD(-5, 10);

    Function<Integer, String> func = new IntToStrKeyFunction();
    JavaPairRDD<Integer, Integer> rdd2a = javaFunctions(rdd1).joinGemFireRegion(regionPath, func);
    JavaPairRDD<Integer, Integer> rdd2b = javaFunctions(rdd1).joinGemFireRegion(regionPath, func);
    //System.out.println("=== Result RDD =======\n" + rdd2a.collect() + "\n=========================");

    HashMap<Integer, Integer> expectedMap = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      expectedMap.put(i, i);
    }
    matchMapAndPairList(expectedMap, rdd2a.collect());
    matchMapAndPairList(expectedMap, rdd2b.collect());
  }

  @Test
  public void testRDDOuterJoinWithSameKeyType() throws Exception {
    prepareStrIntRegion(regionPath, 0, numObjects);
    JavaRDD<Integer> rdd1 = prepareIntJavaRDD(-5, 10);

    Function<Integer, String> func = new IntToStrKeyFunction();
    JavaPairRDD<Integer, Option<Integer>> rdd2a = javaFunctions(rdd1).outerJoinGemFireRegion(regionPath, func);
    JavaPairRDD<Integer, Option<Integer>> rdd2b = javaFunctions(rdd1).outerJoinGemFireRegion(regionPath, func);
    //System.out.println("=== Result RDD =======\n" + rdd2a.collect() + "\n=========================");

    HashMap<Integer, Option<Integer>> expectedMap = new HashMap<>();
    for (int i = -5; i < 10; i++) {
      if (i < 0)
        expectedMap.put(i, Option.apply((Integer)null));
      else
        expectedMap.put(i, Some.apply(i));
    }
    matchMapAndPairList(expectedMap, rdd2a.collect());
    matchMapAndPairList(expectedMap, rdd2b.collect());
  }

  // --------------------------------------------------------------------------------------------
  //   JavaRDD.joinGemFireRegion
  // --------------------------------------------------------------------------------------------

  static class IntToStrIntPairFunction implements PairFunction<Integer, String, Integer> {
    @Override
    public Tuple2<String, Integer> call(Integer x) throws Exception {
      return new Tuple2<>("k_" + x, x);
    }
  }

  static class IntIntPairToStrKeyFunction implements Function<Tuple2<Integer, Integer>, String> {
    @Override
    public String call(Tuple2<Integer, Integer> pair) throws Exception {
      return "k_" + pair._1();
    }
  }

  // --------------------------------------------------------------------------------------------
  //   JavaRDD.outerJoinGemFireRegion
  // --------------------------------------------------------------------------------------------

  static class IntToStrKeyFunction implements Function<Integer, String> {
    @Override
    public String call(Integer x) throws Exception {
      return "k_" + x;
    }
  }

}
