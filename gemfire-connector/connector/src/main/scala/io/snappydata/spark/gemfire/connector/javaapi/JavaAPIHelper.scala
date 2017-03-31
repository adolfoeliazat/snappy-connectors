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
package io.snappydata.spark.gemfire.connector.javaapi

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.streaming.api.java.{JavaDStream, JavaPairDStream}

/**
  * A helper class to make it possible to access components written in Scala from Java code.
  */
private[connector] object JavaAPIHelper {

  /** an empty Map[String, String] for default opConf **/
  val emptyStrStrMap: Map[String, String] = Map.empty

  /** Returns a `ClassTag` of a given runtime class. */
  def getClassTag[T](clazz: Class[T]): ClassTag[T] = ClassTag(clazz)

  /**
    * Produces a ClassTag[T], which is actually just a casted ClassTag[AnyRef].
    * see JavaSparkContext.fakeClassTag in Spark for more info.
    */
  def fakeClassTag[T]: ClassTag[T] = ClassTag.AnyRef.asInstanceOf[ClassTag[T]]

  /** Converts a Java `Properties` to a Scala immutable `Map[String, String]`. */
  def propertiesToScalaMap[K, V](props: java.util.Properties): Map[String, String] =
  Map(props.toSeq: _*)

  /** convert a JavaRDD[(K,V)] to JavaPairRDD[K,V] */
  def toJavaPairRDD[K, V](rdd: JavaRDD[(K, V)]): JavaPairRDD[K, V] =
  JavaPairRDD.fromJavaRDD(rdd)

  /** convert a JavaDStream[(K,V)] to JavaPairDStream[K,V] */
  def toJavaPairDStream[K, V](ds: JavaDStream[(K, V)]): JavaPairDStream[K, V] =
  JavaPairDStream.fromJavaDStream(ds)
}
