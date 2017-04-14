/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 */

package org.apache.spark.sql.sources.connector.gemfire

import java.beans.Introspector

import scala.reflect.ClassTag

import io.snappydata.spark.gemfire.connector.internal.rdd.GemFireRegionRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GenericRow}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, JavaTypeInference}
import org.apache.spark.sql.collection.{Utils => OtherUtils}
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SQLContext, SaveMode, SnappyContext, _}
import org.apache.spark.util.{Utils => MainUtils}

case class GemFireRelation(@transient override val sqlContext: SnappyContext, regionPath: String,
    primaryKeyColumnName: Option[String], valueColumnName: Option[String],
    keyConstraint: Option[String], valueConstraint: Option[String])
    extends BaseRelation with TableScan {

  private val keyTag = ClassTag[Any](keyConstraint.map(MainUtils.classForName(_)).
      getOrElse(classOf[Any]))
  private val valueTag = ClassTag[Any](valueConstraint.map(MainUtils.classForName(_)).
      getOrElse(classOf[Any]))

  override def buildScan(): RDD[Row] = {
    val rdd = GemFireRegionRDD(sqlContext.sparkContext, regionPath,
      Map.empty[String, String])(keyTag, valueTag)
    rdd.mapPartitions(iter => {
      val keyClass = keyConstraint.map(MainUtils.classForName(_))
      val valueClass = valueConstraint.map(MainUtils.classForName(_))
      val (totalSize, keyConverter, valueConverter) = GemFireRelation.getLengthAndConverters(
        keyClass, valueClass)
      iter.map { case (k, v) => {
        val array = Array.ofDim[Any](totalSize)
        keyConverter(k, array)
        valueConverter(v, array)
        new GenericRow(array): Row

      }
      }
    }, true)
  }

  override val schema = {
    val (inferedKeyType, nullableKey) = JavaTypeInference.inferDataType(keyTag.runtimeClass)
    val (inferedValType, nullableValue) = JavaTypeInference.inferDataType(valueTag.runtimeClass)

    val keyStructFields = inferedKeyType match {
      case x: StructType => x.fields
      case _ => Array(StructField(primaryKeyColumnName.getOrElse(Constants.defaultKeyColumnName),
        inferedKeyType, nullableKey))
    }
    val valueStructFields = inferedValType match {
      case x: StructType => x.fields
      case _ => Array(StructField(valueColumnName.getOrElse(Constants.defaultValueColumnName),
        inferedValType, nullableValue))
    }
    StructType(keyStructFields ++ valueStructFields)

  }
}

object GemFireRelation {

  private def getSchema(beanClass: Class[_]): Seq[AttributeReference] = {
    val (dataType, _) = JavaTypeInference.inferDataType(beanClass)
    dataType.asInstanceOf[StructType].fields.map { f =>
      AttributeReference(f.name, f.dataType, f.nullable)()
    }
  }

  private def getLengthAndConverters(keyClass: Option[Class[_]], valueClass: Option[Class[_]]):
  (Int, (Any, Array[Any]) => Unit, (Any, Array[Any]) => Unit) = {

    val (keyLength, keyConverter) = keyClass.map(className => {
      val keyType = inferDataType(className)
      if (keyType.isDefined) {
        (1, (e: Any, array: Array[Any]) => {
          array(0) = e
        })
      } else {
        getLengthAndExtractorForBeanClass(className, 0)
      }
    }).getOrElse((0, (e: Any, arr: Array[Any]) => {}))


    val (valueLength, valueConverter) = valueClass.map(className => {
      val valType = inferDataType(className)
      if (valType.isDefined) {
        (1, (e: Any, array: Array[Any]) => {
          array(keyLength) = e
        })
      } else {
        getLengthAndExtractorForBeanClass(className, keyLength)
      }
    }).getOrElse((0, (e: Any, arr: Array[Any]) => {}))

    (keyLength + valueLength, keyConverter, valueConverter)
  }


  private def getLengthAndExtractorForBeanClass(clazz: Class[_], startIndex: Int):
  (Int, (Any, Array[Any]) => Unit) = {

    val className = clazz.getName
    val attributeSeq: Seq[AttributeReference] = GemFireRelation.getSchema(clazz)

    // BeanInfo is not serializable so we must rediscover it remotely for each partition.
    val localBeanInfo = Introspector.getBeanInfo(clazz)


    val extractors = localBeanInfo.getPropertyDescriptors.
        filterNot(_.getName == "class").map(_.getReadMethod)
    val methodsToConvert = extractors.zip(attributeSeq).map { case (e, attr) =>
      attr.dataType match {
        case strct: StructType => {
          val (length, cnvrtr) = getLengthAndExtractorForBeanClass(e.getReturnType, 0)
          val arr = Array.ofDim[Any](length)
          (e, (x: Any) => {
             cnvrtr(x, arr)
             new GenericRow(arr)
          })
        }
        case _ => (e, CatalystTypeConverters.createToCatalystConverter(attr.dataType))
      }

    }
    val length = attributeSeq.size
    var index = startIndex
    (length, (e: Any, array: Array[Any]) => {
      methodsToConvert.foreach {
        case (m, convert) => {
          array(index) = convert(m.invoke(e))
          index += 1
        }
      }
      index = startIndex
    })

  }


  private def inferDataType(c: Class[_]): Option[DataType] = {
    c match {

      case c: Class[_] if c == classOf[String] => Some(StringType)
      case c: Class[_] if c == classOf[java.lang.Short] => Some(ShortType)
      case c: Class[_] if c == classOf[Short] => Some(ShortType)
      case c: Class[_] if c == classOf[java.lang.Integer] => Some(IntegerType)
      case c: Class[_] if c == classOf[Int] => Some(IntegerType)
      case c: Class[_] if c == classOf[java.lang.Long] => Some(LongType)
      case c: Class[_] if c == classOf[Long] => Some(LongType)
      case c: Class[_] if c == classOf[java.lang.Double] => Some(DoubleType)
      case c: Class[_] if c == classOf[Double] => Some(DoubleType)
      case c: Class[_] if c == classOf[java.lang.Byte] => Some(ByteType)
      case c: Class[_] if c == classOf[Byte] => Some(ByteType)
      case c: Class[_] if c == classOf[java.lang.Float] => Some(FloatType)
      case c: Class[_] if c == classOf[Float] => Some(FloatType)
      case c: Class[_] if c == classOf[java.lang.Boolean] => Some(BooleanType)
      case c: Class[_] if c == classOf[Boolean] => Some(BooleanType)
      case c: Class[_] if c == classOf[java.math.BigDecimal] => Some(DecimalType.SYSTEM_DEFAULT)
      case c: Class[_] if c == classOf[BigDecimal] => Some(DecimalType.SYSTEM_DEFAULT)
      case c: Class[_] if c == classOf[java.math.BigInteger] => Some(DecimalType.BigIntDecimal)
      case c: Class[_] if c == classOf[BigInt] => Some(DecimalType.BigIntDecimal)
      case c: Class[_] if c == classOf[java.sql.Date] => Some(DateType)
      case c: Class[_] if c == classOf[java.sql.Timestamp] => Some(TimestampType)
      case _ => None
    }

  }


}

final class DefaultSource
    extends RelationProvider with SchemaRelationProvider with DataSourceRegister {

  // with CreatableRelationProvider

  def shortName(): String = "GemFire"

  def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], schemaOpt: Option[StructType],
      asSelect: Boolean): BaseRelation = {

    val params = new CaseInsensitiveMutableHashMap(options)

    val snc = sqlContext.asInstanceOf[SnappyContext]
    val regionPath = params.getOrElse(Constants.REGION_PATH, throw OtherUtils.analysisException(
      "GemFire Region Path is missing"))
    val pkColumnName = params.get(Constants.PRIMARY_KEY_COLUMN_NAME)
    val valueColumnName = params.get(Constants.VALUE_COLUMN_NAME)
    val kc = params.get(Constants.keyConstraintClass)
    val vc = params.get(Constants.valueConstraintClass)
    if (kc.isEmpty && vc.isEmpty) {
      OtherUtils.analysisException("Either Key Class  or value class  " +
          "need to be provided for the table definition")
    }
    GemFireRelation(snc, regionPath, pkColumnName, valueColumnName, kc, vc)
  }

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String]): BaseRelation = {

    val allowExisting = options.get(JdbcExtendedUtils
        .ALLOW_EXISTING_PROPERTY).exists(_.toBoolean)
    val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists
    createRelation(sqlContext, mode, options, None, asSelect = false)
  }

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String], schema: StructType): BaseRelation = {

    val allowExisting = options.get(JdbcExtendedUtils
        .ALLOW_EXISTING_PROPERTY).exists(_.toBoolean)
    val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists
    createRelation(sqlContext, mode, options, Some(schema), asSelect = false)
  }

  /*
  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], data: DataFrame): BaseRelation = {
    val relation = createRelation(sqlContext, mode, options, Some(data.schema),
      asSelect = true)
    var success = false
    try {
      relation.insert(data, mode == SaveMode.Overwrite)
      success = true
      relation
    } finally {
      if (!success && !relation.tableExists) {
        relation.destroy(ifExists = true)
      }
    }
  }
  */
}
