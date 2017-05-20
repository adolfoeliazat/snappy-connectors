/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 */

package org.apache.spark.sql.sources.connector.gemfire

import java.beans.Introspector

import scala.reflect.{ClassTag, classTag}

import io.snappydata.spark.gemfire.connector.GemFireDataFrameFunctions
import io.snappydata.spark.gemfire.connector.internal.DefaultGemFireConnectionManager
import io.snappydata.spark.gemfire.connector.internal.GemFireRow
import io.snappydata.spark.gemfire.connector.internal.rdd.behaviour.ComputeLogic
import io.snappydata.spark.gemfire.connector.internal.rdd.{GemFireRDDPartition, GemFireRegionRDD}

import org.apache.spark.{Logging, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, GenericRow}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, JavaTypeInference}
import org.apache.spark.sql.collection.{Utils => OtherUtils}
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql.{SQLContext, SaveMode, SnappyContext, _}
import org.apache.spark.util.{Utils => MainUtils}

case class GemFireRelation(@transient override val sqlContext: SnappyContext,
    val regionPath: String, val primaryKeyColumnName: Option[String],
    val valueColumnName: Option[String], val keyConstraint: Option[String],
    val valueConstraint: Option[String], val providedSchema: Option[StructType],
    val asSelect: Boolean)
    extends BaseRelation with TableScan with SchemaInsertableRelation
        with PrunedFilteredScan with Logging {

  private val keyTag = ClassTag[Any](keyConstraint.map(MainUtils.classForName(_)).
      getOrElse(classOf[Any]))
  private val valueTag = ClassTag[Any](valueConstraint.map(MainUtils.classForName(_)).
      getOrElse(classOf[Any]))

  if (this.isDebugEnabled) {
    this.logDebug(s"GemFireRelation::constructor:run time class in keyTag  = "
        + keyTag.runtimeClass.getName)
    this.logDebug(s"GemFireRelation::constructor:run time class in valueTag  = "
        + valueTag.runtimeClass.getName)
  }
  val inferredKeySchema = StructType(keyConstraint.map(x => {
    // check if it matches any primitie types
    val primitiveTypeOp = GemFireRelation.inferDataType(keyTag.runtimeClass)
    if (this.isDebugEnabled) {
      this.logDebug(s"GemFireRelation::constructor: key Class " +
          s"match with scala primitive   = " + primitiveTypeOp)
    }

    val (inferedKeyType, nullableKey) = primitiveTypeOp.map((_, false)).getOrElse(
      JavaTypeInference.inferDataType(keyTag.runtimeClass))
    convertDataTypeToStructFields(inferedKeyType, nullableKey,
      primaryKeyColumnName.getOrElse(Constants.defaultKeyColumnName), providedSchema)
  }).getOrElse(Array.empty[StructField]))

  val rowObjectLength: Option[Int] = if (classOf[Row].isAssignableFrom(valueTag.runtimeClass)) {
    if (providedSchema.isDefined) {
      // there should be no embedded struct type for now
      Some(providedSchema.get.length - inferredKeySchema.length)
      /*
      if (!providedSchema.map(_.exists(f => f.dataType match {
        case _: StructType => true
        case _ => false
      })).get) {
        Some(providedSchema.get.length - inferredKeySchema.length)
      } else {
        throw OtherUtils.analysisException("Provided schema for Row objects should not have" +
            "nested struct type. For this to work, convert Row object into a Bean class")
      }
      */
    } else {
      throw OtherUtils.analysisException("To access Row objects from GemFire region, " +
          "provided a schema ")
    }
  } else {
    None
  }

  val computeRegionAsRows = (rdd: GemFireRegionRDD[Any, Any, Row]) => {
    GemFireRelation.computeForRegionAsRows[Any, Any](Some(rdd.kClassTag.runtimeClass.getName),
      Some(rdd.vClassTag.runtimeClass.getName), rdd.rowObjectLength)
  }


  val computeOQLAsRows = (rdd: GemFireRegionRDD[Any, Any, Row]) => {
    GemFireRelation.computeForOQL[Row]
  }


  val computeForCount = (rdd: GemFireRegionRDD[Any, Any, Row]) => GemFireRelation.computeForCount

  override def buildScan(): RDD[Row] = {
    if (this.isDebugEnabled) {
      logDebug("GemFireRelation: computing for empty build scan")
    }
    new GemFireRegionRDD[Any, Any, Row](sqlContext.sparkContext,
      Some(regionPath), computeRegionAsRows, Map.empty[String, String],
      rowObjectLength, None, None, rowObjectLength.map(_ =>
        this.inferredValueSchema))(keyTag, valueTag, classTag[Row])
  }


  val inferredValueSchema = providedSchema.map(st =>
    StructType(st.drop(inferredKeySchema.length))).getOrElse(
    {
      if (rowObjectLength.isDefined) {
        throw OtherUtils.analysisException(s" schema needs to be provided for" +
            s" Row objects in GemFire")
      }
      StructType(valueConstraint.map(x => {
        // check if it matches any primitie types
        val primitiveTypeOp = GemFireRelation.inferDataType(valueTag.runtimeClass)
        val (inferedValType, nullableValue) = primitiveTypeOp.map((_, false)).
            getOrElse(JavaTypeInference.inferDataType(valueTag.runtimeClass))
        convertDataTypeToStructFields(inferedValType, nullableValue,
          valueColumnName.getOrElse(Constants.defaultValueColumnName), None)
      }).getOrElse(Array.empty[StructField]))
    }
  )


  override val schema = providedSchema.getOrElse(inferredKeySchema.merge(inferredValueSchema))

  private def conditionOQLAttributeForCaseRow(attribName: String,
      spansOnlyValue: Boolean): (String, StructField) = {
    val index = this.inferredValueSchema.indexWhere(sf =>
      sf.name.equalsIgnoreCase(attribName))
    if (index != -1) {
      if (spansOnlyValue) {
        s"x.get($index)" -> this.inferredValueSchema(index)
      } else {
        s"x.getValue().get($index)" -> this.inferredValueSchema(index)
      }
    } else {
      if (spansOnlyValue) {
        throw OtherUtils.analysisException(s" column name $attribName not found in schema")
      } else {
        // chcek in the keys
        val index = this.inferredKeySchema.indexWhere(sf =>
          sf.name.equalsIgnoreCase(attribName))
        if (index != -1) {
          s"x.getKey()" + (if (this.inferredKeySchema.size == 1) "" else s".$attribName") ->
              this.inferredKeySchema(index)
        } else {
          throw OtherUtils.analysisException(s" column name $attribName not found in schema")
        }
      }
    }
  }

  private def conditionOQLAttributeForCaseDomain(attribName: String,
      spansOnlyValue: Boolean): (String, StructField) = {
    val index = this.inferredValueSchema.indexWhere(sf =>
      sf.name.equalsIgnoreCase(attribName))
    if (this.isDebugEnabled) {
      logDebug(s"GemFireRelation: conditionOQLAttributeForCaseDomain. " +
          s"attribute name = $attribName " +
          "inferred value schema = " + this.inferredValueSchema)
    }
    if (index != -1) {
      (if (spansOnlyValue) {
        "x"
      } else {
        "x.getValue"
      }) + (if (this.inferredValueSchema.size == 1) "" else s".$attribName") ->
          this.inferredValueSchema(index)
    } else {
      if (spansOnlyValue) {
        throw OtherUtils.analysisException(s" column name $attribName not found in schema")
      } else {
        // chcek in the keys
        val index = this.inferredKeySchema.indexWhere(sf =>
          sf.name.equalsIgnoreCase(attribName))
        if (index != -1) {
          s"x.getKey()" + (if (this.inferredKeySchema.size == 1) "" else s".$attribName") ->
              this.inferredKeySchema(index)
        } else {
          throw OtherUtils.analysisException(s" column name $attribName not found in schema")
        }
      }
    }

  }

  private def getProjectionString(requiredColumns: Array[String],
      spansOnlyValue: Boolean): (String, Option[StructType]) = {
    if (requiredColumns.isEmpty) {
      (" 1 ", Some(StructType(Array(StructField("f1", ByteType, false)))))
    } else if (rowObjectLength.isDefined) {
      // the data stored in region is Object[]
      // Map the required columns to the indices of array
      val(projs, structfields) = requiredColumns.map(name => {
        conditionOQLAttributeForCaseRow(name, spansOnlyValue)
      }).unzip
      projs.mkString(",") -> Some(StructType(structfields))
    } else {
      val(projs, structfields) = requiredColumns.map(name => {
        conditionOQLAttributeForCaseDomain(name, spansOnlyValue)
      }).unzip
      projs.mkString(",") -> Some(StructType(structfields))
    }

  }

  private def getFilterString(filters: Array[Filter],
      spansOnlyValue: Boolean): String = {
    val valueConverter = (attributeName: String, value: Any) => {
      val dataType = this.schema.find(x =>
        x.name.equalsIgnoreCase(attributeName)).get.dataType
      GemFireRelation.convertToOQLString(dataType, value)
    }

    val (attribConverter) = if (rowObjectLength.isDefined) {
      (attributeName: String) => {
        conditionOQLAttributeForCaseRow(attributeName, spansOnlyValue)._1
      }
    } else {
      (attributeName: String) => {
        conditionOQLAttributeForCaseDomain(attributeName, spansOnlyValue)._1

      }
    }

    filters.map(GemFireRelation.filterConverter(_)(attribConverter,
      valueConverter)).mkString(" and ")
  }


  private def convertToOQL(requiredColumns: Array[String], filters: Array[Filter],
      spansOnlyValue: Boolean): (String, Option[StructType]) = {
    val (projString, schemaOpt) = getProjectionString(requiredColumns, spansOnlyValue)
    val builder = new StringBuilder("select ").append(projString).append(" from /").
        append(regionPath).
        append(if (spansOnlyValue) " as x " else ".entries as x")

    (if (filters.isEmpty) {
      builder.toString()
    } else {
      builder.append(" where ").append(getFilterString(filters, spansOnlyValue)).toString()
    }, schemaOpt)

  }

  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val spansOnlyValue = this.inferredKeySchema.length == 0 || !requiredColumns.exists(x =>
      this.inferredKeySchema.exists(sf => sf.name.equalsIgnoreCase(x)))
    // Given the cols , identify if it spans only value or keys too
    if (requiredColumns.isEmpty) {
      // it is a count query
      val whereClause = if (filters.isEmpty) {
        None
      } else if (spansOnlyValue) {
        Some(getFilterString(filters, spansOnlyValue))
      } else {
        throw new UnsupportedOperationException("count query with where " +
            "clause spannng keys not supported")
      }
      if (this.isDebugEnabled) {
        logDebug("GemFireRelation: computing for count")
      }
      new GemFireRegionRDD[Any, Any, Row](sqlContext.sparkContext,
        Some(regionPath), computeForCount, Map.empty[String, String], None, whereClause,
        None)(keyTag, valueTag, classTag[Row])
    } else if (requiredColumns.size == this.schema.size && filters.isEmpty &&
        !requiredColumns.zip(schema).exists(tup => !tup._1.equalsIgnoreCase(tup._2.name))) {
      if (this.isDebugEnabled) {
        logDebug("GemFireRelation: computing for empty build scan called from pruneFilter")
      }
      this.buildScan()
    } else {
      val (oql, schemaOpt) = convertToOQL(requiredColumns, filters, spansOnlyValue)
      if (this.isDebugEnabled) {
        this.logDebug(s"GemFireRelation::buildScan:oql executed = $oql")
      }

      new GemFireRegionRDD[Any, Any, Row](sqlContext.sparkContext,
        Some(regionPath), computeOQLAsRows, Map.empty[String, String], rowObjectLength, None,
        Some(oql), schemaOpt)(keyTag, valueTag, classTag[Row])
    }

  }

  override def insertableRelation(sourceSchema: Seq[Attribute]): Option[InsertableRelation] = None

  override def append(rows: RDD[Row], time: Long): Unit = {
    val df = sqlContext.createDataFrame(rows, schema)
    this.insert(df, true)
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val pkIndex = this.schema.indexWhere(sf => sf.name.equalsIgnoreCase(
      this.primaryKeyColumnName.get))
   new GemFireDataFrameFunctions(data).saveToGemFire[Any](regionPath, row => row(pkIndex))
  }

  private def convertDataTypeToStructFields(dataType: DataType, nullable: Boolean,
      singleFieldColumnName: String, providedSchema: Option[StructType]): Array[StructField] = {
    // a provided shema will be of Some cas eonly for key. for value provided schema will be none
    dataType match {
      case x: StructType => {
        // if schema is provided we need to validate if it is correct for key
        val isProvidedSchemaCorrect = providedSchema.map(st =>
          StructType(st.dropRight(providedSchema.size - x.size)).equals(x)
        ).getOrElse(true)
        if (!isProvidedSchemaCorrect) {
          throw OtherUtils.analysisException(s"The provided schema for" +
              s" key is not consistent")
        }
        x.fields
      }
      case _ => providedSchema.map(st => Array(st(0))).getOrElse(
        Array(StructField(singleFieldColumnName, dataType, nullable)))
    }
  }
}

object GemFireRelation {

  val filterConverter: PartialFunction[Filter,
      (String => String, (String, Any) => String) => String] = {
    case EqualTo(attribute, value) => (x: String => String, y: (String, Any) => String) => {
      s"${x(attribute)} = ${y(attribute, value)}"
    }

    case GreaterThan(attribute, value) => (x: String => String,
        y: (String, Any) => String) => {
      s"${x(attribute)} > ${y(attribute, value)}"
    }

    case GreaterThanOrEqual(attribute, value) => (x: String => String,
        y: (String, Any) => String) => {
      s"${x(attribute)} >= ${y(attribute, value)}"
    }

    case LessThan(attribute, value) => (x: String => String, y: (String, Any) => String) => {
      s"${x(attribute)} < ${y(attribute, value)}"
    }

    case LessThanOrEqual(attribute, value) => (x: String => String,
        y: (String, Any) => String) => {
      s"${x(attribute)} <= ${y(attribute, value)}"
    }

    case IsNotNull(attribute) => (x: String => String,
        y: (String, Any) => String) => {
      s" ${x(attribute)} != null "
    }

    case Not(child) => {
      val childConverter = filterConverter(child)
      (_x: String => String, _y: (String, Any) => String) => {
        s"!(${childConverter(_x, _y)})"
      }
    }

    case And(left, right) => {
      val leftFilter = filterConverter(left)
      val rightFilter = filterConverter(right)
      (_x: String => String, _y: (String, Any) => String) => {
        s"${leftFilter(_x, _y)}  and ${rightFilter(_x, _y)}"
      }
    }

    case Or(left, right) => {
      val leftFilter = filterConverter(left)
      val rightFilter = filterConverter(right)
      (_x: String => String, _y: (String, Any) => String) =>
        s"${leftFilter(_x, _y)}  or ${rightFilter(_x, _y)}"

    }

    case EqualNullSafe(attribute, value) => (x: String => String, y: (String, Any) => String) =>
      s" ( (${x(attribute)} = null and  ${y(attribute, value)} == null)  or " +
          s" ${x(attribute)} = ${y(attribute, value)})"

    case In(attribute: String, values: Array[Any]) =>
      (x: String => String, y: (String, Any) => String) =>
        s"${x(attribute)} IN (${
          values.map(y(attribute, _).mkString(","))
        })"

    case StringStartsWith(attribute: String, value: String) => (x: String => String,
        y: (String, Any) => String) => {
      s"${x(attribute)}.toString().startsWith(${y(attribute, value)})"
    }

    case StringEndsWith(attribute: String, value: String) => (x: String => String,
        y: (String, Any) => String) => {
      s"${x(attribute)}.toString().endsWith(${y(attribute, value)})"
    }

    case StringContains(attribute: String, value: String) => (x: String => String,
        y: (String, Any) => String) => {
      s"${x(attribute)}.toString().contains(${y(attribute, value)})"
    }

  }


  def convertToOQLString(dataType: DataType, value: Any): String = dataType match {
    case _: NumericType => value.toString
    case _ => s"'${value.toString}'"
  }


  private def getSchema(beanClass: Class[_]): Seq[AttributeReference] = {
    val (dataType, _) = JavaTypeInference.inferDataType(beanClass)
    dataType.asInstanceOf[StructType].fields.map { f =>
      AttributeReference(f.name, f.dataType, f.nullable)()
    }
  }

  // TODO: If the Key length of schema is 0, then reuse the value array
  def getLengthAndConverters(keyClass: Option[Class[_]],
      valueClass: Option[Class[_]], rowObjectLength: Option[Int]):
  (Int, Int, (Any, Array[Any]) => Unit, (Any, Array[Any]) => Unit) = {

    val (keyLength, keyConverter) = keyClass.map(className => {
      val keyType = inferDataType(className)
      if (keyType.isDefined) {
        (1, (e: Any, array: Array[Any]) => {
          array(0) = if (keyType.get.isInstanceOf[NumericType]) {
            e match {
              case x: java.lang.Byte => x.byteValue
              case x: java.lang.Short => x.shortValue
              case x: java.lang.Integer => x.intValue
              case x: java.lang.Long => x.longValue
              case x: java.lang.Float => x.floatValue
              case x: java.lang.Double => x.doubleValue
              case _ => e
            }
          } else {
            e
          }
        })
      } else {
        getLengthAndExtractorForBeanClass(className, 0)
      }
    }).getOrElse((0, (e: Any, arr: Array[Any]) => {}))


    val (valueLength, valueConverter) = rowObjectLength.map(length => {
      (length, (e: Any, array: Array[Any]) => {
        val temp = e.asInstanceOf[Array[Any]]
        Array.copy(temp, 0, array, keyLength, temp.length)
      })
    }).getOrElse(
      valueClass.map(className => {

        val valType = inferDataType(className)
        if (valType.isDefined) {
          (1, (e: Any, array: Array[Any]) => {
            array(keyLength) = e
          })
        } else {
          getLengthAndExtractorForBeanClass(className, keyLength)
        }
      }
      ).getOrElse((0, (e: Any, arr: Array[Any]) => {}))
    )
    (keyLength + valueLength, keyLength, keyConverter, valueConverter)
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


/*
  private def inferDataType(c: Class[_]): Option[DataType] = {
    c match {
      case c: Class[_] if c == classOf[String] => Some(StringType)
      case c: Class[_] if c == classOf[java.lang.Short] => Some(ShortType)
      case c: Class[_] if c == classOf[scala.Short] => Some(ShortType)
      case c: Class[_] if c == classOf[java.lang.Integer] => Some(IntegerType)
      case c: Class[_] if c == classOf[scala.Int] => Some(IntegerType)
      case c: Class[_] if c == classOf[java.lang.Long] => Some(LongType)
      case c: Class[_] if c == classOf[scala.Long] => Some(LongType)
      case c: Class[_] if c == classOf[java.lang.Double] => Some(DoubleType)
      case c: Class[_] if c == classOf[scala.Double] => Some(DoubleType)
      case c: Class[_] if c == classOf[java.lang.Byte] => Some(ByteType)
      case c: Class[_] if c == classOf[scala.Byte] => Some(ByteType)
      case c: Class[_] if c == classOf[java.lang.Float] => Some(FloatType)
      case c: Class[_] if c == classOf[scala.Float] => Some(FloatType)
      case c: Class[_] if c == classOf[java.lang.Boolean] => Some(BooleanType)
      case c: Class[_] if c == classOf[scala.Boolean] => Some(BooleanType)
      case c: Class[_] if c == classOf[java.math.BigDecimal] => Some(DecimalType.SYSTEM_DEFAULT)
      case c: Class[_] if c == classOf[scala.BigDecimal] => Some(DecimalType.SYSTEM_DEFAULT)
      case c: Class[_] if c == classOf[java.math.BigInteger] => Some(DecimalType.BigIntDecimal)
      case c: Class[_] if c == classOf[scala.BigInt] => Some(DecimalType.BigIntDecimal)
      case c: Class[_] if c == classOf[java.sql.Date] => Some(DateType)
      case c: Class[_] if c == classOf[java.sql.Timestamp] => Some(TimestampType)
      case _ => None
    }

  }
  */

  private def is[T <: Any : Manifest](implicit cls: Class[_]) = cls == manifest[T].runtimeClass



private def inferDataType(c: Class[_]): Option[DataType] = {
  implicit  val arg = c
  val className = c.getName
  if (is[String]) {
    Some(StringType)
  } else if (is[java.lang.Short] || is[Short] || className == "scala.Short") {
    Some(ShortType)
  } else if (is[java.lang.Integer] || is[Int] || className == "scala.Int") {
    Some(IntegerType)
  } else if (is[java.lang.Long] || is[Long] || className == "scala.Long") {
    Some(LongType)
  } else if (is[java.lang.Double] || is[Double] || className == "scala.Double") {
    Some(DoubleType)
  } else if (is[java.lang.Byte] || is[Byte]|| className == "scala.Byte") {
    Some(ByteType)
  } else if (is[java.lang.Float] || is[Float] || className == "scala.Float") {
    Some(FloatType)
  } else if (is[java.lang.Boolean] || is[Boolean]|| className == "scala.Boolean") {
    Some(BooleanType)
  } else if (is[java.math.BigDecimal] || is[BigDecimal]) {
    Some(DecimalType.SYSTEM_DEFAULT)
  } else if (is[java.math.BigInteger] || is[BigInt]) {
    Some(DecimalType.BigIntDecimal)
  } else if (is[java.sql.Date]) {
    Some(DateType)
  } else if (is[java.sql.Timestamp]) {
    Some(TimestampType)
  } else {
    None
  }
  /*
  c match {
    case c: Class[_] if c == classOf[String] => Some(StringType)
    case c: Class[_] if c == classOf[java.lang.Short] => Some(ShortType)
    case c: Class[_] if c == classOf[scala.Short] => Some(ShortType)
    case c: Class[_] if c == classOf[java.lang.Integer] => Some(IntegerType)
    case c: Class[_] if c == classOf[scala.Int] => Some(IntegerType)
    case c: Class[_] if c == classOf[java.lang.Long] => Some(LongType)
    case c: Class[_] if c == classOf[scala.Long] => Some(LongType)
    case c: Class[_] if c == classOf[java.lang.Double] => Some(DoubleType)
    case c: Class[_] if c == classOf[scala.Double] => Some(DoubleType)
    case c: Class[_] if c == classOf[java.lang.Byte] => Some(ByteType)
    case c: Class[_] if c == classOf[scala.Byte] => Some(ByteType)
    case c: Class[_] if c == classOf[java.lang.Float] => Some(FloatType)
    case c: Class[_] if c == classOf[scala.Float] => Some(FloatType)
    case c: Class[_] if c == classOf[java.lang.Boolean] => Some(BooleanType)
    case c: Class[_] if c == classOf[scala.Boolean] => Some(BooleanType)
    case c: Class[_] if c == classOf[java.math.BigDecimal] => Some(DecimalType.SYSTEM_DEFAULT)
    case c: Class[_] if c == classOf[scala.BigDecimal] => Some(DecimalType.SYSTEM_DEFAULT)
    case c: Class[_] if c == classOf[java.math.BigInteger] => Some(DecimalType.BigIntDecimal)
    case c: Class[_] if c == classOf[scala.BigInt] => Some(DecimalType.BigIntDecimal)
    case c: Class[_] if c == classOf[java.sql.Date] => Some(DateType)
    case c: Class[_] if c == classOf[java.sql.Timestamp] => Some(TimestampType)
    case _ => None
  }
  */

}



  def computeForRegionAsRows[K: ClassTag, V: ClassTag](keyConstraint: Option[String],
      valueConstraint: Option[String], rowObjectLength: Option[Int]): ComputeLogic[K, V, Row] = {

    new ComputeLogic[K, V, Row]() {
      override def apply(rdd: GemFireRegionRDD[K, V, Row],
          partition: GemFireRDDPartition, taskContext: TaskContext): Iterator[Row] = {
        val keyClass = keyConstraint.map(MainUtils.classForName(_))
        val valueClass = valueConstraint.map(MainUtils.classForName(_))
        val (totalSize, keyLength, keyConverter, valueConverter) = GemFireRelation.
            getLengthAndConverters(keyClass, valueClass, rowObjectLength)
        val iter = DefaultGemFireConnectionManager.getConnection.
            getRegionData[Any, Any](rdd.regionPath.get, rdd.whereClause, partition,
          keyLength, rdd.schema)
        val hasNestedGemFireRow = rdd.schema.map(_.exists(sf => sf.dataType match {
          case _: StructType => true
          case _ => false
        })).getOrElse(false)
        if (keyLength == 0) {
          iter.asInstanceOf[Iterator[Any]].map(v => {
            val array = Array.ofDim[Any](totalSize)
            valueConverter(v, array)
            new GenericRow(
              if (!hasNestedGemFireRow) {
              array
            } else {
               GemFireRowHelper.convertNestedGemFireRowToRow(rdd.schema.get, array, 0)
            }
            ): Row
          }
          )
        } else {
          iter.asInstanceOf[Iterator[(Any, Any)]].map { case (k, v) => {
            val array = Array.ofDim[Any](totalSize)
            keyConverter(k, array)
            valueConverter(v, array)
            new GenericRow(
              if (!hasNestedGemFireRow) {
                array
              } else {
                GemFireRowHelper.convertNestedGemFireRowToRow(rdd.schema.get, array, keyLength)
              }
            ): Row
          }

          }
        }
      }
    }
  }


  def computeForOQL[T]: ComputeLogic[Any, Any, T] = {
    new ComputeLogic[Any, Any, T]() {
      override def apply(rdd: GemFireRegionRDD[Any, Any, T],
          partition: GemFireRDDPartition, taskContext: TaskContext): Iterator[T] = {
        val buckets = partition.asInstanceOf[GemFireRDDPartition].bucketSet
        val region = rdd.regionPath.getOrElse(rdd.oql.map(
          GemFireRegionRDD.getRegionPathFromQuery(_)).
            getOrElse(throw new IllegalStateException("Unknown region")))
        val iter = DefaultGemFireConnectionManager.getConnection.
            executeQuery(region, buckets, rdd.oql.get, rdd.schema ).
            asInstanceOf[Iterator[Any]]
        val hasNestedGemFireRow = rdd.schema.map(_.exists(sf => sf.dataType match {
          case _: StructType => true
          case _ => false
        })).getOrElse(false)

        if (rdd.regionPath.isDefined ) {
          iter.map {
            elem => elem match {
              case arr: Array[Any] => if (hasNestedGemFireRow) {
                new GenericRow(GemFireRowHelper.
                    convertNestedGemFireRowToRow(rdd.schema.get, arr, 0))
              } else {
                Row(arr: _*)
              }
              case _ => if (hasNestedGemFireRow) {
                new GenericRow(GemFireRowHelper.convertNestedGemFireRowToRow(
                  rdd.schema.get, Array(elem), 0))
              } else {
                Row(elem)
              }
            }
          }.asInstanceOf[Iterator[T]]
        } else {
          iter.asInstanceOf[Iterator[T]]
        }

      }
    }
  }


  def computeForCount: ComputeLogic[Any, Any, Row] = {
    new ComputeLogic[Any, Any, Row]() {
      override def apply(rdd: GemFireRegionRDD[Any, Any, Row],
          partition: GemFireRDDPartition, taskContext: TaskContext): Iterator[Row] = {
        val buckets = partition.bucketSet
        val rgnSize = DefaultGemFireConnectionManager.getConnection.
            getCount(rdd.regionPath.get, buckets, rdd.whereClause)
        new Iterator[Row]() {
          var current = 0
          val fixed = Row(1)

          override def hasNext: Boolean = current < rgnSize

          override def next(): Row = if (hasNext) {
            current += 1
            fixed
          } else throw new NoSuchElementException
        }
      }
    }
  }


}

final class DefaultSource
    extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider
        with DataSourceRegister with Logging {

  def shortName(): String = "GemFire"

  def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], schemaOpt: Option[StructType],
      asSelect: Boolean): GemFireRelation = {

    val params = new CaseInsensitiveMutableHashMap(options)

    val snc = sqlContext.asInstanceOf[SnappyContext]
    val tableName = options.get(JdbcExtendedUtils.DBTABLE_PROPERTY)
    val regionPath = params.getOrElse(Constants.REGION_PATH, throw OtherUtils.analysisException(
      "GemFire Region Path is missing"))
    val pkColumnName = params.get(Constants.PRIMARY_KEY_COLUMN_NAME)
    val valueColumnName = params.get(Constants.VALUE_COLUMN_NAME)
    val kc = params.get(Constants.keyConstraintClass)
    val vc = params.get(Constants.valueConstraintClass)
    if (kc.isEmpty && vc.isEmpty) {
      throw OtherUtils.analysisException("Either Key Class  or value class  " +
          "need to be provided for the table definition")
    }
    val relation = GemFireRelation(snc, regionPath, pkColumnName, valueColumnName,
      kc, vc, schemaOpt, asSelect)

    val catalog = sqlContext.sparkSession.asInstanceOf[SnappySession].sessionCatalog
    if (this.isDebugEnabled) {
      this.logDebug("GemFireRelation :options passed in table creation =" + options)
    }
   if (tableName.isDefined) {
     if (this.isDebugEnabled) {
       this.logDebug("GemFireRelation :registering relation with name = " + tableName.get)
     }
     catalog.registerDataSourceTable(
       catalog.newQualifiedTableName(tableName.get), Some(relation.schema),
       Array.empty[String], classOf[connector.gemfire.DefaultSource].getCanonicalName,
       options, relation)

   }
   relation

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


  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], data: DataFrame): BaseRelation = {
    if (!options.contains(Constants.PRIMARY_KEY_COLUMN_NAME)) {
      throw OtherUtils.analysisException("To insert into GemFire Region, primary key " +
          "column needs to be specified")
    }
    val modOptions = options + (Constants.valueConstraintClass -> classOf[Row].getName)
    val relation = createRelation(sqlContext, mode, modOptions, Some(data.schema),
       asSelect = true)
     var success = false
     try {
       relation.insert(data, mode == SaveMode.Overwrite)
       success = true
       relation
     } finally {
       /*
       if (!success && !relation.tableExists) {

         relation.destroy(ifExists = true)
       }
       */
     }

  }

}
