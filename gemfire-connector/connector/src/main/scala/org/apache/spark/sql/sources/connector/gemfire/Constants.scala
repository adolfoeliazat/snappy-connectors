/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 */
package org.apache.spark.sql.sources.connector.gemfire

object Constants {
  val REGION_PATH: String = "regionPath"
  val PRIMARY_KEY_COLUMN_NAME: String = "primaryKeyColumnName"
  val VALUE_COLUMN_NAME: String = "valueColumnName"
  val keyConstraintClass = "keyClass"
  val valueConstraintClass = "valueClass"
  val defaultKeyColumnName = "KeyColumn"
  val defaultValueColumnName = "ValueColumn"
}
