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
package io.snappydata.spark.gemfire.connector.internal

import java.io.{ByteArrayInputStream, DataInput, DataInputStream, DataOutput, IOException}
import java.sql.{Date, Timestamp}

import scala.collection.mutable.BitSet

import com.gemstone.gemfire.{DataSerializable, DataSerializer}
import io.snappydata.spark.gemfire.connector.internal.gemfirefunctions.shared.{NonVersionedHeapDataOutputStream, SchemaMappings}

@SerialVersionUID(1362354784026L)
class GemFireRow(var schemaCode: Array[Byte],
    var deser: Array[Any]) extends DataSerializable{


  def this(){
    this(null, null)
  }


  @throws[IOException]
  def toData(dataOutput: DataOutput) {
   /*
    if (ser != null) {
      DataSerializer.writePrimitiveInt(ser.length, dataOutput)
      this.writeSchema(dataOutput)
      dataOutput.write(ser)
    }
    else {
      val hdos: NonVersionedHeapDataOutputStream = new NonVersionedHeapDataOutputStream
      this.writeSchema(hdos)
      val initialSize: Int = hdos.size
      this.writeData(hdos)
      val endSize: Int = hdos.size
      val serDataSize: Int = endSize - initialSize
      DataSerializer.writePrimitiveInt(serDataSize, dataOutput)
      hdos.sendTo(dataOutput)
    }
    */
   val hdos: NonVersionedHeapDataOutputStream = new NonVersionedHeapDataOutputStream
    this.writeSchema(hdos)
    this.writeData(hdos)
    hdos.sendTo(dataOutput)
  }

  @throws[IOException]
  def toDataWithoutTopSchema(hdos: NonVersionedHeapDataOutputStream) {
    /*
    if (ser != null) {
      hdos.write(ser)
    }
    else {
      this.writeData(hdos)
      this.ser = hdos.toByteArray
    }
    */
    this.writeData(hdos)
  }

  @throws[IOException]
  private def writeSchema(hdos: DataOutput) {
    DataSerializer.writeByteArray(schemaCode, hdos)
  }

  @throws[IOException]
  private def writeData(hdos: NonVersionedHeapDataOutputStream) {
    val numLongs: Int = GemFireRow.getNumLongsForBitSet(schemaCode.length)
    val longUpdaters = Array.fill[NonVersionedHeapDataOutputStream.LongUpdater](
      numLongs)(hdos.reserveLong)

    val bitset = new BitSet(schemaCode.length)
   // var i: Int = 0
    0 until deser.length foreach(i => {
      val elem: Any = deser(i)
      if (elem != null) {
        bitset += i
        schemaCode(i) match {
          case SchemaMappings.stringg =>
            DataSerializer.writeString(elem.asInstanceOf[String], hdos)
          case SchemaMappings.shortt =>
            DataSerializer.writePrimitiveShort(elem.asInstanceOf[Short], hdos)
          case SchemaMappings.intt =>
            DataSerializer.writePrimitiveInt(elem.asInstanceOf[Int], hdos)
          case SchemaMappings.longg =>
            DataSerializer.writePrimitiveLong(elem.asInstanceOf[Long], hdos)
          case SchemaMappings.doublee =>
            DataSerializer.writePrimitiveDouble(elem.asInstanceOf[Double], hdos)
          case SchemaMappings.bytee =>
            DataSerializer.writePrimitiveByte(elem.asInstanceOf[Byte], hdos)
          case SchemaMappings.floatt =>
            DataSerializer.writePrimitiveFloat(elem.asInstanceOf[Float], hdos)
          case SchemaMappings.binary =>
            DataSerializer.writeByteArray(elem.asInstanceOf[Array[Byte]], hdos)
          case SchemaMappings.booll =>
            DataSerializer.writePrimitiveBoolean(elem.asInstanceOf[Boolean], hdos)
          case SchemaMappings.datee =>
            DataSerializer.writePrimitiveLong((elem.asInstanceOf[Date]).getTime, hdos)
          case SchemaMappings.timestampp =>
            DataSerializer.writePrimitiveLong((elem.asInstanceOf[Timestamp]).getTime, hdos)
            DataSerializer.writePrimitiveInt((elem.asInstanceOf[Timestamp]).getNanos, hdos)
          case SchemaMappings.structtypee => {
            val row: GemFireRow = elem.asInstanceOf[GemFireRow]
            row.toData(hdos)
          }
          case SchemaMappings.unoptimizedtype => {
            DataSerializer.writeObject(elem, hdos)
          }
        }
      }
    }
    )
    bitset.toBitMask.zip(longUpdaters).foreach(tuple => tuple._2.update(tuple._1))

  }

  @throws[IOException]
  def fromData(dataInput: DataInput) {
    /*
    val dataLength: Int = DataSerializer.readPrimitiveInt(dataInput)
    schemaCode = DataSerializer.readByteArray(dataInput)
    ser = new Array[Byte](dataLength)
    dataInput.readFully(ser)
    */
    schemaCode = DataSerializer.readByteArray(dataInput)
    this.deser = this.readArrayData(dataInput)
  }

  @throws[IOException]
  @throws[ClassNotFoundException]
  def getArray: Array[Any] = {
    /*
    if (deser == null) {
      val dis: DataInputStream = new DataInputStream(new ByteArrayInputStream(this.ser))
      this.deser = this.readArrayData(dis)
    }
    */
    return deser
  }

  @throws[IOException]
  @throws[ClassNotFoundException]
  def get(pos: Int): Any = {
    return this.deser(pos)
  }

  @throws[IOException]
  @throws[ClassNotFoundException]
  def readArrayData(dis: DataInput): Array[Any] = {
    val numLongs: Int = GemFireRow.getNumLongsForBitSet(schemaCode.length)
    val masks = Array.fill[Long](numLongs)(dis.readLong)

    val bitset = BitSet.fromBitMaskNoCopy(masks)
    val deserialzed: Array[Any] = Array.tabulate[Any](schemaCode.length)(i => {
      if (bitset(i)) {
        schemaCode(i) match {
          case SchemaMappings.stringg =>
            DataSerializer.readString(dis)
          case SchemaMappings.shortt =>
            DataSerializer.readPrimitiveShort(dis)
          case SchemaMappings.intt =>
            DataSerializer.readPrimitiveInt(dis)
          case SchemaMappings.longg =>
           DataSerializer.readPrimitiveLong(dis)
          case SchemaMappings.doublee =>
            DataSerializer.readPrimitiveDouble(dis)
          case SchemaMappings.bytee =>
            DataSerializer.readPrimitiveByte(dis)
          case SchemaMappings.floatt =>
            DataSerializer.readPrimitiveFloat(dis)
          case SchemaMappings.binary =>
            DataSerializer.readByteArray(dis)
          case SchemaMappings.booll =>
            DataSerializer.readPrimitiveBoolean(dis)
          case SchemaMappings.datee => {
            val time: Long = DataSerializer.readPrimitiveLong(dis)
            new Date(time)
          }
          case SchemaMappings.timestampp => {
            val time: Long = DataSerializer.readPrimitiveLong(dis)
            val nano: Int = DataSerializer.readPrimitiveInt(dis)
            val ts: Timestamp = new Timestamp(time)
            ts.setNanos(nano)
            ts
          }
          case SchemaMappings.structtypee => {
            val gfRow: GemFireRow = new GemFireRow
            gfRow.fromData(dis)
            gfRow

          }
          case SchemaMappings.unoptimizedtype =>
            DataSerializer.readObject(dis)
        }
      }
    }
    )

    return deserialzed
  }
}

object GemFireRow {
  private val ADDRESS_BITS_PER_WORD: Int = 6

  def getNumLongsForBitSet(nbits: Int): Int = wordIndex(nbits - 1) + 1

  def wordIndex(bitIndex: Int): Int = bitIndex >> ADDRESS_BITS_PER_WORD
}
