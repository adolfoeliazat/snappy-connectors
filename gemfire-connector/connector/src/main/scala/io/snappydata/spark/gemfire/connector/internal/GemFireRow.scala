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

import java.io.{DataInput, DataOutput, IOException}
import java.sql.{Date, Timestamp}

import scala.collection.mutable.BitSet

import com.gemstone.gemfire.{DataSerializable, DataSerializer}
import io.snappydata.spark.gemfire.connector.internal.gemfirefunctions.shared.{NonVersionedHeapDataOutputStream,
SchemaMappings}

/*
Because OQL Row serializer is sensitive to readArray WriteArray methods, the data start positions long array
is not made part of serialized data, instead at this point, it is kept as separate array

With field start positions, bit mask is redundant partially , but there are situations where we just send
the array data without top schema & field start positions, in which case bitmask helps.
but we need to avoid redundant data sending

new data format :
total length (int) ( schema length + schema bytes +  array data start + start field pos longs )
 */
@SerialVersionUID(1362354784026L)
class GemFireRow(var schemaCode: Array[Byte],
    var deser: Array[Any]) extends DataSerializable {


  def this() {
    this(null, null)
  }


  @throws[IOException]
  def toData(dataOutput: DataOutput) {
    val hdos: NonVersionedHeapDataOutputStream = new NonVersionedHeapDataOutputStream
    this.writeSchema(hdos)
    // val dataStartPositions = this.reserveDataPositions(hdos)

    this.writeDataWithFieldStartPosAppended(hdos)

    DataSerializer.writePrimitiveInt(hdos.size(), dataOutput)
    hdos.sendTo(dataOutput)

  }

  /*
  private def reserveDataPositions(hdos: NonVersionedHeapDataOutputStream):
  Seq[NonVersionedHeapDataOutputStream.LongUpdater] = {
    val num = Utils.getNumberOfPositionsForFieldAddress(schemaCode.length)
    for (i <- 0 until num) yield hdos.reserveLong()
  }
  */



  @throws[IOException]
  private def writeSchema(hdos: DataOutput) {
    // DataSerializer.writeByteArray(schemaCode, hdos)
    hdos.writeInt(schemaCode.length);
    hdos.write(schemaCode);

  }

  @throws[IOException]
  private def writeDataWithFieldStartPosAppended(hdos: NonVersionedHeapDataOutputStream) {
    val numLongs: Int = GemFireRow.getNumLongsForBitSet(schemaCode.length)
    val longUpdaters = Array.fill[NonVersionedHeapDataOutputStream.LongUpdater](
      numLongs)(hdos.reserveLong)
    var fieldStartPosition = 0
    val bitset = new BitSet(schemaCode.length)
    val fieldStartPositions = for (i <- 0 until deser.length) yield {
      val elem: Any = deser(i)
      if (elem != null) {
        bitset += i
       val fieldStartPosition = hdos.size()
        // dataStartPositions(i).update(hdos.size())
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
        fieldStartPosition
      } else {
        0
      }

    }
    bitset.toBitMask.zip(longUpdaters).foreach(tuple => tuple._2.update(tuple._1))
    fieldStartPositions.foreach(pos => if (pos != 0) {
      hdos.writeInt(pos)
    })
  }

  @throws[IOException]
  def fromData(dataInput: DataInput) {
    /*
    val dataLength: Int = DataSerializer.readPrimitiveInt(dataInput)
    schemaCode = DataSerializer.readByteArray(dataInput)
    ser = new Array[Byte](dataLength)
    dataInput.readFully(ser)
    */
    // read & ignore the total data length
    DataSerializer.readPrimitiveInt(dataInput)
    // read schema code
    val schemaCodeLength = dataInput.readInt();
    schemaCode = Array.ofDim[Byte](schemaCodeLength)
    dataInput.readFully(schemaCode)

    this.deser = this.readArrayData(dataInput)

    // consume the data start positions .
    // we do not need the array positions I suppose in connector VM
    // if we think we need it , we will store it
    // ignore the field start positions at the end of data , they for now
    // are not needed to be read

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

    // consume all not null positions
    0 until schemaCode.length foreach {
      i => if (bitset(i)) {
        dis.readInt
      }
    }

    deserialzed
  }
}

object GemFireRow {
  private val ADDRESS_BITS_PER_WORD: Int = 6

  def getNumLongsForBitSet(nbits: Int): Int = wordIndex(nbits - 1) + 1

  def wordIndex(bitIndex: Int): Int = bitIndex >> ADDRESS_BITS_PER_WORD



}
