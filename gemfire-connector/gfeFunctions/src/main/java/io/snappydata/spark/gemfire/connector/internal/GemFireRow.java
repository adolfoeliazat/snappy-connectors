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
package io.snappydata.spark.gemfire.connector.internal;


import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.BitSet;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import io.snappydata.spark.gemfire.connector.internal.gemfirefunctions.shared.NonVersionedHeapDataOutputStream;
import io.snappydata.spark.gemfire.connector.internal.gemfirefunctions.shared.SchemaMappings;

public class GemFireRow implements DataSerializable {

  private volatile byte[] serializedBytes = null;

  private static int ADDRESS_BITS_PER_WORD = 6;
  public static long serialVersionUID = 1362354784026L;

  public GemFireRow() {
  }

  public static GemFireRow create(byte[] schemaCode, Object[] deser) throws IOException {
    NonVersionedHeapDataOutputStream hdos = new NonVersionedHeapDataOutputStream();

    writeSchema(hdos, schemaCode);

    writeDataWithFieldStartPositionsAppended(hdos, schemaCode, deser);
    return new GemFireRow(hdos.toByteArray());
  }

  private GemFireRow(byte[] serBytes) {
    this.serializedBytes = serBytes;
  }

  @Override
  public void toData(DataOutput dataOutput) throws IOException {
    DataSerializer.writePrimitiveInt(serializedBytes.length, dataOutput);
    dataOutput.write(serializedBytes);
    /*
      NonVersionedHeapDataOutputStream hdos = new NonVersionedHeapDataOutputStream();
      this.writeSchema(hdos);
      int initialSize = hdos.size();
      this.writeData(hdos);
      int endSize = hdos.size();
      int serDataSize = endSize - initialSize ;
      DataSerializer.writePrimitiveInt(serDataSize, dataOutput);
      hdos.sendTo(dataOutput);
*/

  }

  private int startPositionForArrayData() {
    ByteBuffer buffer = ByteBuffer.wrap(this.serializedBytes);
    int schemaLength = buffer.getInt();
    int startPosOfArrayData = 4 ; //schema length
    startPosOfArrayData += schemaLength;
    return startPosOfArrayData;
  }

  private int lengthOfArrayData(int startPositionOfArrayData) {
    int schemaLength = startPositionOfArrayData - 4 ; // subtract the 4 bytes used to write schema length to get
    // schema length
    ByteBuffer buffer = ByteBuffer.wrap(this.serializedBytes);
    buffer.position(startPositionOfArrayData);
    // find the total not null cols
    int numLongs = getNumLongsForBitSet(schemaLength);
    long[] masks = new long[numLongs];
    for (int i = 0; i < numLongs; ++i) {
      masks[i] = buffer.getLong();
    }
    BitSet bitset = BitSet.valueOf(masks);
    int numStartPositionsAppended = bitset.cardinality();
    int arrayDataLength = this.serializedBytes.length - startPositionOfArrayData - 4 * numStartPositionsAppended;
    return arrayDataLength;

  }



  public void toDataWithoutTopSchemaAndFieldPositions(NonVersionedHeapDataOutputStream hdos) throws IOException {
    int startPosForArrayData = startPositionForArrayData();
    int arrayDataLen = lengthOfArrayData(startPosForArrayData);
    hdos.write(this.serializedBytes, startPosForArrayData, arrayDataLen);

  }

  private static void writeSchema(DataOutput hdos, byte[] schema) throws IOException {
    //DataSerializer.writeByteArray(schemaCode, hdos);
    hdos.writeInt(schema.length);
    hdos.write(schema);
  }



  private static void writeDataWithFieldStartPositionsAppended(NonVersionedHeapDataOutputStream hdos,
      byte[] schemaCode,  Object[] deser)
      throws IOException {
    int numLongs = getNumLongsForBitSet(schemaCode.length);
    NonVersionedHeapDataOutputStream.LongUpdater[] longUpdaters =
        new NonVersionedHeapDataOutputStream.LongUpdater[numLongs];
    for (int i = 0; i < numLongs; ++i) {
      longUpdaters[i] = hdos.reserveLong();
    }
    int[] fieldStartPositions = new int[schemaCode.length];
    BitSet bitset = new BitSet(schemaCode.length);
    for (int i = 0; i < deser.length; ++i) {
      Object elem = deser[i];
      if (elem != null) {
        fieldStartPositions[i] = hdos.size();
        bitset.set(i);
        switch (schemaCode[i]) {
          case SchemaMappings.stringg:
            DataSerializer.writeString((String)elem, hdos);
            break;
          case SchemaMappings.shortt:
            DataSerializer.writePrimitiveShort(
                (Short)elem, hdos);
            break;
          case SchemaMappings.intt:
            DataSerializer.writePrimitiveInt((Integer)elem, hdos);
            break;
          case SchemaMappings.longg:
            DataSerializer.writePrimitiveLong((Long)elem, hdos);
            break;
          case SchemaMappings.doublee:
            DataSerializer.writePrimitiveDouble(
                (Double)elem, hdos);
            break;
          case SchemaMappings.bytee:
            DataSerializer.writePrimitiveByte((Byte)elem, hdos);
            break;
          case SchemaMappings.floatt:
            DataSerializer.writePrimitiveFloat(
                (Float)elem, hdos);
            break;
          case SchemaMappings.binary:
            DataSerializer.writeByteArray(
                (byte[])elem, hdos);
            break;
          case SchemaMappings.booll:
            DataSerializer.writePrimitiveBoolean(
                (Boolean)elem, hdos);
            break;

          case SchemaMappings.datee:
            DataSerializer.writePrimitiveLong(
                ((Date)elem).getTime(), hdos);
            break;
          case SchemaMappings.timestampp:
            DataSerializer.writePrimitiveLong(
                ((Timestamp)elem).getTime(), hdos);
            DataSerializer.writePrimitiveInt(((Timestamp)elem).getNanos(), hdos);
            break;

          case SchemaMappings.structtypee: {
            GemFireRow row = (GemFireRow)elem;
            row.toData(hdos);
            break;
          }
          case SchemaMappings.unoptimizedtype: {
            DataSerializer.writeObject(elem, hdos);
            break;
          }

        }
      } else {
        fieldStartPositions[i] = 0;
      }
    }
    long[] masks = bitset.toLongArray();
    for (int i = 0; i < numLongs; ++i) {
      longUpdaters[i].update(masks[i]);
    }
    for (int pos : fieldStartPositions ) {
      if (pos != 0) {
        hdos.writeInt(pos);
      }
    }
  }


  @Override
  public void fromData(DataInput dataInput) throws IOException, ClassNotFoundException {
    int dataLength = DataSerializer.readPrimitiveInt(dataInput);
    /*
    schemaCode = DataSerializer.readByteArray(dataInput);

    this.fieldStartPositions = new int[schemaCode.length];
    for(int i = 0 ; i < schemaCode.length; ++i) {
      this.fieldStartPositions[i] = (int)dataInput.readLong();
    }
    */
    this.serializedBytes = new byte[dataLength];
    dataInput.readFully(this.serializedBytes);

  }

  public Object[] getArray() throws IOException, ClassNotFoundException {
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(this.serializedBytes));
    int schemaLen = dis.readInt();
    byte[] schemaCode = new byte[schemaLen];
    dis.readFully(schemaCode);
    Object[] deser = this.readArrayData(dis, schemaCode);
    return deser;
  }


  public Object get(int pos) throws IOException, ClassNotFoundException {

    byte dataType = this.serializedBytes[4 + pos];
    DataInputStream dis  = new DataInputStream(new ByteArrayInputStream(this.serializedBytes));
    dis.mark(this.serializedBytes.length);
    int schemaLength = dis.readInt();
    dis.skipBytes(schemaLength);

    int numLongs = getNumLongsForBitSet(schemaLength);
    long[] masks = new long[numLongs];
    for (int i = 0; i < numLongs; ++i) {
      masks[i] = dis.readLong();
    }
    BitSet bitset = BitSet.valueOf(masks);

    if (!bitset.get(pos)) {
      return null;
    } else {
      int totalNotNullColsFromPos = 0;
      for (int i = pos ; i < schemaLength; ++i) {
        if (bitset.get(i)) {
          ++totalNotNullColsFromPos;
        }
      }
      int addressLocation = this.serializedBytes.length - totalNotNullColsFromPos * 4;
      dis.reset();
      dis.skipBytes(addressLocation);
      int address = dis.readInt();
      dis.reset();
      dis.skipBytes(address);
      return readForDataType(dataType, dis);
    }



  }

  private Object[] readArrayData(DataInput dis, byte[] schemaCode) throws IOException, ClassNotFoundException {
    int numLongs = getNumLongsForBitSet(schemaCode.length);
    long[] masks = new long[numLongs];
    for (int i = 0; i < numLongs; ++i) {
      masks[i] = dis.readLong();
    }

    BitSet bitset = BitSet.valueOf(masks);
    Object[] deserialzed = new Object[schemaCode.length];
    for (int i = 0; i < schemaCode.length; ++i) {
      if (bitset.get(i)) {
        deserialzed[i] = readForDataType(schemaCode[i], dis);
      }
    }

    return deserialzed;
  }

  private Object readForDataType(byte dataType, DataInput dis) throws  IOException, ClassNotFoundException {

    switch (dataType) {
      case SchemaMappings.stringg:
        return DataSerializer.readString(dis);
      case SchemaMappings.shortt:
        return Short.valueOf(DataSerializer.readPrimitiveShort(dis));
      case SchemaMappings.intt:
        return Integer.valueOf(DataSerializer.readPrimitiveInt(dis));
      case SchemaMappings.longg:
        return Long.valueOf(DataSerializer.readPrimitiveLong(dis));

      case SchemaMappings.doublee:
        return Double.valueOf(DataSerializer.readPrimitiveDouble(dis));

      case SchemaMappings.bytee:
        return Byte.valueOf(DataSerializer.readPrimitiveByte(dis));

      case SchemaMappings.floatt:
        return Float.valueOf(DataSerializer.readPrimitiveFloat(dis));

      case SchemaMappings.binary:
        return DataSerializer.readByteArray(dis);

      case SchemaMappings.booll:
        return Boolean.valueOf(DataSerializer.readPrimitiveBoolean(dis));

      case SchemaMappings.datee: {
        long time = DataSerializer.readPrimitiveLong(dis);
        return new java.sql.Date(time);

      }
      case SchemaMappings.timestampp: {
        long time = DataSerializer.readPrimitiveLong(dis);
        int nano = DataSerializer.readPrimitiveInt(dis);
        Timestamp ts = new Timestamp(time);
        ts.setNanos(nano);
        return ts;

      }
      case SchemaMappings.structtypee: {
        GemFireRow gfRow = new GemFireRow();
        gfRow.fromData(dis);
        return gfRow;

      }
      case SchemaMappings.unoptimizedtype:
        return DataSerializer.readObject(dis);

    }
    throw new IllegalStateException("unknown data type = " + dataType);
  }

  public static int getNumLongsForBitSet(int nbits) {
    return wordIndex(nbits - 1) + 1;
  }

  private static int wordIndex(int bitIndex) {
    return bitIndex >> ADDRESS_BITS_PER_WORD;
  }
}