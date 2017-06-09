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
import java.lang.ref.WeakReference;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.BitSet;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import io.snappydata.spark.gemfire.connector.internal.gemfirefunctions.shared.NonVersionedHeapDataOutputStream;
import io.snappydata.spark.gemfire.connector.internal.gemfirefunctions.shared.SchemaMappings;

public class GemFireRow implements DataSerializable {

  private volatile byte[] schemaCode = null;
  private volatile WeakReference<Object[]> weakDeser = null;
  private volatile byte[] serData = null;
  private static int ADDRESS_BITS_PER_WORD = 6;
  public static long serialVersionUID = 1362354784026L;

  public GemFireRow() {
  }

  public static GemFireRow create(byte[] schemaCode, Object[] deser) throws IOException {
    NonVersionedHeapDataOutputStream hdos = new NonVersionedHeapDataOutputStream();
    writeData(hdos, schemaCode, deser);
    return new GemFireRow(schemaCode, hdos.toByteArray());
  }

  private GemFireRow(byte[] schemaCode, byte[] ser) {
    this.schemaCode = schemaCode;
    this.serData = ser;
  }

  @Override
  public void toData(DataOutput dataOutput) throws IOException {
    DataSerializer.writePrimitiveInt(serData.length, dataOutput);
    this.writeSchema(dataOutput);
    dataOutput.write(serData);
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
    /*
    NonVersionedHeapDataOutputStream hdos = new NonVersionedHeapDataOutputStream();
    this.writeSchema(hdos);
    this.writeData(hdos);
    hdos.sendTo(dataOutput);
    */
  }


  public void toDataWithoutTopSchema(NonVersionedHeapDataOutputStream hdos) throws IOException {
    hdos.write(this.serData);
    /*
    if (ser != null) {
      hdos.write(ser);
    } else {
    */
    //this.writeData(hdos);
    // this.ser = hdos.toByteArray();
    //}
  }

  private void writeSchema(DataOutput hdos) throws IOException {
    DataSerializer.writeByteArray(schemaCode, hdos);
  }

  private static void writeData(NonVersionedHeapDataOutputStream hdos, byte[] schemaCode, Object[] deser)
      throws IOException {
    int numLongs = getNumLongsForBitSet(schemaCode.length);
    NonVersionedHeapDataOutputStream.LongUpdater[] longUpdaters =
        new NonVersionedHeapDataOutputStream.LongUpdater[numLongs];
    for (int i = 0; i < numLongs; ++i) {
      longUpdaters[i] = hdos.reserveLong();
    }

    BitSet bitset = new BitSet(schemaCode.length);
    for (int i = 0; i < deser.length; ++i) {
      Object elem = deser[i];
      if (elem != null) {
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
      }

    }
    long[] masks = bitset.toLongArray();
    for (int i = 0; i < numLongs; ++i) {
      longUpdaters[i].update(masks[i]);
    }
  }


  @Override
  public void fromData(DataInput dataInput) throws IOException, ClassNotFoundException {
    int dataLength = DataSerializer.readPrimitiveInt(dataInput);
    schemaCode = DataSerializer.readByteArray(dataInput);
    this.serData = new byte[dataLength];
    dataInput.readFully(serData);
    //this.deser = this.readArrayData(dataInput);
  }

  public Object[] getArray() throws IOException, ClassNotFoundException {
    if (weakDeser != null) {
      Object[] deser = weakDeser.get();
      if (deser != null) {
        return deser;
      } else {
        return getAndSetDeser();
      }
    } else {
      return getAndSetDeser();
    }

  }

  private Object[] getAndSetDeser() throws IOException, ClassNotFoundException {
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(this.serData));
    Object[] deser = this.readArrayData(dis);
    this.weakDeser = new WeakReference<Object[]>(deser);
    return deser;
  }

  public Object get(int pos) throws IOException, ClassNotFoundException {
    return getArray()[pos];

  }

  public Object[] readArrayData(DataInput dis) throws IOException, ClassNotFoundException {
    int numLongs = getNumLongsForBitSet(schemaCode.length);
    long[] masks = new long[numLongs];
    for (int i = 0; i < numLongs; ++i) {
      masks[i] = dis.readLong();
    }

    BitSet bitset = BitSet.valueOf(masks);
    Object[] deserialzed = new Object[schemaCode.length];
    for (int i = 0; i < schemaCode.length; ++i) {
      if (bitset.get(i)) {
        switch (schemaCode[i]) {
          case SchemaMappings.stringg:
            deserialzed[i] = DataSerializer.readString(dis);
            break;
          case SchemaMappings.shortt:
            deserialzed[i] = Short.valueOf(DataSerializer.readPrimitiveShort(dis));
            break;
          case SchemaMappings.intt:
            deserialzed[i] = Integer.valueOf(DataSerializer.readPrimitiveInt(dis));
            break;
          case SchemaMappings.longg:
            deserialzed[i] = Long.valueOf(DataSerializer.readPrimitiveLong(dis));
            break;
          case SchemaMappings.doublee:
            deserialzed[i] = Double.valueOf(DataSerializer.readPrimitiveDouble(dis));
            break;
          case SchemaMappings.bytee:
            deserialzed[i] = Byte.valueOf(DataSerializer.readPrimitiveByte(dis));
            break;
          case SchemaMappings.floatt:
            deserialzed[i] = Float.valueOf(DataSerializer.readPrimitiveFloat(dis));
            break;
          case SchemaMappings.binary:
            deserialzed[i] = DataSerializer.readByteArray(dis);
            break;
          case SchemaMappings.booll:
            deserialzed[i] = Boolean.valueOf(DataSerializer.readPrimitiveBoolean(dis));
            break;
          case SchemaMappings.datee: {
            long time = DataSerializer.readPrimitiveLong(dis);
            deserialzed[i] = new java.sql.Date(time);
            break;
          }
          case SchemaMappings.timestampp: {
            long time = DataSerializer.readPrimitiveLong(dis);
            int nano = DataSerializer.readPrimitiveInt(dis);
            Timestamp ts = new Timestamp(time);
            ts.setNanos(nano);
            deserialzed[i] = ts;
            break;
          }
          case SchemaMappings.structtypee: {
            GemFireRow gfRow = new GemFireRow();
            gfRow.fromData(dis);
            deserialzed[i] = gfRow;
            break;
          }
          case SchemaMappings.unoptimizedtype:
            deserialzed[i] = DataSerializer.readObject(dis);
            break;
        }
      }
    }

    return deserialzed;
  }

  public static int getNumLongsForBitSet(int nbits) {
    return wordIndex(nbits - 1) + 1;
  }

  private static int wordIndex(int bitIndex) {
    return bitIndex >> ADDRESS_BITS_PER_WORD;
  }
}