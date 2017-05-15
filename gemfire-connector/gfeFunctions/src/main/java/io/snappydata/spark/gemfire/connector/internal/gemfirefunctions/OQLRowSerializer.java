package io.snappydata.spark.gemfire.connector.internal.gemfirefunctions;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.BitSet;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import io.snappydata.spark.gemfire.connector.internal.gemfirefunctions.shared.SchemaMappings;


/**
 * Created by ashahid on 5/12/17.
 */
public class OQLRowSerializer {
  public static void serializeRaw(Object[] values, byte[] schemaCode, HeapDataOutputStream hdos)  throws IOException{
    int numLongs = schemaCode.length / 8 + schemaCode.length % 8 > 0 ? 1 : 0;
    HeapDataOutputStream.LongUpdater [] updaters = new HeapDataOutputStream.LongUpdater[numLongs];
    for(int i = 0; i < numLongs; ++i) {
      updaters[i] = hdos.reserveLong();
    }
    BitSet bs = new BitSet(schemaCode.length);
    for (int i = 0 ; i < schemaCode.length; ++i) {
      Object elem = values[i];
      if (elem != null) {
        bs.set(i);
        byte code = schemaCode[i];
        writeNotNull(elem, hdos, code);
      }
    }
    long[] maskArr = bs.toLongArray();
    for(int i = 0; i < updaters.length; ++i) {
      updaters[i].update(maskArr[i]);
    }
  }

  private static void writeNotNull(Object elem, HeapDataOutputStream hdos, byte code) throws IOException{
    if(code == SchemaMappings.bigintt) {
      DataSerializer.writeObject(elem, hdos);
    } else if (code == SchemaMappings.booll) {
      DataSerializer.writePrimitiveBoolean(((Boolean)elem).booleanValue(), hdos);
    } else if (code == SchemaMappings.bytee) {
      DataSerializer.writePrimitiveByte(((Byte)elem).byteValue(), hdos);
    } else if (code == SchemaMappings.datee) {
      Date val = (Date)elem;
      DataSerializer.writePrimitiveLong(val.getTime(), hdos);
    } else if (code == SchemaMappings.decimall) {
      DataSerializer.writeObject(elem, hdos);
    } else if (code == SchemaMappings.doublee) {
      DataSerializer.writePrimitiveDouble(((Double)elem).doubleValue(), hdos);
    } else if (code == SchemaMappings.floatt) {
      DataSerializer.writePrimitiveFloat(((Float)elem).floatValue(), hdos);
    } else if (code == SchemaMappings.intt) {
      DataSerializer.writePrimitiveInt(((Integer)elem).intValue(), hdos);
    } else if (code == SchemaMappings.longg) {
      DataSerializer.writePrimitiveLong(((Long)elem).longValue(), hdos);
    } else if (code == SchemaMappings.shortt) {
      DataSerializer.writePrimitiveShort(((Short)elem).shortValue(), hdos);
    } else if (code == SchemaMappings.stringg) {
      DataSerializer.writeString((String)elem, hdos);
    } else if (code == SchemaMappings.structtypee) {
      DataSerializer.writeObject(elem, hdos);
    } else if (code == SchemaMappings.timestampp) {
      long val = ((Timestamp)elem).getTime();
      int nano = ((Timestamp)elem).getNanos();
      DataSerializer.writePrimitiveLong(val, hdos);
      DataSerializer.writePrimitiveInt(nano, hdos);
    }
  }

  /*
  public static void serializeRaw(Object value, byte schemaCode, HeapDataOutputStream hdos) throws IOException {
    DataSerializer.writePrimitiveBoolean(value != null, hdos);
    if (value != null) {
      writeNotNull(value, hdos, schemaCode);
    }
  }
  */
}
