package io.snappydata.spark.gemfire.connector.internal.gemfirefunctions.shared;

/**
 * Created by ashahid on 6/13/17.
 */
public class Utils {

  public static int getNumberOfPositionsForFieldAddress(int schemaLen){
    return schemaLen / 2 + schemaLen % 2;
  }

  public static long setOddPositionAddressAndGetFinalLong(int partAddressEven, int partAddressOdd){
    return ((long)partAddressOdd << 32) | partAddressEven;
  }

  public static int getPartAddress(long combined, boolean evenPart) {
    if (evenPart) {
     return (int)(combined & 0xffffffff) ;
    } else {
      return  (int)(combined >>> 32);
    }
  }

}


