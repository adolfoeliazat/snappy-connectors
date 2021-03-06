/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.snappydata.spark.gemfire.connector.internal.gemfirefunctions;

import java.io.IOException;
import java.util.Iterator;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.internal.cache.CachedDeserializable;
import com.gemstone.gemfire.internal.logging.LogService;
import io.snappydata.spark.gemfire.connector.internal.gemfirefunctions.shared.NonVersionedHeapDataOutputStream;
import io.snappydata.spark.gemfire.connector.internal.gemfirefunctions.shared.StructStreamingResult;
import org.apache.logging.log4j.Logger;
/**
 * ConnectionStreamingResultSender and StructStreamingResultCollector  are paired
 * to transfer result of list of `com.gemstone.gemfire.cache.query.Struct`
 * from GemFire server to Spark Connector (the client of GemFire server)
 * in streaming, i.e., while sender sending the result, the collector can
 * start processing the arrived result without waiting for full result to
 * become available.
 */
public class ConnectionStreamingResultSender<T> implements StructStreamingResult{


  protected static final Logger logger = LogService.getLogger();
  private static final int CHUNK_SIZE = 4096;


  // Note: The type of ResultSender returned from GemFire FunctionContext is
  //  always ResultSender<Object>, so can't use ResultSender<byte[]> here
  private final ResultSender<Object> sender;

  private final Iterator<T> rows;
  private String desc;
  private boolean closed = false;
  private final boolean isValOnly;

  /**
   * the Constructor
   *
   * @param sender the base ResultSender that send data in byte array

   * @param rows   the iterator of the collection of results
   * @param desc   description of this result (used for logging)
   */
  public ConnectionStreamingResultSender(
      ResultSender<Object> sender, Iterator<T> rows,
      String desc, boolean isValOnly) {
    if (sender == null || rows == null)
      throw new NullPointerException("sender=" + sender + ", rows=" + rows);
    this.sender = sender;
    this.rows = rows;
    this.desc = desc;
    this.isValOnly = isValOnly;
  }

  /**
   * the Constructor with default `desc`
   */
  public ConnectionStreamingResultSender(
      ResultSender<Object> sender, Iterator<T> rows, boolean isValOnly) {
    this(sender, rows, "ConnectionStreamingResultSender", isValOnly);
  }

  /**
   * Send the result in chunks. There are 3 types of chunk: TYPE, DATA, and ERROR.
   * TYPE chunk for sending struct type info, DATA chunk for sending data, and
   * ERROR chunk for sending exception. There are at most 1 TYPE chunk (omitted
   * for `KeyValueType`) and 1 ERROR chunk (if there's error), but usually
   * there are multiple DATA chunks. Each DATA chunk contains multiple rows
   * of data. The chunk size is determined by the const `CHUNK_SIZE`. If an
   * exception is thrown, it is serialized and sent as the last chunk  of the
   * result (in the form of ERROR chunk).
   */
  public void send() {
    if (closed) throw new RuntimeException("sender is closed.");

    NonVersionedHeapDataOutputStream buf = new NonVersionedHeapDataOutputStream(CHUNK_SIZE + 2048);

    int typeSize = 0;
    int rowCount = 0;
    int dataSize = 0;
    try {
      if (rows.hasNext()) {
        buf.writeByte(DATA_CHUNK);
        if (isValOnly) {
          while (rows.hasNext()) {
            rowCount++;
            T row = rows.next();
            if (row instanceof CachedDeserializable) {
              buf.writeByte(SER_DATA);
              DataSerializer.writeByteArray(((CachedDeserializable)row).getSerializedValue(), buf);
            } else if (row instanceof byte[]) {
              buf.writeByte(BYTEARR_DATA);
              DataSerializer.writeByteArray((byte[])row, buf);
            } else {
              buf.writeByte(UNSER_DATA);
              this.serializeValue(row, buf);
            }
            if (buf.size() > CHUNK_SIZE) {
              dataSize += sendBufferredData(buf, false);
              buf.writeByte(DATA_CHUNK);
            }
          }
        } else {
          int rowSize = 2;
          while (rows.hasNext()) {
            rowCount++;
            Object[] row = (Object[])rows.next();
            //if (rowCount < 2) dataType = entryDataType(row);
            if (rowSize != row.length)
              throw new IOException(rowToString("Expect " + rowSize + " columns, but got ", row));
            serializeRowToBuffer(row, buf);
            if (buf.size() > CHUNK_SIZE) {
              dataSize += sendBufferredData(buf, false);
              buf.writeByte(DATA_CHUNK);
            }
          }
        }
      }
      // send last piece of data or empty byte array
      dataSize += sendBufferredData(buf, true);
      logger.info(desc + ": " + rowCount + " rows, type.size=" +
          typeSize + ", data.size=" + dataSize + ", row.avg.size=" +
          (rowCount == 0 ? "NaN" : String.format("%.1f", ((float)dataSize) / rowCount)));
    } catch (IOException | RuntimeException e) {
      logger.error("Exception in sending region data", e);
      sendException(buf, e);
    } finally {
      closed = true;
    }
  }

  protected void serializeValue(Object value, NonVersionedHeapDataOutputStream buf) throws IOException {
    DataSerializer.writeObject(value, buf);
  }

  private String rowToString(String rowDesc, Object[] row) {
    StringBuilder buf = new StringBuilder();
    buf.append(rowDesc).append("(");
    for (int i = 0; i < row.length; i++) buf.append(i == 0 ? "" : " ,").append(row[i]);
    return buf.append(")").toString();
  }
/*
  private String entryDataType(Object[] row) {
    StringBuilder buf = new StringBuilder();
    buf.append("(");
    for (int i = 0; i < row.length; i++) {
      if (i != 0) buf.append(", ");
      buf.append(row[i].getClass().getCanonicalName());
    }
    return buf.append(")").toString();
  }
  */

  protected void serializeRowToBuffer(Object[] row, NonVersionedHeapDataOutputStream buf) throws IOException {
    for (Object data : row) {
      if (data instanceof CachedDeserializable) {
        buf.writeByte(SER_DATA);
        DataSerializer.writeByteArray(((CachedDeserializable)data).getSerializedValue(), buf);
      } else if (data instanceof byte[]) {
        buf.writeByte(BYTEARR_DATA);
        DataSerializer.writeByteArray((byte[])data, buf);
      } else {
        buf.writeByte(UNSER_DATA);
        DataSerializer.writeObject(data, buf);
      }
    }
  }



  private int sendBufferredData(NonVersionedHeapDataOutputStream buf, boolean isLast) throws IOException {
    if (isLast) sender.lastResult(buf.toByteArray());
    else sender.sendResult(buf.toByteArray());
    // logData(buf.toByteArray(), desc);
    int s = buf.size();
    buf.reset();
    return s;
  }

  /**
   * Send the exception as the last chunk of the result.
   */
  private void sendException(NonVersionedHeapDataOutputStream buf, Exception e) {
    // Note: if exception happens during the serialization, the `buf` may contain
    // partial serialized data, which may cause de-serialization hang or error.
    // Therefore, always empty the buffer before sending the exception
    if (buf.size() > 0) buf.reset();

    try {
      buf.writeByte(ERROR_CHUNK);
      DataSerializer.writeObject(e, buf);
    } catch (IOException ioe) {
      logger.error("ConnectionStreamingResultSender failed to send the result:", e);
      logger.error("ConnectionStreamingResultSender failed to serialize the exception:", ioe);
      buf.reset();
    }
    // Note: send empty chunk as the last result if serialization of exception 
    // failed, and the error is logged on the GemFire server side.
    sender.lastResult(buf.toByteArray());
    // logData(buf.toByteArray(), desc);
  }

//  private void logData(byte[] data, String desc) {
//    StringBuilder buf = new StringBuilder();
//    buf.append(desc);
//    for (byte b : data) {
//      buf.append(" ").append(b);
//    }
//    logger.info(buf.toString());
//  }

}
