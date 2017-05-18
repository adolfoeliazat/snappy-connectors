package io.snappydata.spark.gemfire.connector.internal.gemfirefunctions;

import java.io.IOException;
import java.util.Iterator;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.internal.cache.CachedDeserializable;

import io.snappydata.spark.gemfire.connector.internal.GemFireRow;
import io.snappydata.spark.gemfire.connector.internal.gemfirefunctions.shared.NonVersionedHeapDataOutputStream;

public class RowStreamingResultSender<T>  extends ConnectionStreamingResultSender<T> {
  /**
   * the Constructor
   *
   * @param sender the base ResultSender that send data in byte array
   * @param rows   the iterator of the collection of results
   * @param desc   description of this result (used for logging)
   */
  public RowStreamingResultSender(
      ResultSender<Object> sender, Iterator<T> rows,
      String desc, boolean isValOnly) {
    super(sender, rows, desc, isValOnly);
  }

  /**
   * the Constructor with default `desc`
   */
  public RowStreamingResultSender(
      ResultSender<Object> sender, Iterator<T> rows, boolean isValOnly) {
    this(sender, rows, "RowStreamingResultSender", isValOnly);
  }

  @Override
  protected void serializeValue(Object value, NonVersionedHeapDataOutputStream buf) throws IOException {
    ((GemFireRow)value).toDataWithoutTopSchema(buf);
  }


  @Override
  protected void serializeRowToBuffer(Object[] row, NonVersionedHeapDataOutputStream buf) throws IOException {
    this.serialize(row[0], true, buf);
    this.serialize(row[1], false, buf);
  }

  private void serialize(Object data , boolean isKey, NonVersionedHeapDataOutputStream buf) throws IOException {
    if (data instanceof CachedDeserializable) {
      buf.writeByte(SER_DATA);
      DataSerializer.writeByteArray(((CachedDeserializable)data).getSerializedValue(), buf);
    } else if (data instanceof byte[]) {
      buf.writeByte(BYTEARR_DATA);
      DataSerializer.writeByteArray((byte[])data, buf);
    } else {
      buf.writeByte(UNSER_DATA);
      if (isKey) {
        DataSerializer.writeObject(data, buf);
      } else {
        this.serializeValue(data, buf);
      }
    }
  }

}