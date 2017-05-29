package io.snappydata.app;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.function.BiPredicate;

import io.snappydata.app.SQLServerCdcBase;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SnappySession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.jdbc.SnappyStreamSink;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.Utils;

import static org.apache.spark.SnappyJavaUtils.snappyJavaUtil;

public class JavaCdcStreamingTestSuite extends SQLServerCdcBase {

  private JavaCdcStreamingTestSuite(String[] args) throws Exception {
    super(args, null);
  }

  public static void main(String[] args) throws Exception {
    JavaCdcStreamingTestSuite _this = new JavaCdcStreamingTestSuite(fillDefaults(args));
    _this.connect();
    ProcessEvents p = new ProcessEvents();
    _this.startJob();
  }

  @Override
  protected SparkConf extraConf(SparkConf conf) {
    conf.setMaster("local[4]");
    return conf;
  }

  @Override
  protected StreamingQuery getStreamWriter(String tableName,
      Dataset<Row> reader) throws IOException {

    return reader.writeStream()
        .format("snappystore")
        .option("sink", ProcessEvents.class.getName())
        .option("checkpointLocation",
            Utils.createTempDir("/data/wrk/w/snappydata/tmg-temp", "tmg-spark")
                .getCanonicalPath())
        .option("tableName", tableName)
        .start();
  }

  public static class ProcessEvents implements SnappyStreamSink {

    private static Logger log = Logger.getLogger(ProcessEvents.class.getName());

    @Override
    public void process(SnappySession snappySession, Properties sinkProps,
        long batchId, Dataset<Row> df) {

      /*
        NOTES:
          The incoming df has few conversions automatically done compared to the current c#
          script of data type mappings.

          firstly, microsoft sql server data types are mapped to standard jdbc types done by the
          microsoft jdbc driver used here.

          secondly, using auto type inference, snappydata custom StreamSource utilizes JdbcRDD to
           automap the incoming jdbc type to spark catalyst type and therefore ready to be
           consumed by the SnappySession apis.
       */

      String sqlsrvTable = sinkProps.getProperty("tableName").toUpperCase();
      String table = sqlsrvTable.substring(sqlsrvTable.indexOf("DBO_") + 4,
          sqlsrvTable.indexOf("_CT"));
      log.info("SB: Processing for " + table + " batchId " + batchId);

      /* --------------[ Preferred Way ] ----------------

          Try to use df api(s) as far as possbible so that
          a) plan -> plan transformations are captured and intermitent garbage is less,
          b) plan level optimization is possible whereever applicable and stage push might happen.
          c) better code readability

          One can choose to implement UDFs for complex column handling and apply them here
          instead of handling all the columns all the time.
       --------------------------------------------------*/
      StructType dfSchema = df.schema();
      String[] columns = new String[dfSchema.size() - 6];
      for (int i = 6; i < dfSchema.size(); i++) {
        columns[i - 6] = dfSchema.apply(i).name();
      }

      Dataset<Row> snappyCustomerUpsert = df
          // pick only insert/update ops
          .filter("\"__$operation\" = 4 OR \"__$operation\" = 2")
          // exclude the first 5 columns and pick the rest as columns have
          // 1-1 correspondence with snappydata customer table.
          // For more complex mapping, one can take a UDF route instead of
          // .mapPartitions as demonstrated below.
          .select(dfSchema.apply(5).name(), columns);

      // System.out.println(snappyCustomerUpsert.count());
      // a simple snappySession.putInto(...) will be provided shortly.
      snappyJavaUtil(snappyCustomerUpsert.write()
          .format("row")
      ).putInto("APP." + table);

      Dataset<Row> snappyCustomerDelete = df
          // pick only delete ops
          .filter("\"__$operation\" = 1")
          // exclude the first 5 columns and pick the columns that needs to control
          // the WHERE clause of the delete operation.
          .select(dfSchema.apply(5).name());

      // System.out.println(snappyCustomerUpsert.count());
      // a simple snappySession.deletFrom(...) will be provided shortly.
      snappyJavaUtil(snappyCustomerDelete.write()
          .format("row")
      ).deleteFrom("APP." + table);

//      /* --------------[ Alternate Way ] ----------------
//
//        Here we are tranforming the incoming row from its internal form to external form
//        (GenericRow) and then the returned GenericRowWithSchema is transformed back to internal
//        form (InternalRow).
//
//        This have an obvious disadvantage in terms of garbage and overhead to deal with multiple
//        columns but offers flexibility to expand into a custom java code.
//
//       --------------------------------------------------*/
//      StructType snappyCustomerSchema = snappySession.table("APP.CUSTOMER").schema();
//      ExpressionEncoder<Row> encoder = RowEncoder.apply(dfSchema);
//      Dataset<Row> snappy_customer = df.mapPartitions((MapPartitionsFunction<Row, Row>)input ->
//          new Iterator<Row>() {
//
//            // caching the next valid row for consumption in .next()
//            private Row sqlserverRow;
//
//            @Override
//            public boolean hasNext() {
//              do {
//                if (input.hasNext()) {
//                  sqlserverRow = input.next();
//                } else {
//                  sqlserverRow = null;
//                }
//
//                // skipping everything other than delete operation
//              } while(sqlserverRow != null && sqlserverRow.getInt(1) != 1);
//
//              return sqlserverRow != null;
//            }
//
//            @Override
//            public Row next() {
//              // map the sqlserver incoming row with snappydata target row.
//              Object[] newRow = new Object[snappyCustomerSchema.size()];
//
//              for (int i = 0; i < newRow.length; i++) {
//                newRow[i] = sqlserverRow.get(i + 5); // after first 3 columns, all are 1-1 mapped.
//              }
//
//              return new GenericRowWithSchema(newRow, snappyCustomerSchema);
//            }
//          }, encoder);
//
//      // a simple snappySession.deleteFrom(...) will be provided shortly.
//      snappyJavaUtil(snappy_customer.write()
//          .format("row")
//          .mode(SaveMode.Append)
//      ).deleteFrom("APP.CUSTOMER");
    }

    public ProcessEvents() {

    }

  }

  private static String[] fillDefaults(String[] args) {
    ArrayList newArgs = new ArrayList();
    Collections.addAll(newArgs, args);

    BiPredicate<String, String> idxOf = (a, b) -> a.indexOf(b) > 0 || b.indexOf(a) > 0;
    if (!contains(args, "driver", idxOf)) {
      newArgs.add("driver=com.microsoft.sqlserver.jdbc.SQLServerDriver");
    }
    if (!contains(args, "url", idxOf)) {
      newArgs.add("url=jdbc:sqlserver://snappydb16.westus.cloudapp.azure.com:1433");
    }
    if (!contains(args, "user", idxOf)) {
      newArgs.add("user=sqldb");
    }
    if (!contains(args, "password", idxOf)) {
      newArgs.add("password=snappydata#msft1");
    }

    if (!contains(args, "snappydata.connection", idxOf)) {
      newArgs.add("snappydata.connection=localhost:1527");
    }

    if (!contains(args, "tables", idxOf)) {
      newArgs.add("tables=tengb.cdc.dbo_customer_CT");
    }

    if (!contains(args, "databaseName", idxOf)) {
      newArgs.add("databaseName=tengb");
    }

    if (!contains(args, "pollInternal", idxOf)) {
      newArgs.add("pollInternal=30"); // poll for CDC events once every 30 seconds
    }

    if (!contains(args, "conflate", idxOf)) {
      newArgs.add("conflate=true"); // conflate the events
    }

    if (!contains(args, "fullMode", idxOf)) {
      // clears the recorded state
      // and starts from begining of the table.
      newArgs.add("fullMode=false");
    }

    if (!contains(args, "maxEvents", idxOf)) {
      newArgs.add("maxEvents=50000"); // poll for CDC events once every 30 seconds
    }

    if(!contains(args, "tengb.cdc.dbo_customer_CT.partitionBy", idxOf)) {
      newArgs.add("tengb.cdc.dbo_customer_CT.partitionBy=convert(varchar(20), C_CustKey)");
    }

    // --------------------------------------------
    // below are more for tuning purposes.
    // --------------------------------------------
    if (!contains(args, "tengb.cdc.dbo_customer_CT.partitionByQuery", idxOf)) {
      newArgs.add("tengb.cdc.dbo_customer_CT.partitionByQuery=" +
          "select distinct C_CustKey from $getBatch");
    }
/*
    // example-1
    if (!contains(args, "tengb.cdc.dbo_customer_CT.partitionByQuery", idxOf)) {
      newArgs.add("tengb.cdc.dbo_customer_CT.partitionByQuery=" +
          "ordered:select distinct C_CustKey from $getBatch order by 1");
    }

    // example-2
    if (!contains(args, "tengb.cdc.dbo_customer_CT.partitionByQuery", idxOf)) {
      newArgs.add("tengb.cdc.dbo_customer_CT.partitionByQuery=" +
          "ranged:select min(C_CustKey) lowerInclusive, max(C_CustKey) upperInclusive " +
          "from (" +
          "  select C_CustKey, ntile($parallelism) over (order by C_CustKey) rank " +
          "  from ( " +
          "     select distinct C_CustKey from customer" +
          "  ) uniqueKeys " +
          ") rankedTable " +
          "group by rank");
    }

    // example-1
    if(!contains(args, "tengb.cdc.dbo_customer_CT.cachePartitioningValuesFrom", idxOf)) {
      newArgs.add("tengb.cdc.dbo_customer_CT.cachePartitioningValuesFrom=" +
          "select distinct $partitionBy as parts from customer");
    }

    // example-2
    if(!contains(args, "tengb.cdc.dbo_customer_CT.cachePartitioningValuesFrom", idxOf)) {
      newArgs.add("tengb.cdc.dbo_customer_CT.cachePartitioningValuesFrom=" +
          "ordered:select distinct $partitionBy as parts from customer order by $partitionBy ");
    }

    // example-3
    if(!contains(args, "tengb.cdc.dbo_customer_CT.cachePartitioningValuesFrom", idxOf)) {
      newArgs.add("tengb.cdc.dbo_customer_CT.cachePartitioningValuesFrom=" +
          "ranged: select min($partitionBy) lowerBoundInclusive, " +
          " max($partitionBy) upperBoundInclusive FROM " +
          "(select distinct $partitionBy as parts, category from customer) uniqueValues " +
          "group by category");
    }
*/

    return (String[])newArgs.toArray(new String[newArgs.size()]);
  }

  private static boolean contains(String[] list,
      String search,
      BiPredicate<String, String> eval) {
    for (String a : list) {
      if (eval.test(a, search)) {
        return true;
      }
    }

    return false;
  }

//  static class WriterToSnappyData extends ForeachWriter<Row> {
//    private SnappySession session;
//
//    public WriterToSnappyData(SnappySession session) {
//      this.session = session;
//    }
//
//    @Override
//    public void process(Row value) {
//      StructField[] types = value.schema().fields();
//      int operation = value.getInt(1);
//      switch (operation) {
//        case 1: // delete
//          // println(s"DELETE entry ${r.get(5)}")
//          break;
//        case 2: // insert
//          // println(s"INSERT entry ${r.get(5)}")
//          break;
//        case 4 | 5: // update
//          // println(s"UPSERT entry ${r.get(5)}")
//          break;
//      }
//    }
//
//    @Override
//    public void close(Throwable errorOrNull) {
//
//    }
//
//    @Override
//    public boolean open(long partitionId, long version) {
//      return true;
//    }
//  }
//

}
