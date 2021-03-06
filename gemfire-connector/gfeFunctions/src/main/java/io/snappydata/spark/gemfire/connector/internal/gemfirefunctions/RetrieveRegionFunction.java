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

import java.util.Iterator;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.internal.cache.LocalDataSet;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.execute.InternalRegionFunctionContext;
import com.gemstone.gemfire.internal.cache.execute.InternalResultSender;
import com.gemstone.gemfire.internal.cache.partitioned.PREntriesIterator;
import com.gemstone.gemfire.internal.logging.LogService;
import io.snappydata.spark.gemfire.connector.internal.gemfirefunctions.shared.ConnectorFunctionIDs;
import org.apache.logging.log4j.Logger;
/**
 * GemFire function that is used by `SparkContext.gemfireRegion(regionPath, whereClause)`
 * to retrieve region data set for the given bucket set as a RDD partition
 **/
public class RetrieveRegionFunction implements Function {

  private static final Logger logger = LogService.getLogger();
  private static final RetrieveRegionFunction instance = new RetrieveRegionFunction();

  public RetrieveRegionFunction() {
  }

  /** ------------------------------------------ */
  /**     interface Function implementation      */
  /**
   * ------------------------------------------
   */

  public static RetrieveRegionFunction getInstance() {
    return instance;
  }

  @Override
  public String getId() {
    return ConnectorFunctionIDs.RetrieveRegionFunction_ID;
  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public boolean optimizeForWrite() {
    return true;
  }

  @Override
  public boolean isHA() {
    return true;
  }

  @Override
  public void execute(FunctionContext context) {
    String[] args = (String[])context.getArguments();
    String where = args[0];
    String taskDesc = args[1];
    int keyLength = Integer.parseInt(args[2]);
    boolean schemaAvailableOnClient = Boolean.valueOf(args[3]);
    InternalRegionFunctionContext irfc = (InternalRegionFunctionContext)context;
    LocalRegion localRegion = (LocalRegion)irfc.getDataSet();
    boolean partitioned = localRegion.getDataPolicy().withPartitioning();
    if (where.trim().isEmpty())
      retrieveFullRegion(irfc, partitioned, taskDesc, keyLength, schemaAvailableOnClient);
    else
      retrieveRegionWithWhereClause(irfc, localRegion, partitioned, where, taskDesc, keyLength,
          schemaAvailableOnClient);
  }

  /** ------------------------------------------ */
  /**    Retrieve region data with where clause  */
  /**
   * ------------------------------------------
   */

  private void retrieveRegionWithWhereClause(
      InternalRegionFunctionContext context, LocalRegion localRegion, boolean partitioned, String where,
      String desc, int keyLength, boolean schemaAvailableOnClient) {
    String regionPath = localRegion.getFullPath();
    String qstr = "";
    if( keyLength == 0) {
      qstr = "select * from " + regionPath + " where " + where;
    } else {
      qstr = "select key, value from " + regionPath + ".entries where " + where;
    }
    logger.info(desc + ": " + qstr);

    try {
      Cache cache = CacheFactory.getAnyInstance();
      QueryService queryService = cache.getQueryService();
      Query query = queryService.newQuery(qstr);
      SelectResults<?> results = (SelectResults<?>)(partitioned ? query.execute(context) : query.execute());
      InternalResultSender irs = (InternalResultSender)context.getResultSender();
      if(keyLength == 0) {
        ConnectionStreamingResultSender sender =  schemaAvailableOnClient? new RowStreamingResultSender<>(irs,
            ((SelectResults<Object>)results).asList().iterator(), desc, true):
            new ConnectionStreamingResultSender<>(irs, ((SelectResults<Object>)results).asList().iterator(),
                desc, true);
        sender.send();
      } else {
        Iterator<Object[]> entries = getStructIteratorWrapper(((SelectResults<Struct>)results).asList().iterator());
        ConnectionStreamingResultSender sender = schemaAvailableOnClient? new RowStreamingResultSender<>(irs,
            entries, desc, false): new ConnectionStreamingResultSender<>(irs, entries, desc, false);
        sender.send();
      }

    } catch (Exception e) {
      throw new FunctionException(e);
    }
  }

  private Iterator<Object[]> getStructIteratorWrapper(Iterator<Struct> entries) {
    return new WrapperIterator<Struct, Iterator<Struct>>(entries) {
      @Override
      public Object[] next() {
        return delegate.next().getFieldValues();
      }
    };
  }

  /** ------------------------------------------ */
  /**         Retrieve full region data          */
  /**
   * ------------------------------------------
   */

   private  void retrieveFullRegion(InternalRegionFunctionContext context, boolean partitioned,
      String desc, int keyLength, boolean schemaAvailableOnClient) {
    Iterator<?> dataIter;
    if (partitioned) {
      if(keyLength == 0) {
        dataIter =((LocalDataSet)PartitionRegionHelper.getLocalDataForContext(context)).values().iterator();

      } else {
        PREntriesIterator<Region.Entry> iter = (PREntriesIterator<Region.Entry>)
            ((LocalDataSet)PartitionRegionHelper.getLocalDataForContext(context)).entrySet().iterator();

        dataIter = getSimpleEntryIterator(iter);
      }
    } else {
      LocalRegion owner = (LocalRegion)context.getDataSet();
      if (keyLength == 0) {
        dataIter = owner.values().iterator();
      } else {
        Iterator<Region.Entry> iter =(Iterator<Region.Entry>)owner.entrySet().iterator();
        // entries = getRREntryIterator(iter, owner);
        dataIter = getSimpleEntryIterator(iter);
      }
    }
    InternalResultSender irs = (InternalResultSender)context.getResultSender();

     if(keyLength == 0) {
       ConnectionStreamingResultSender sender =  schemaAvailableOnClient?
           new RowStreamingResultSender<>(irs, (Iterator<Object>)dataIter, desc,
               keyLength == 0):
           new ConnectionStreamingResultSender<>(irs, (Iterator<Object>)dataIter, desc,
               keyLength == 0);
       sender.send();
     } else {
       ConnectionStreamingResultSender<?> sender = schemaAvailableOnClient ?
           new RowStreamingResultSender<>(irs, (Iterator<Object[]>)dataIter, desc, keyLength == 0):
           new ConnectionStreamingResultSender<>(irs, (Iterator<Object[]>)dataIter, desc, keyLength == 0);
       sender.send();
     }

  }

//  /** An iterator for partitioned region that uses internal API to get serialized value */
//  private Iterator<Object[]> getPREntryIterator(PREntriesIterator<Region.Entry> iterator) {
//    return new WrapperIterator<Region.Entry, PREntriesIterator<Region.Entry>>(iterator) {
//      @Override public Object[] next() {
//        Region.Entry entry = delegate.next();
//        int bucketId = delegate.getBucketId();
//        KeyInfo keyInfo = new KeyInfo(entry.getKey(), null, bucketId);
//        // owner needs to be the bucket region not the enclosing partition region
//        LocalRegion owner = ((PartitionedRegion) entry.getRegion()).getDataStore().getLocalBucketById(bucketId);
//        Object value = owner.getDeserializedValue(keyInfo, false, true, true, null, false);
//        return new Object[] {keyInfo.getKey(), value};
//      }
//    };
//  }
//
//  /** An iterator for replicated region that uses internal API to get serialized value */
//  private Iterator<Object[]> getRREntryIterator(Iterator<Region.Entry> iterator, LocalRegion region) {
//    final LocalRegion owner = region;
//    return new WrapperIterator<Region.Entry, Iterator<Region.Entry>>(iterator) {
//      @Override public Object[] next() {
//        Region.Entry entry =  delegate.next();
//        KeyInfo keyInfo = new KeyInfo(entry.getKey(), null, null);
//        Object value = owner.getDeserializedValue(keyInfo, false, true, true, null, false);
//        return new Object[] {keyInfo.getKey(), value};
//      }
//    };
//  }

  // todo. compare performance of regular and simple iterator

  /**
   * An general iterator for both partitioned and replicated region that returns un-serialized value
   */
  private Iterator<Object[]> getSimpleEntryIterator(Iterator<Region.Entry> iterator) {
    return new WrapperIterator<Region.Entry, Iterator<Region.Entry>>(iterator) {
      @Override
      public Object[] next() {
        Region.Entry entry = delegate.next();
        return new Object[]{entry.getKey(), entry.getValue()};
      }
    };
  }

  /** ------------------------------------------ */
  /**        abstract wrapper iterator           */
  /** ------------------------------------------ */

  /**
   * An abstract wrapper iterator to reduce duplicated code of anonymous iterators
   */
  abstract class WrapperIterator<T, S extends Iterator<T>> implements Iterator<Object[]> {

    final S delegate;

    protected WrapperIterator(S delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
      return delegate.hasNext();
    }

    @Override
    public void remove() {
    }
  }
}
