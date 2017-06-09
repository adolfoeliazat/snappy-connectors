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

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.cache.LocalDataSet;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.execute.InternalRegionFunctionContext;
import com.gemstone.gemfire.internal.logging.LogService;
import io.snappydata.spark.gemfire.connector.internal.gemfirefunctions.shared.ConnectorFunctionIDs;
import org.apache.logging.log4j.Logger;

public class QueryFunction implements Function {


  private static final long serialVersionUID = 4866641340803692882L;
  private final static QueryFunction instance = new QueryFunction();

  private static final Logger logger = LogService.getLogger();

  private static final int CHUNK_SIZE = 4096;

  public static QueryFunction getInstance() {
    return instance;
  }

  @Override
  public String getId() {
    return ConnectorFunctionIDs.QueryFunction_ID;
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
  public boolean hasResult() {
    return true;
  }

  @Override
  public void execute(FunctionContext context) {
    try {
      String[] args = (String[])context.getArguments();
      String queryString = args[0];
      String bucketSet = args[1];
      String schemaMapping = args[2];
      byte[] schemaCode = null;
      if (!"".equals(schemaMapping)) {
        StringTokenizer stz = new StringTokenizer(schemaMapping, ",");
        schemaCode = new byte[stz.countTokens()];
        int i = 0;
        while (stz.hasMoreTokens()) {
          schemaCode[i++] = Byte.valueOf(stz.nextToken());
        }
      }
      InternalRegionFunctionContext irfc = (InternalRegionFunctionContext)context;
      LocalRegion localRegion = (LocalRegion)irfc.getDataSet();
      boolean partitioned = localRegion.getDataPolicy().withPartitioning();

      Query query = CacheFactory.getAnyInstance().getQueryService().newQuery(queryString);
      Iterator<Object> iter = null;
      if (partitioned) {
        iter = new BucketIterator(query, irfc, logger);
      } else {
        Object result = query.execute();
        iter = ((SelectResults)result).iterator();

      }
      ResultSender<Object> sender = context.getResultSender();
      HeapDataOutputStream buf = new HeapDataOutputStream(CHUNK_SIZE, null);


      if (schemaCode != null && schemaCode.length > 1) {
        while (iter.hasNext()) {
          Struct row = (Struct)iter.next();
          Object[] values = row.getFieldValues();
          OQLRowSerializer.serializeRaw(values, schemaCode, buf);

          if (buf.size() > CHUNK_SIZE) {
            sender.sendResult(buf.toByteArray());

            if (logger.isDebugEnabled()) {
              logger.debug("OQL query=" + queryString +
                  " bucket set=" + bucketSet + " sendResult(), data size=" + buf.size());
            }
            buf.reset();
          }
        }
      } else {
        while (iter.hasNext()) {
          Object row = iter.next();
          DataSerializer.writeObject(row, buf);
          if (buf.size() > CHUNK_SIZE) {
            sender.sendResult(buf.toByteArray());
            if (logger.isDebugEnabled()) {
              logger.debug("OQL query=" + queryString + " bucket set=" + bucketSet +
                  " sendResult(), data size=" + buf.size());
            }

            buf.reset();
          }
        }
      }

      sender.lastResult(buf.toByteArray());
      if (logger.isInfoEnabled()) {
        logger.info("OQL query=" + queryString + " bucket set=" + bucketSet + " lastResult(), data size=" + buf.size());
      }
      buf.reset();
    } catch (Exception e) {
      logger.error("QueryFunction:: Exception in executing function", e);
      throw new FunctionException(e);
    }

  }


  static class WrapperRegionFunctionContext implements InternalRegionFunctionContext {
    private InternalRegionFunctionContext delegate;
    private int bucket;

    WrapperRegionFunctionContext(InternalRegionFunctionContext delegate, int bucket) {
      this.delegate = delegate;
      this.bucket = bucket;
    }

    @Override
    public <K, V> Region<K, V> getLocalDataSet(Region<K, V> region) {
      return new LocalDataSet(
          ((LocalDataSet)this.delegate.getLocalDataSet(region)).getProxy(), Collections.singleton(bucket));

    }

    @Override
    public Map<String, LocalDataSet> getColocatedLocalDataSets() {
      throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public <K, V> Set<Integer> getLocalBucketSet(Region<K, V> region) {
      return Collections.singleton(this.bucket);
    }

    @Override
    public Set<?> getFilter() {
      return this.delegate.getFilter();
    }

    @Override
    public <K, V> Region<K, V> getDataSet() {
      return this.delegate.getDataSet();
    }

    @Override
    public Object getArguments() {
      return this.delegate.getArguments();
    }

    @Override
    public String getFunctionId() {
      return this.delegate.getFunctionId();
    }

    @Override
    public <T> ResultSender<T> getResultSender() {
      return this.delegate.getResultSender();
    }

    @Override
    public boolean isPossibleDuplicate() {
      return this.delegate.isPossibleDuplicate();
    }
  }

  static class BucketIterator implements Iterator<Object> {
    private Iterator<Integer> bucketsIter;

    Query query;
    Logger logger;
    InternalRegionFunctionContext irfc;
    private Iterator<Object> currentIterator = null;

    BucketIterator(Query query,
        InternalRegionFunctionContext irfc, Logger logger) {
      this.bucketsIter = irfc.getLocalBucketSet(irfc.getDataSet()).iterator();
      this.query = query;
      this.irfc = irfc;
      this.logger = logger;
    }

    @Override
    public boolean hasNext() {
      while (true) {
        if (!(bucketsIter.hasNext() || currentIterator.hasNext())) {
          return false;
        } else {
          if (currentIterator != null && currentIterator.hasNext()) {
            return true;
          } else {
            try {
              int bucket = bucketsIter.next();
              if (logger.isDebugEnabled()) {
                logger.debug("BucketIterator: about to query for bucket id =" + bucket);
              }

              InternalRegionFunctionContext wrfc = new WrapperRegionFunctionContext(irfc, bucket);
              // Thread.sleep(500);

              Object result = query.execute(wrfc);

              this.currentIterator = ((SelectResults)result).iterator();
            } catch (Exception ie) {
              throw new FunctionException(ie);
            }
          }
        }
      }
    }

    @Override
    public Object next() {
      return this.currentIterator.next();
    }
  }

}


