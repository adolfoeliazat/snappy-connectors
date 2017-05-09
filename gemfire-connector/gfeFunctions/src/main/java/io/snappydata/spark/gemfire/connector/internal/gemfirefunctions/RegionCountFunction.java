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


import java.util.Set;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDataStore;
import com.gemstone.gemfire.internal.cache.execute.InternalRegionFunctionContext;
import com.gemstone.gemfire.internal.logging.LogService;
import io.snappydata.spark.gemfire.connector.internal.gemfirefunctions.shared.ConnectorFunctionIDs;
import org.apache.logging.log4j.Logger;

/**
 * GemFire function that is used by `SparkContext.gemfireRegion(regionPath, whereClause)`
 * to retrieve region data set for the given bucket set as a RDD partition
 **/
public class RegionCountFunction implements Function {

  private static final Logger logger = LogService.getLogger();
  private static final RegionCountFunction instance = new RegionCountFunction();

  public RegionCountFunction() {
  }

  /** ------------------------------------------ */
  /**     interface Function implementation      */
  /**
   * ------------------------------------------
   */

  public static RegionCountFunction getInstance() {
    return instance;
  }

  @Override
  public String getId() {
    return ConnectorFunctionIDs.RegionCountFunction_ID;
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
    InternalRegionFunctionContext irfc = (InternalRegionFunctionContext)context;

    LocalRegion localRegion = (LocalRegion)irfc.getDataSet();
    boolean partitioned = localRegion.getDataPolicy().withPartitioning();
    Set<Integer> buckets = irfc.getLocalBucketSet(localRegion);
    String[] args = (String[])context.getArguments();
    String where = args[0];

    long count = 0;
    if("".equals(where)) {
      if (partitioned && !buckets.isEmpty()) {
        PartitionedRegionDataStore pds = ((PartitionedRegion)localRegion).getDataStore();
        for (int bucketId : buckets) {
          count += pds.getLocalBucketById(bucketId).size();
        }
      } else {
        count = localRegion.size();
      }
    } else {
      String regionPath = localRegion.getFullPath();
      String queryString = "select count(*) from " + regionPath + " x  where " + where ;
      Query query = CacheFactory.getAnyInstance().getQueryService().newQuery(queryString);
      try {
        Object result = partitioned ? query.execute((InternalRegionFunctionContext)context) : query.execute();
        count = ((Integer)((SelectResults)result).iterator().next()).longValue();
      } catch( Exception e) {
        throw new FunctionException(e);
      }

    }
    if(logger.isDebugEnabled()) {
      logger.debug("RegionCountFunction : for buckets = " + buckets + " total count obtained=" + count);
    }
    context.getResultSender().lastResult(count);
  }
}
