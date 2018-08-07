/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.HiveRelMdDistinctRowCount;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.HiveRelMdRowCount;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.HiveRelMdSelectivity;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.HiveRelMdSize;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.HiveRelMdUniqueKeys;

/**
 * Metadata provider specifically created for materialized views (vs metadata provider
 * used in other phases such as join reordering).
 * This metadata provider takes some useful pieces from the default Hive provider
 * (selectivity improvements or distinct row count) and combines them with some of the
 * original Calcite providers, e.g., overall cost, since provider for cost in default
 * Hive cost model is tailored towards join reordering, e.g., cost is accounted only
 * for join operators and not other operators, etc.
 */
public class HiveMaterializedViewsRelMetadataProvider extends ChainedRelMetadataProvider {

  public static final HiveMaterializedViewsRelMetadataProvider INSTANCE =
      new HiveMaterializedViewsRelMetadataProvider();

  protected HiveMaterializedViewsRelMetadataProvider() {
    super(
        ImmutableList.of(
            HiveRelMdDistinctRowCount.SOURCE,
            HiveRelMdSelectivity.SOURCE,
            HiveRelMdRowCount.SOURCE,
            HiveRelMdUniqueKeys.SOURCE,
            HiveRelMdSize.SOURCE,
            DefaultRelMetadataProvider.INSTANCE));
  }

}
