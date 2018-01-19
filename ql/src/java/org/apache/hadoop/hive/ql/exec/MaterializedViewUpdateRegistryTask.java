/**
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

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveMaterializedViewsRegistry;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration.AnalyzeState;
import org.apache.hadoop.hive.ql.plan.api.StageType;

import java.io.Serializable;

/**
 * This task adds the materialized view to the registry.
 */
public class MaterializedViewUpdateRegistryTask extends Task<MaterializedViewUpdateRegistryWork> implements Serializable {

  private static final long serialVersionUID = 1L;

  public MaterializedViewUpdateRegistryTask() {
    super();
  }

  @Override
  public int execute(DriverContext driverContext) {
    if (driverContext.getCtx().getExplainAnalyze() == AnalyzeState.RUNNING) {
      return 0;
    }
    try {
      if (getWork().isRetrieveAndInclude()) {
        Hive db = Hive.get(conf);
        Table mvTable = db.getTable(getWork().getViewName());
        HiveMaterializedViewsRegistry.get().createMaterializedView(db.getConf(), mvTable);
      } else if (getWork().isDisableRewrite()) {
        // Disabling rewriting, removing from cache
        String[] names = getWork().getViewName().split("\\.");
        HiveMaterializedViewsRegistry.get().dropMaterializedView(names[0], names[1]);
      }
    } catch (HiveException e) {
      LOG.debug("Exception during materialized view cache update", e);
    }
    return 0;
  }

  @Override
  public StageType getType() {
    return StageType.DDL;
  }

  @Override
  public String getName() {
    return MaterializedViewUpdateRegistryTask.class.getSimpleName();
  }
}
