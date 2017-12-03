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
package org.apache.hadoop.hive.metastore;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hive.metastore.api.Materialization;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * 
 *
 */
@SuppressWarnings("serial")
public class MaterializationInvalidationInfo extends Materialization {

  /*
   * 
   */
  private AtomicInteger invalidationTime;

  public MaterializationInvalidationInfo(Table materializationTable, Set<String> tablesUsed) {
    super(materializationTable, tablesUsed, 0);
    this.invalidationTime = new AtomicInteger(0);
  }

  public boolean compareAndSetInvalidationTime(int expect, int update) {
    boolean success = invalidationTime.compareAndSet(expect, update);
    if (success) {
      super.setInvalidationTime(update);
    }
    return success;
  }

  public int getInvalidationTime() {
    return invalidationTime.get();
  }

  public void setInvalidationTime(int invalidationTime) {
    throw new UnsupportedOperationException("You should call compareAndSetInvalidationTime instead");
  }

}
