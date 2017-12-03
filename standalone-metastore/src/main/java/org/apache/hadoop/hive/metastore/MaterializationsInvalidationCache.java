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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.commons.lang.ObjectUtils;
import org.apache.curator.shaded.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hive.metastore.api.BasicNotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * 
 */
public final class MaterializationsInvalidationCache {

  private static final Logger LOG = LoggerFactory.getLogger(MaterializationsInvalidationCache.class);

  /* Singleton */
  private static final MaterializationsInvalidationCache SINGLETON = new MaterializationsInvalidationCache();

  /* Key is the database name. Value a map from a unique identifier for the view comprising
   * the qualified name and the creation time, to the view object.
   * Since currently we cannot alter a materialized view, that should suffice to identify
   * whether the cached view is up to date or not.
   * Creation time is useful to ensure correctness in case multiple HS2 instances are used. */
  private final ConcurrentMap<String, ConcurrentMap<MaterializationKey, Materialization>> materializations =
      new ConcurrentHashMap<String, ConcurrentMap<MaterializationKey, Materialization>>();

  /*
   * 
   */
  private final ConcurrentMap<String, ConcurrentSkipListSet<TableModificationKey>> tableModifications =
      new ConcurrentHashMap<String, ConcurrentSkipListSet<TableModificationKey>>();

  private RawStore store;

  private MaterializationsInvalidationCache() {
  }

  /**
   * Get instance of HiveMaterializationsRegistry.
   *
   * @return the singleton
   */
  public static MaterializationsInvalidationCache get() {
    return SINGLETON;
  }

  /**
   * Initialize the registry for the given database. It will extract the materialized views
   * that are enabled for rewriting from the metastore for the current user, parse them,
   * and register them in this cache.
   *
   * The loading process runs on the background; the method returns in the moment that the
   * runnable task is created, thus the views will still not be loaded in the cache when
   * it does.
   */
  public void init(final RawStore store) {
    this.store = store;
    try {
      List<Table> materializations = new ArrayList<Table>();
      for (String dbName : store.getAllDatabases()) {
        materializations.addAll(
            store.getTableObjectsByName(dbName, store.getTables(dbName, null, TableType.MATERIALIZED_VIEW)));
        for (Table mv : materializations) {
          List<Table> tablesUsed =
              store.getTableObjectsByName(dbName, ImmutableList.copyOf(mv.getCreationSignature().keySet()));
          addMaterializedView(mv, ImmutableSet.copyOf(tablesUsed), false);
        }
      }
    } catch (Exception e) {
      LOG.error("Problem connecting to the metastore when initializing the view registry");
    }
  }

  /**
   * Adds a newly created materialized view to the cache.
   *
   * @param materializedViewTable the materialized view
   */
  public void createMaterializedView(Table materializedViewTable, Set<Table> tablesUsed) {
    addMaterializedView(materializedViewTable, tablesUsed, true);
  }

  /**
   * Adds the materialized view to the cache.
   *
   * @param materializedViewTable the materialized view
   */
  private void addMaterializedView(Table materializedViewTable, Set<Table> tablesUsed, boolean create) {
    // Bail out if it is not enabled for rewriting
    if (!materializedViewTable.isRewriteEnabled()) {
      return;
    }
    // We are going to create the map for each view in the given database
    ConcurrentMap<MaterializationKey, Materialization> cq =
        new ConcurrentHashMap<MaterializationKey, Materialization>();
    final ConcurrentMap<MaterializationKey, Materialization> prevCq = materializations.putIfAbsent(
        materializedViewTable.getDbName(), cq);
    if (prevCq != null) {
      cq = prevCq;
    }
    // Bail out if it already exists
    final MaterializationKey vk = new MaterializationKey(
        materializedViewTable.getTableName(), materializedViewTable.getCreateTime());
    if (cq.containsKey(vk)) {
      return;
    }
    // Start the process to add materialization to the cache
    final Materialization materialization = new Materialization(materializedViewTable, tablesUsed);
    // Before loading the materialization in the cache, we need to update some
    // important information in the registry to account for rewriting invalidation
    for (Table tableUsed : tablesUsed) {
      final String qualifiedName = Warehouse.getQualifiedName(tableUsed.getDbName(), tableUsed.getTableName());
      // First we insert a new tree set to keep table modifications, unless it already exists
      ConcurrentSkipListSet<TableModificationKey> modificationsTree = new ConcurrentSkipListSet<TableModificationKey>();
      final ConcurrentSkipListSet<TableModificationKey> prevModificationsTree = tableModifications.putIfAbsent(
          qualifiedName, modificationsTree);
      if (prevModificationsTree != null) {
        modificationsTree = prevModificationsTree;
      }
      // We obtain the access time to the table when the materialized view was created.
      // This is a map from table fully qualified name to last modification before MV creation.
      BasicNotificationEvent e = materializedViewTable.getCreationSignature().get(qualifiedName);
      final TableModificationKey lastModificationBeforeCreation =
          new TableModificationKey(e.getEventId(), e.getEventTime());
      modificationsTree.add(lastModificationBeforeCreation);
      if (!create) {
        // If we are not creating the MV at this instant, but instead it was created previously
        // and we are loading it into the cache, we need to go through the event logs and
        // check if the MV is still valid.
        NotificationEvent event = store.getFirstNotificationEventForTableAfterEvent(
            tableUsed.getDbName(), tableUsed.getTableName(), lastModificationBeforeCreation.eventId);
        if (event != null) {
          // We do not need to do anything more for current table, as we detected
          // a modification event that was in the metastore.
          modificationsTree.add(new TableModificationKey(event.getEventId(), event.getEventTime()));
          continue;
        }
      }
    }
    cq.put(vk, materialization);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Cached materialized view for rewriting: " + Warehouse.getQualifiedName(
          materializedViewTable.getDbName(), materializedViewTable.getTableName()));
    }
  }

  /**
   * This method is called when a table is modified. That way we can keep a track of the
   * invalidation for the MVs that use that table.
   */
  public void notifyTableModification(String dbName, String tableName,
      long eventId, int newModificationTime) {
    ConcurrentSkipListSet<TableModificationKey> modificationsTree =
        new ConcurrentSkipListSet<TableModificationKey>();
    final ConcurrentSkipListSet<TableModificationKey> prevModificationsTree =
        tableModifications.putIfAbsent(tableName, modificationsTree);
    if (prevModificationsTree != null) {
      modificationsTree = prevModificationsTree;
    }
    modificationsTree.add(new TableModificationKey(eventId, newModificationTime));
  }

  /**
   * Removes the materialized view from the cache.
   *
   * @param materializedViewTable the materialized view to remove
   */
  public void dropMaterializedView(Table materializedViewTable) {
    // Bail out if it is not enabled for rewriting
    if (!materializedViewTable.isRewriteEnabled()) {
      return;
    }
    final MaterializationKey vk = new MaterializationKey(
        materializedViewTable.getTableName(), materializedViewTable.getCreateTime());
    materializations.get(materializedViewTable.getDbName()).remove(vk);
  }

  /**
   * Returns the materialized views in the cache for the given database.
   *
   * @param dbName the database
   * @return the collection of materialized views, or the empty collection if none
   */
  public Map<String, Materialization> getRewritingMaterializations(String dbName) {
    if (materializations.get(dbName) != null) {
      ImmutableMap.Builder<String, Materialization> m = ImmutableMap.builder();
      Collection<Entry<MaterializationKey,Materialization>> iterable =
          Collections.unmodifiableCollection(materializations.get(dbName).entrySet());
      for (Entry<MaterializationKey, Materialization> e : iterable) {
        String materializationName = e.getKey().materializationName;
        Materialization materialization = e.getValue();
        int invalidationTime = getInvalidationTime(materialization);
        // We need to check whether previous value is zero, as data modification
        // in another table used by the materialized view might have modified
        // the value too
        boolean modified = materialization.compareAndSetInvalidationTime(0, invalidationTime);
        while (!modified) {
          int currentInvalidationTime = materialization.getInvalidationTime();
          if (invalidationTime < currentInvalidationTime) {
            // It was set by other table modification, but it was after this table modification
            // hence we need to set it
            modified = materialization.compareAndSetInvalidationTime(currentInvalidationTime, invalidationTime);
          } else {
            // Nothing to do
            modified = true;
          }
        }
        m.put(materializationName, materialization);
      }
      return m.build();
    }
    return ImmutableMap.of();
  }

  private int getInvalidationTime(Materialization materialization) {
    int firstModificationTimeAfterCreation = 0;
    for (Table tableUsed : materialization.getTablesUsed()) {
      final String qualifiedName = Warehouse.getQualifiedName(tableUsed.getDbName(), tableUsed.getTableName());
      BasicNotificationEvent e = materialization.getMaterializationTable().getCreationSignature().get(qualifiedName);
      final TableModificationKey lastModificationBeforeCreation =
          new TableModificationKey(e.getEventId(), e.getEventTime());
      final TableModificationKey post = tableModifications.get(qualifiedName)
          .higher(lastModificationBeforeCreation);
      if (post != null) {
        if (firstModificationTimeAfterCreation == 0 ||
            post.eventTime < firstModificationTimeAfterCreation) {
          firstModificationTimeAfterCreation = post.eventTime;
        }
      }
    }
    return firstModificationTimeAfterCreation;
  }

  private static class MaterializationKey {
    private String materializationName;
    private int creationDate;

    private MaterializationKey(String materializationName, int creationTime) {
      this.materializationName = materializationName;
      this.creationDate = creationTime;
    }

    @Override
    public boolean equals(Object obj) {
      if(this == obj) {
        return true;
      }
      if((obj == null) || (obj.getClass() != this.getClass())) {
        return false;
      }
      MaterializationKey viewKey = (MaterializationKey) obj;
      return creationDate == viewKey.creationDate && Objects.equals(materializationName, viewKey.materializationName);
    }

    @Override
    public int hashCode() {
      int hash = 7;
      hash = 31 * hash + creationDate;
      hash = 31 * hash + materializationName.hashCode();
      return hash;
    }

    @Override
    public String toString() {
      return "MaterializationKey{" + materializationName + "," + creationDate + "}";
    }
  }

  private static class TableModificationKey implements Comparable<TableModificationKey> {
    private long eventId;
    private int eventTime;

    private TableModificationKey(Long eventId, int eventTime) {
      this.eventId = eventId;
      this.eventTime = eventTime;
    }

    @Override
    public boolean equals(Object obj) {
      if(this == obj) {
        return true;
      }
      if((obj == null) || (obj.getClass() != this.getClass())) {
        return false;
      }
      TableModificationKey tableModificationKey = (TableModificationKey) obj;
      return eventTime == tableModificationKey.eventTime &&
          ObjectUtils.equals(eventId, tableModificationKey.eventId);
    }

    @Override
    public int hashCode() {
      int hash = 7;
      hash = 31 * hash + Long.hashCode(eventId);
      hash = 31 * hash + Integer.hashCode(eventTime);
      return hash;
    }

    @Override
    public int compareTo(TableModificationKey other) {
      if (eventTime == other.eventTime) {
        return Long.compare(eventId, other.eventId);
      }
      return Integer.compare(eventTime, other.eventTime);
    }

    @Override
    public String toString() {
      return "TableModificationKey{" + eventId + "," + eventTime + "}";
    }
  }

}
