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
package org.apache.hadoop.hive.ql.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.calcite.adapter.druid.DruidQuery;
import org.apache.calcite.adapter.druid.DruidSchema;
import org.apache.calcite.adapter.druid.DruidTable;
import org.apache.calcite.adapter.druid.LocalInterval;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.BasicNotificationEvent;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.EventUtils;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.metastore.messaging.event.filters.AndFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.DatabaseAndTableFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.EventTimeBoundaryFilter;
import org.apache.hadoop.hive.metastore.messaging.event.filters.MessageFormatFilter;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTypeSystemImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveMaterialization;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveVolcanoPlanner;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.TypeConverter;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.apache.hadoop.hive.ql.parse.ColumnStatsList;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

/**
 * Registry for materialized views. The goal of this cache is to avoid parsing and creating
 * logical plans for the materialized views at query runtime. When a query arrives, we will
 * just need to consult this cache and extract the logical plans for the views (which had
 * already been parsed) from it.
 */
public final class HiveMaterializedViewsRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(HiveMaterializedViewsRegistry.class);

  /* Singleton */
  private static final HiveMaterializedViewsRegistry SINGLETON = new HiveMaterializedViewsRegistry();

  /* Key is the database name. Value a map from a unique identifier for the view comprising
   * the qualified name and the creation time, to the view object.
   * Since currently we cannot alter a materialized view, that should suffice to identify
   * whether the cached view is up to date or not.
   * Creation time is useful to ensure correctness in case multiple HS2 instances are used. */
  private final ConcurrentMap<String, ConcurrentMap<ViewKey, RelOptHiveMaterialization>> materializedViews =
      new ConcurrentHashMap<String, ConcurrentMap<ViewKey, RelOptHiveMaterialization>>();

  /*
   * 
   */
  private final ConcurrentMap<String, ConcurrentSkipListSet<TableModificationKey>> tableModifications =
      new ConcurrentHashMap<String, ConcurrentSkipListSet<TableModificationKey>>();

  private final ExecutorService pool = Executors.newCachedThreadPool();

  private HiveMaterializedViewsRegistry() {
  }

  /**
   * Get instance of HiveMaterializedViewsRegistry.
   *
   * @return the singleton
   */
  public static HiveMaterializedViewsRegistry get() {
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
  public void init(final Hive db) {
    pool.submit(new Loader(db));
  }

  private class Loader implements Runnable {
    private final Hive db;

    private Loader(Hive db) {
      this.db = db;
    }

    @Override
    public void run() {
      try {
        List<Table> materializedViews = new ArrayList<Table>();
        for (String dbName : db.getAllDatabases()) {
          materializedViews.addAll(db.getAllMaterializedViewObjects(dbName));
        }
        for (Table mv : materializedViews) {
          addMaterializedView(mv, false);
        }
      } catch (HiveException e) {
        LOG.error("Problem connecting to the metastore when initializing the view registry");
      }
    }
  }

  /**
   * Adds a newly created materialized view to the cache.
   *
   * @param materializedViewTable the materialized view
   */
  public RelOptMaterialization createMaterializedView(Table materializedViewTable) {
    return addMaterializedView(materializedViewTable, true);
  }

  /**
   * Adds the materialized view to the cache.
   *
   * @param materializedViewTable the materialized view
   */
  private RelOptMaterialization addMaterializedView(Table materializedViewTable, boolean create) {
    // Bail out if it is not enabled for rewriting
    if (!materializedViewTable.isRewriteEnabled()) {
      return null;
    }
    // We are going to create the map for each view in the given database
    ConcurrentMap<ViewKey, RelOptHiveMaterialization> cq =
        new ConcurrentHashMap<ViewKey, RelOptHiveMaterialization>();
    final ConcurrentMap<ViewKey, RelOptHiveMaterialization> prevCq = materializedViews.putIfAbsent(
        materializedViewTable.getDbName(), cq);
    if (prevCq != null) {
      cq = prevCq;
    }
    // Bail out if it already exists
    final ViewKey vk = new ViewKey(
        materializedViewTable.getTableName(), materializedViewTable.getCreateTime());
    if (cq.containsKey(vk)) {
      return null;
    }
    // Start the process to add MV to the cache
    // First we parse the view query and create the materialization object
    final String viewQuery = materializedViewTable.getViewExpandedText();
    final RelNode tableRel = createTableScan(materializedViewTable);
    if (tableRel == null) {
      LOG.warn("Materialized view " + materializedViewTable.getCompleteName() +
              " ignored; error creating view replacement");
      return null;
    }
    final RelNode queryRel = parseQuery(viewQuery);
    if (queryRel == null) {
      LOG.warn("Materialized view " + materializedViewTable.getCompleteName() +
              " ignored; error parsing original query");
      return null;
    }
    RelOptHiveMaterialization materialization = new RelOptHiveMaterialization(tableRel, queryRel,
        null, tableRel.getTable().getQualifiedName());
    // Before loading the materialization in the cache, we need to update some
    // important information in the registry to account for rewriting invalidation
    List<RelOptTable> tablesUsed = RelOptUtil.findAllTables(queryRel);
    for (RelOptTable tableUsed : tablesUsed) {
      RelOptHiveTable table = (RelOptHiveTable) tableUsed;
      // First we insert a new tree set to keep table modifications, unless it already exists
      ConcurrentSkipListSet<TableModificationKey> modificationsTree = new ConcurrentSkipListSet<TableModificationKey>();
      final ConcurrentSkipListSet<TableModificationKey> prevModificationsTree = tableModifications.putIfAbsent(
          table.getName(), modificationsTree);
      if (prevModificationsTree != null) {
        modificationsTree = prevModificationsTree;
      }
      // We obtain the access time to the table when the materialized view was created.
      // This is a map from table fully qualified name to last modification before MV creation.
      BasicNotificationEvent e = materializedViewTable.getTTable().getCreationSignature().get(table.getName());
      final TableModificationKey lastModificationBeforeCreation =
          new TableModificationKey(e.getEventId(), e.getEventTime());
      modificationsTree.add(lastModificationBeforeCreation);
      if (!create) {
        // If we are not creating the MV at this instant, but instead it was created previously
        // and we are loading it into the cache, we need to go through the event logs and
        // check if the MV is still valid.
        try {
          IMetaStoreClient.NotificationFilter eventsFilter = new AndFilter(
              new DatabaseAndTableFilter(table.getHiveTableMD().getDbName(), table.getHiveTableMD().getTableName()),
              new EventTimeBoundaryFilter(lastModificationBeforeCreation.eventTime, Integer.MAX_VALUE),
              new MessageFormatFilter(MessageFactory.getInstance().getMessageFormat()));
          EventUtils.MSClientNotificationFetcher eventsFetcher
              = new EventUtils.MSClientNotificationFetcher(Hive.get().getMSC());
          EventUtils.NotificationEventIterator eventsIter = new EventUtils.NotificationEventIterator(
              eventsFetcher, 0, -1, eventsFilter);
          if (eventsIter.hasNext()) {
            NotificationEvent event = eventsIter.next();
            int invalidationTime = event.getEventTime();
            if (invalidationTime != 0) {
              // We do not need to do anything more for current table, as we detected
              // a modification event that was in the metastore.
              modificationsTree.add(new TableModificationKey(event.getEventId(), invalidationTime));
              continue;
            }
          }
        } catch (Exception ex) {
          LOG.error("Problem connecting to the metastore when retrieving events");
          // TODO: Invalidate all cache?
        }
      }
    }
    cq.put(vk, materialization);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Cached materialized view for rewriting: " + tableRel.getTable().getQualifiedName());
    }
    return materialization;
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
    final ViewKey vk = new ViewKey(
        materializedViewTable.getTableName(), materializedViewTable.getCreateTime());
    materializedViews.get(materializedViewTable.getDbName()).remove(vk);
  }

  /**
   * Returns the materialized views in the cache for the given database.
   *
   * @param dbName the database
   * @return the collection of materialized views, or the empty collection if none
   */
  List<RelOptHiveMaterialization> getRewritingMaterializedViews(String dbName) {
    if (materializedViews.get(dbName) != null) {
      ImmutableList.Builder<RelOptHiveMaterialization> l = ImmutableList.builder();
      Collection<RelOptHiveMaterialization> iterable =
          Collections.unmodifiableCollection(materializedViews.get(dbName).values());
      for (RelOptHiveMaterialization materialization : iterable) {
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
      }
      return l.build();
    }
    return ImmutableList.of();
  }

  private int getInvalidationTime(RelOptMaterialization materialization) {
    List<RelOptTable> tablesUsed = RelOptUtil.findAllTables(materialization.queryRel);
    int firstModificationTimeAfterCreation = 0;
    for (RelOptTable tableUsed : tablesUsed) {
      RelOptHiveTable table = (RelOptHiveTable) tableUsed;
      Table materializedViewTable = (Table) materialization.tableRel.getTable();
      BasicNotificationEvent e = materializedViewTable.getTTable().getCreationSignature().get(table.getName());
      final TableModificationKey lastModificationBeforeCreation =
          new TableModificationKey(e.getEventId(), e.getEventTime());
      final TableModificationKey post = tableModifications.get(table.getName())
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

//  TODO: Serialization code!
//  Kryo kryo = SerializationUtilities.borrowKryo();
//  try {
//    setPlanPath(conf, hiveScratchDir);
//
//    Path planPath = getPlanPath(conf, name);
//    setHasWork(conf, name);
//
//    OutputStream out = null;
//
//    final long serializedSize;
//    final String planMode;
//    if (HiveConf.getBoolVar(conf, ConfVars.HIVE_RPC_QUERY_PLAN)) {
//      // add it to the conf
//      ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
//      try {
//        out = new DeflaterOutputStream(byteOut, new Deflater(Deflater.BEST_SPEED));
//        SerializationUtilities.serializePlan(kryo, w, out);
//        out.close();
//        out = null;
//      } finally {
//        IOUtils.closeStream(out);
//      }
//      final String serializedPlan = Base64.encodeBase64String(byteOut.toByteArray());
//      serializedSize = serializedPlan.length();
//      planMode = "RPC";
//      conf.set(planPath.toUri().getPath(), serializedPlan);
//    } else {

  private static RelNode createTableScan(Table viewTable) {
    // 0. Recreate cluster
    final RelOptPlanner planner = HiveVolcanoPlanner.createPlanner(null);
    final RexBuilder rexBuilder = new RexBuilder(
            new JavaTypeFactoryImpl(
                    new HiveTypeSystemImpl()));
    final RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

    // 1. Create column schema
    final RowResolver rr = new RowResolver();
    // 1.1 Add Column info for non partion cols (Object Inspector fields)
    StructObjectInspector rowObjectInspector;
    try {
      rowObjectInspector = (StructObjectInspector) viewTable.getDeserializer()
          .getObjectInspector();
    } catch (SerDeException e) {
      // Bail out
      return null;
    }
    List<? extends StructField> fields = rowObjectInspector.getAllStructFieldRefs();
    ColumnInfo colInfo;
    String colName;
    ArrayList<ColumnInfo> cInfoLst = new ArrayList<ColumnInfo>();
    for (int i = 0; i < fields.size(); i++) {
      colName = fields.get(i).getFieldName();
      colInfo = new ColumnInfo(
          fields.get(i).getFieldName(),
          TypeInfoUtils.getTypeInfoFromObjectInspector(fields.get(i).getFieldObjectInspector()),
          null, false);
      rr.put(null, colName, colInfo);
      cInfoLst.add(colInfo);
    }
    ArrayList<ColumnInfo> nonPartitionColumns = new ArrayList<ColumnInfo>(cInfoLst);

    // 1.2 Add column info corresponding to partition columns
    ArrayList<ColumnInfo> partitionColumns = new ArrayList<ColumnInfo>();
    for (FieldSchema part_col : viewTable.getPartCols()) {
      colName = part_col.getName();
      colInfo = new ColumnInfo(colName,
          TypeInfoFactory.getPrimitiveTypeInfo(part_col.getType()), null, true);
      rr.put(null, colName, colInfo);
      cInfoLst.add(colInfo);
      partitionColumns.add(colInfo);
    }

    // 1.3 Build row type from field <type, name>
    RelDataType rowType;
    try {
      rowType = TypeConverter.getType(cluster, rr, null);
    } catch (CalciteSemanticException e) {
      // Bail out
      return null;
    }

    // 2. Build RelOptAbstractTable
    String fullyQualifiedTabName = viewTable.getDbName();
    if (fullyQualifiedTabName != null && !fullyQualifiedTabName.isEmpty()) {
      fullyQualifiedTabName = fullyQualifiedTabName + "." + viewTable.getTableName();
    }
    else {
      fullyQualifiedTabName = viewTable.getTableName();
    }
    RelOptHiveTable optTable = new RelOptHiveTable(null, fullyQualifiedTabName,
        rowType, viewTable, nonPartitionColumns, partitionColumns, new ArrayList<VirtualColumn>(),
        SessionState.get().getConf(), new HashMap<String, PrunedPartitionList>(),
        new HashMap<String, ColumnStatsList>(), new AtomicInteger());
    RelNode tableRel;

    // 3. Build operator
    if (obtainTableType(viewTable) == TableType.DRUID) {
      // Build Druid query
      String address = HiveConf.getVar(SessionState.get().getConf(),
          HiveConf.ConfVars.HIVE_DRUID_BROKER_DEFAULT_ADDRESS);
      String dataSource = viewTable.getParameters().get(Constants.DRUID_DATA_SOURCE);
      Set<String> metrics = new HashSet<>();
      List<RelDataType> druidColTypes = new ArrayList<>();
      List<String> druidColNames = new ArrayList<>();
      for (RelDataTypeField field : rowType.getFieldList()) {
        druidColTypes.add(field.getType());
        druidColNames.add(field.getName());
        if (field.getName().equals(DruidTable.DEFAULT_TIMESTAMP_COLUMN)) {
          // timestamp
          continue;
        }
        if (field.getType().getSqlTypeName() == SqlTypeName.VARCHAR) {
          // dimension
          continue;
        }
        metrics.add(field.getName());
      }
      // TODO: Default interval will be an Interval once Calcite 1.15.0 is released.
      // We will need to update the type of this list.
      List<LocalInterval> intervals = Arrays.asList(DruidTable.DEFAULT_INTERVAL);

      DruidTable druidTable = new DruidTable(new DruidSchema(address, address, false),
          dataSource, RelDataTypeImpl.proto(rowType), metrics, DruidTable.DEFAULT_TIMESTAMP_COLUMN,
          intervals, null, null);
      final TableScan scan = new HiveTableScan(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
          optTable, viewTable.getTableName(), null, false, false);
      tableRel = DruidQuery.create(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
          optTable, druidTable, ImmutableList.<RelNode>of(scan));
    } else {
      // Build Hive Table Scan Rel
      tableRel = new HiveTableScan(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION), optTable,
          viewTable.getTableName(), null, false, false);
    }
    return tableRel;
  }

  private static RelNode parseQuery(String viewQuery) {
    try {
      final ASTNode node = ParseUtils.parse(viewQuery);
      final QueryState qs =
          new QueryState.Builder().withHiveConf(SessionState.get().getConf()).build();
      CalcitePlanner analyzer = new CalcitePlanner(qs);
      analyzer.initCtx(new Context(SessionState.get().getConf()));
      analyzer.init(false);
      return analyzer.genLogicalPlan(node);
    } catch (Exception e) {
      // We could not parse the view
      LOG.error(e.getMessage());
      return null;
    }
  }

  private static class ViewKey {
    private String viewName;
    private int creationDate;

    private ViewKey(String viewName, int creationTime) {
      this.viewName = viewName;
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
      ViewKey viewKey = (ViewKey) obj;
      return creationDate == viewKey.creationDate && Objects.equals(viewName, viewKey.viewName);
    }

    @Override
    public int hashCode() {
      int hash = 7;
      hash = 31 * hash + creationDate;
      hash = 31 * hash + viewName.hashCode();
      return hash;
    }

    @Override
    public String toString() {
      return "ViewKey{" + viewName + "," + creationDate + "}";
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

  private static TableType obtainTableType(Table tabMetaData) {
    if (tabMetaData.getStorageHandler() != null &&
            tabMetaData.getStorageHandler().toString().equals(
                    Constants.DRUID_HIVE_STORAGE_HANDLER_ID)) {
      return TableType.DRUID;
    }
    return TableType.NATIVE;
  }

  private enum TableType {
    DRUID,
    NATIVE
  }
}
