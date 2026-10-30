/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.common.sink.state;

import com.datastax.oss.common.sink.AbstractField;
import com.datastax.oss.common.sink.AbstractSchema;
import com.datastax.oss.common.sink.AbstractSinkRecord;
import com.datastax.oss.common.sink.RecordMapper;
import com.datastax.oss.common.sink.config.CassandraSinkConfig;
import com.datastax.oss.common.sink.config.TableConfig;
import com.datastax.oss.common.sink.record.RawData;
import com.datastax.oss.common.sink.schema.CqlDdl;
import com.datastax.oss.common.sink.schema.CqlTypeMapper;
import com.datastax.oss.common.sink.schema.FatalSchemaException;
import com.datastax.oss.common.sink.schema.SchemaMismatchException;
import com.datastax.oss.common.sink.util.SinkUtil;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.UnauthorizedException;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates missing tables and adds missing columns from the record schema. */
public class DdlSchemaReconciler implements SchemaReconciler {
  private static final Logger log = LoggerFactory.getLogger(DdlSchemaReconciler.class);
  private static final String KEY = "key";
  private static final String VALUE = "value";
  private static final String NOW_FUNCTION = "now()";
  private static final int MAX_CACHED_VERSIONS = 8;

  private final CqlSession session;
  private final CassandraSinkConfig config;
  private final InstanceState state;
  private final ConcurrentMap<TableConfig, Resolved> resolved = new ConcurrentHashMap<>();
  private final ConcurrentMap<TableConfig, Object> locks = new ConcurrentHashMap<>();

  public DdlSchemaReconciler(CqlSession session, CassandraSinkConfig config, InstanceState state) {
    this.session = session;
    this.config = config;
    this.state = state;
  }

  @Override
  public RecordMapper mapperFor(TableConfig tableConfig, AbstractSinkRecord record) {
    if (record.value() == null) {
      // A tombstone carries no value schema; deletes only need the prepared mapper.
      RecordMapper existing = state.getRecordMapper(tableConfig);
      if (existing == null) {
        throw new SchemaMismatchException(
            String.format(
                "Tombstone: cannot apply a delete to %s before its first non-null record",
                tableConfig.getKeyspaceAndTable()));
      }
      return existing;
    }
    Object keyId = record.keySchemaIdentity();
    Object valueId = record.valueSchemaIdentity();
    RecordMapper mapper = Resolved.find(resolved.get(tableConfig), keyId, valueId);
    if (mapper != null) {
      return mapper;
    }
    synchronized (locks.computeIfAbsent(tableConfig, t -> new Object())) {
      Resolved cached = resolved.get(tableConfig);
      mapper = Resolved.find(cached, keyId, valueId);
      if (mapper == null) {
        mapper = reconcile(tableConfig, record);
        resolved.put(tableConfig, Resolved.append(cached, keyId, valueId, mapper));
      }
      return mapper;
    }
  }

  private RecordMapper reconcile(TableConfig tableConfig, AbstractSinkRecord record) {
    if (tableConfig.isImplicit()) {
      return reconcileImplicit(tableConfig, record);
    }
    RecordMapper existing = state.getRecordMapper(tableConfig);
    if (existing != null) {
      return existing;
    }
    Optional<TableMetadata> found = LifeCycleManager.findTableMetadata(session, tableConfig);
    if (!found.isPresent()) {
      createTable(tableConfig, record);
    }
    // Runs after CREATE too: IF NOT EXISTS may have kept a table with fewer columns.
    TableMetadata table =
        addMissingColumns(
            tableConfig,
            found.isPresent() ? found.get() : createdTable(tableConfig),
            tableConfig.getMapping(),
            source -> explicitColumnType(tableConfig, record, source));
    return prepare(tableConfig, table, tableConfig.getMapping());
  }

  private RecordMapper reconcileImplicit(TableConfig tableConfig, AbstractSinkRecord record) {
    AbstractSchema keySchema = record.keySchema();
    AbstractSchema valueSchema = record.valueSchema();
    if (valueSchema == null || valueSchema.type() != AbstractSchema.Type.STRUCT) {
      throw new SchemaMismatchException(
          String.format(
              "Implicit mapping to %s needs a struct record value schema",
              tableConfig.getKeyspaceAndTable()));
    }
    Map<CqlIdentifier, CqlIdentifier> mapping = ImplicitMapping.forSchemas(keySchema, valueSchema);
    Optional<TableMetadata> found = LifeCycleManager.findTableMetadata(session, tableConfig);
    if (!found.isPresent()) {
      createImplicitTable(tableConfig, keySchema, valueSchema, mapping);
    }
    TableMetadata existing = found.isPresent() ? found.get() : createdTable(tableConfig);
    mapping = ImplicitMapping.resolveColumns(mapping, existing, tableConfig.getKeyspaceAndTable());
    TableMetadata table =
        addMissingColumns(
            tableConfig, existing, mapping, source -> sourceType(keySchema, valueSchema, source));
    return prepare(tableConfig, table, mapping);
  }

  private TableMetadata createdTable(TableConfig tableConfig) {
    return LifeCycleManager.findTableMetadata(session, tableConfig)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format(
                        "Table %s is not visible after creation",
                        tableConfig.getKeyspaceAndTable())));
  }

  private RecordMapper prepare(
      TableConfig tableConfig, TableMetadata table, Map<CqlIdentifier, CqlIdentifier> mapping) {
    List<CqlIdentifier> primaryKey =
        table.getPrimaryKey().stream().map(ColumnMetadata::getName).collect(Collectors.toList());
    try {
      LifeCycleManager.prepareStatementsAsync(
              session,
              state.getTopicState(tableConfig.getTopicName()),
              tableConfig,
              table,
              primaryKey,
              mapping,
              true)
          .toCompletableFuture()
          .join();
    } catch (CompletionException e) {
      throw e.getCause() instanceof RuntimeException ? (RuntimeException) e.getCause() : e;
    }
    return state.getRecordMapper(tableConfig);
  }

  private void createImplicitTable(
      TableConfig tableConfig,
      AbstractSchema keySchema,
      AbstractSchema valueSchema,
      Map<CqlIdentifier, CqlIdentifier> mapping) {
    if (keySchema == null || keySchema.type() != AbstractSchema.Type.STRUCT) {
      throw new SchemaMismatchException(
          String.format(
              "Cannot create table %s: implicit table creation needs a struct record key",
              tableConfig.getKeyspaceAndTable()));
    }
    LinkedHashMap<CqlIdentifier, DataType> partitionKey = new LinkedHashMap<>();
    LinkedHashMap<CqlIdentifier, DataType> columns = new LinkedHashMap<>();
    for (Map.Entry<CqlIdentifier, CqlIdentifier> entry : mapping.entrySet()) {
      String source = entry.getValue().asInternal();
      DataType type = sourceType(keySchema, valueSchema, source);
      if (source.startsWith(KEY + '.')) {
        partitionKey.put(entry.getKey(), type);
      } else {
        columns.put(entry.getKey(), type);
      }
    }
    if (partitionKey.isEmpty()) {
      throw new SchemaMismatchException(
          String.format(
              "Cannot create table %s: the record key has no fields to form the primary key",
              tableConfig.getKeyspaceAndTable()));
    }
    createTable(tableConfig, partitionKey, columns);
  }

  /** Adds missing mapped value columns when evolveTable is on, else fails; returns the table. */
  private TableMetadata addMissingColumns(
      TableConfig tableConfig,
      TableMetadata table,
      Map<CqlIdentifier, CqlIdentifier> mapping,
      Function<String, DataType> typeOf) {
    List<Map.Entry<CqlIdentifier, CqlIdentifier>> missing = missingColumns(table, mapping);
    if (missing.isEmpty()) {
      return table;
    }
    String missingKeyColumns =
        missing
            .stream()
            .filter(e -> isKeySourced(e.getValue()))
            .map(e -> e.getKey().asCql(true))
            .collect(Collectors.joining(", "));
    if (!missingKeyColumns.isEmpty()) {
      throw new SchemaMismatchException(
          String.format(
              "The primary key of table %s does not match the record key fields; missing key columns: %s",
              tableConfig.getKeyspaceAndTable(), missingKeyColumns));
    }
    LinkedHashMap<CqlIdentifier, DataType> columns = new LinkedHashMap<>();
    missing.forEach(e -> columns.put(e.getKey(), typeOf.apply(e.getValue().asInternal())));
    if (!config.isEvolveTable()) {
      String alters =
          columns
              .entrySet()
              .stream()
              .map(e -> addColumnCql(tableConfig, e.getKey(), e.getValue()))
              .collect(Collectors.joining("; "));
      throw new SchemaMismatchException(
          String.format(
              "Table %s has no column for some record fields; add them with: %s",
              tableConfig.getKeyspaceAndTable(), alters));
    }
    return alterTable(tableConfig, columns);
  }

  private TableMetadata alterTable(
      TableConfig tableConfig, LinkedHashMap<CqlIdentifier, DataType> columns) {
    for (Map.Entry<CqlIdentifier, DataType> column : columns.entrySet()) {
      String cql = addColumnCql(tableConfig, column.getKey(), column.getValue());
      try {
        session.execute(SimpleStatement.newInstance(cql));
      } catch (UnauthorizedException e) {
        throw notAuthorized("ALTER TABLE", tableConfig, e);
      } catch (InvalidQueryException e) {
        // Another task or worker may have added it first; existing columns are never retyped.
        session.refreshSchema();
        if (!visibleTable(tableConfig).getColumn(column.getKey()).isPresent()) {
          throw invalid("ALTER TABLE", tableConfig, cql, e);
        }
      }
    }
    if (!session.checkSchemaAgreement()) {
      log.warn(
          "Schema agreement not reached after altering table {}",
          tableConfig.getKeyspaceAndTable());
    }
    session.refreshSchema();
    return visibleTable(tableConfig);
  }

  private TableMetadata visibleTable(TableConfig tableConfig) {
    return LifeCycleManager.findTableMetadata(session, tableConfig)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    String.format(
                        "Table %s is no longer visible", tableConfig.getKeyspaceAndTable())));
  }

  private static String addColumnCql(TableConfig tableConfig, CqlIdentifier column, DataType type) {
    return CqlDdl.addColumn(tableConfig.getKeyspace(), tableConfig.getTable(), column, type);
  }

  /** Mapped columns missing from the table, ignoring the ttl and timestamp pseudo-columns. */
  static List<Map.Entry<CqlIdentifier, CqlIdentifier>> missingColumns(
      TableMetadata table, Map<CqlIdentifier, CqlIdentifier> mapping) {
    return mapping
        .entrySet()
        .stream()
        .filter(e -> !SinkUtil.isTtlMappingColumn(e.getKey()))
        .filter(e -> !SinkUtil.isTimestampMappingColumn(e.getKey()))
        .filter(e -> !table.getColumn(e.getKey()).isPresent())
        .collect(Collectors.toList());
  }

  /** True for sources whose column type can be derived on evolve: value fields and now(). */
  static boolean isEvolvableSource(CqlIdentifier source) {
    String name = source.asInternal();
    return name.startsWith(VALUE + '.') || name.equals(NOW_FUNCTION);
  }

  private static boolean isKeySourced(CqlIdentifier source) {
    String name = source.asInternal();
    return name.equals(KEY) || name.startsWith(KEY + '.');
  }

  private static DataType sourceType(
      AbstractSchema keySchema, AbstractSchema valueSchema, String source) {
    return source.startsWith(KEY + '.')
        ? fieldType(keySchema, KEY, source)
        : fieldType(valueSchema, VALUE, source);
  }

  private void createTable(TableConfig tableConfig, AbstractSinkRecord record) {
    LinkedHashMap<CqlIdentifier, DataType> partitionKey = new LinkedHashMap<>();
    LinkedHashMap<CqlIdentifier, DataType> columns = new LinkedHashMap<>();
    for (Map.Entry<CqlIdentifier, CqlIdentifier> entry : tableConfig.getMapping().entrySet()) {
      CqlIdentifier column = entry.getKey();
      if (SinkUtil.isTtlMappingColumn(column) || SinkUtil.isTimestampMappingColumn(column)) {
        continue;
      }
      String source = entry.getValue().asInternal();
      DataType type = explicitColumnType(tableConfig, record, source);
      if (source.startsWith(KEY + '.')) {
        partitionKey.put(column, type);
      } else {
        columns.put(column, type);
      }
    }
    if (partitionKey.isEmpty()) {
      throw new SchemaMismatchException(noKeyColumn(tableConfig));
    }
    createTable(tableConfig, partitionKey, columns);
  }

  /** Why an explicit mapping cannot create its table, or null if it can. */
  @Nullable
  static String createTableProblem(TableConfig tableConfig) {
    boolean hasKey = false;
    for (Map.Entry<CqlIdentifier, CqlIdentifier> entry : tableConfig.getMapping().entrySet()) {
      CqlIdentifier column = entry.getKey();
      if (SinkUtil.isTtlMappingColumn(column) || SinkUtil.isTimestampMappingColumn(column)) {
        continue;
      }
      String source = entry.getValue().asInternal();
      if (source.startsWith(KEY + '.')) {
        hasKey = true;
      } else if (!source.startsWith(VALUE + '.') && !source.equals(NOW_FUNCTION)) {
        return underivable(tableConfig, source);
      }
    }
    return hasKey ? null : noKeyColumn(tableConfig);
  }

  private static String noKeyColumn(TableConfig tableConfig) {
    return String.format(
        "Cannot create table %s: no mapped column comes from the record key to form the primary key",
        tableConfig.getKeyspaceAndTable());
  }

  private static String underivable(TableConfig tableConfig, String source) {
    return String.format(
        "Cannot derive a column type for %s in table %s; define the column manually",
        source, tableConfig.getKeyspaceAndTable());
  }

  private static DataType explicitColumnType(
      TableConfig tableConfig, AbstractSinkRecord record, String source) {
    if (source.startsWith(KEY + '.')) {
      return fieldType(record.keySchema(), KEY, source);
    }
    if (source.startsWith(VALUE + '.')) {
      return fieldType(record.valueSchema(), VALUE, source);
    }
    if (source.equals(NOW_FUNCTION)) {
      return DataTypes.TIMEUUID;
    }
    throw new SchemaMismatchException(underivable(tableConfig, source));
  }

  private void createTable(
      TableConfig tableConfig,
      LinkedHashMap<CqlIdentifier, DataType> partitionKey,
      LinkedHashMap<CqlIdentifier, DataType> columns) {
    String cql =
        CqlDdl.createTable(
            tableConfig.getKeyspace(), tableConfig.getTable(), partitionKey, columns);
    if (!config.isCreateTable()) {
      throw new SchemaMismatchException(
          String.format(
              "Table %s does not exist; create it with: %s",
              tableConfig.getKeyspaceAndTable(), cql));
    }
    execute(cql, "CREATE TABLE", tableConfig);
  }

  private void execute(String cql, String kind, TableConfig tableConfig) {
    try {
      session.execute(SimpleStatement.newInstance(cql));
    } catch (UnauthorizedException e) {
      throw notAuthorized(kind, tableConfig, e);
    } catch (InvalidQueryException e) {
      throw invalid(kind, tableConfig, cql, e);
    }
    if (!session.checkSchemaAgreement()) {
      log.warn(
          "Schema agreement not reached after creating table {}",
          tableConfig.getKeyspaceAndTable());
    }
    session.refreshSchema();
  }

  private static FatalSchemaException invalid(
      String kind, TableConfig tableConfig, String cql, InvalidQueryException e) {
    return new FatalSchemaException(
        String.format(
            "Failed to run %s on %s (%s): %s",
            kind, tableConfig.getKeyspaceAndTable(), cql, e.getMessage()),
        e);
  }

  private static FatalSchemaException notAuthorized(
      String kind, TableConfig tableConfig, UnauthorizedException e) {
    return new FatalSchemaException(
        String.format(
            "Not authorized to run %s on %s: %s",
            kind, tableConfig.getKeyspaceAndTable(), e.getMessage()),
        e);
  }

  private static DataType fieldType(AbstractSchema schema, String part, String source) {
    if (schema == null) {
      throw new SchemaMismatchException(
          String.format(
              "Deriving a column type needs a structured schema on the record %s (mapped from %s)",
              part, source));
    }
    String field = source.substring(part.length() + 1);
    if (field.equals(RawData.FIELD_NAME)) {
      return CqlTypeMapper.toCql(schema);
    }
    AbstractField fieldSchema =
        schema.type() == AbstractSchema.Type.STRUCT ? schema.field(field) : null;
    if (fieldSchema == null) {
      throw new SchemaMismatchException(
          String.format("Field %s is not defined in the record %s schema", field, part));
    }
    return CqlTypeMapper.toCql(fieldSchema.schema());
  }

  /** Immutable, insertion-ordered mappers of the most recent schema versions. */
  private static final class Resolved {
    private final Object[] keyIds;
    private final Object[] valueIds;
    private final RecordMapper[] mappers;

    private Resolved(Object[] keyIds, Object[] valueIds, RecordMapper[] mappers) {
      this.keyIds = keyIds;
      this.valueIds = valueIds;
      this.mappers = mappers;
    }

    private static RecordMapper find(Resolved resolved, Object keyId, Object valueId) {
      if (resolved != null) {
        for (int i = resolved.mappers.length - 1; i >= 0; i--) {
          if (resolved.keyIds[i] == keyId && resolved.valueIds[i] == valueId) {
            return resolved.mappers[i];
          }
        }
      }
      return null;
    }

    private static Resolved append(
        Resolved previous, Object keyId, Object valueId, RecordMapper mapper) {
      int kept = previous == null ? 0 : Math.min(previous.mappers.length, MAX_CACHED_VERSIONS - 1);
      Object[] keyIds = new Object[kept + 1];
      Object[] valueIds = new Object[kept + 1];
      RecordMapper[] mappers = new RecordMapper[kept + 1];
      if (kept > 0) {
        int from = previous.mappers.length - kept;
        System.arraycopy(previous.keyIds, from, keyIds, 0, kept);
        System.arraycopy(previous.valueIds, from, valueIds, 0, kept);
        System.arraycopy(previous.mappers, from, mappers, 0, kept);
      }
      keyIds[kept] = keyId;
      valueIds[kept] = valueId;
      mappers[kept] = mapper;
      return new Resolved(keyIds, valueIds, mappers);
    }
  }
}
