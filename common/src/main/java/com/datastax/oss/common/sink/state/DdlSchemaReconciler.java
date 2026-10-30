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
import com.datastax.oss.driver.api.core.servererrors.UnauthorizedException;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/** Creates missing tables from the record schema before handing out the mapper. */
public class DdlSchemaReconciler implements SchemaReconciler {
  private static final String KEY = "key";
  private static final String VALUE = "value";
  private static final String NOW_FUNCTION = "now()";

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
    Object keyId = identity(record.keySchema());
    Object valueId = identity(record.valueSchema());
    Resolved cached = resolved.get(tableConfig);
    if (cached != null && cached.matches(keyId, valueId)) {
      return cached.mapper;
    }
    synchronized (locks.computeIfAbsent(tableConfig, t -> new Object())) {
      cached = resolved.get(tableConfig);
      if (cached != null && cached.matches(keyId, valueId)) {
        return cached.mapper;
      }
      RecordMapper mapper = reconcile(tableConfig, record);
      resolved.put(tableConfig, new Resolved(keyId, valueId, mapper));
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
    Optional<TableMetadata> table = LifeCycleManager.findTableMetadata(session, tableConfig);
    if (!table.isPresent()) {
      createTable(tableConfig, record);
      table = LifeCycleManager.findTableMetadata(session, tableConfig);
      if (!table.isPresent()) {
        throw new IllegalStateException(
            String.format(
                "Table %s is not visible after creation", tableConfig.getKeyspaceAndTable()));
      }
    }
    return prepare(tableConfig, table.get(), tableConfig.getMapping());
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
    Optional<TableMetadata> table = LifeCycleManager.findTableMetadata(session, tableConfig);
    if (!table.isPresent()) {
      createImplicitTable(tableConfig, keySchema, valueSchema, mapping);
      table = LifeCycleManager.findTableMetadata(session, tableConfig);
      if (!table.isPresent()) {
        throw new IllegalStateException(
            String.format(
                "Table %s is not visible after creation", tableConfig.getKeyspaceAndTable()));
      }
    } else {
      rejectMissingColumns(tableConfig, table.get(), keySchema, valueSchema, mapping);
    }
    return prepare(tableConfig, table.get(), mapping);
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

  private static void rejectMissingColumns(
      TableConfig tableConfig,
      TableMetadata table,
      AbstractSchema keySchema,
      AbstractSchema valueSchema,
      Map<CqlIdentifier, CqlIdentifier> mapping) {
    String missingKeyColumns =
        mapping
            .entrySet()
            .stream()
            .filter(e -> !table.getColumn(e.getKey()).isPresent())
            .filter(e -> isKeySourced(e.getValue()))
            .map(e -> e.getKey().asCql(true))
            .collect(Collectors.joining(", "));
    if (!missingKeyColumns.isEmpty()) {
      throw new SchemaMismatchException(
          String.format(
              "The primary key of table %s does not match the record key fields; missing key columns: %s",
              tableConfig.getKeyspaceAndTable(), missingKeyColumns));
    }
    List<String> alters =
        mapping
            .entrySet()
            .stream()
            .filter(e -> !table.getColumn(e.getKey()).isPresent())
            .map(
                e ->
                    CqlDdl.addColumn(
                        tableConfig.getKeyspace(),
                        tableConfig.getTable(),
                        e.getKey(),
                        sourceType(keySchema, valueSchema, e.getValue().asInternal())))
            .collect(Collectors.toList());
    if (!alters.isEmpty()) {
      throw new SchemaMismatchException(
          String.format(
              "Table %s has no column for some record fields; add them with: %s",
              tableConfig.getKeyspaceAndTable(), String.join("; ", alters)));
    }
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
      if (source.startsWith(KEY + '.')) {
        partitionKey.put(column, fieldType(record.keySchema(), KEY, source));
      } else if (source.startsWith(VALUE + '.')) {
        columns.put(column, fieldType(record.valueSchema(), VALUE, source));
      } else if (source.equals(NOW_FUNCTION)) {
        columns.put(column, DataTypes.TIMEUUID);
      } else {
        throw new SchemaMismatchException(
            String.format(
                "Cannot derive a column type for %s to create table %s; create the table manually",
                source, tableConfig.getKeyspaceAndTable()));
      }
    }
    if (partitionKey.isEmpty()) {
      throw new SchemaMismatchException(
          String.format(
              "Cannot create table %s: no mapped column comes from the record key to form the primary key",
              tableConfig.getKeyspaceAndTable()));
    }
    createTable(tableConfig, partitionKey, columns);
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
      throw new FatalSchemaException(
          String.format(
              "Not authorized to run %s on %s: %s",
              kind, tableConfig.getKeyspaceAndTable(), e.getMessage()),
          e);
    }
    session.checkSchemaAgreement();
    session.refreshSchema();
  }

  private static DataType fieldType(AbstractSchema schema, String part, String source) {
    if (schema == null) {
      throw new SchemaMismatchException(
          String.format(
              "Creating a table needs a structured schema on the record %s (mapped from %s)",
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

  private static Object identity(AbstractSchema schema) {
    return schema == null ? null : schema.schemaIdentity();
  }

  private static final class Resolved {
    private final Object keyId;
    private final Object valueId;
    private final RecordMapper mapper;

    private Resolved(Object keyId, Object valueId, RecordMapper mapper) {
      this.keyId = keyId;
      this.valueId = valueId;
      this.mapper = mapper;
    }

    private boolean matches(Object keyId, Object valueId) {
      return this.keyId == keyId && this.valueId == valueId;
    }
  }
}
