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

import static com.datastax.oss.common.sink.config.TableConfig.MAPPING_OPT;
import static com.datastax.oss.common.sink.schema.FakeSchema.prim;
import static com.datastax.oss.common.sink.schema.FakeSchema.struct;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.common.sink.AbstractSchema;
import com.datastax.oss.common.sink.AbstractSinkRecord;
import com.datastax.oss.common.sink.RecordMapper;
import com.datastax.oss.common.sink.config.CassandraSinkConfig;
import com.datastax.oss.common.sink.config.TableConfig;
import com.datastax.oss.common.sink.config.TableConfigBuilder;
import com.datastax.oss.common.sink.schema.FatalSchemaException;
import com.datastax.oss.common.sink.schema.SchemaMismatchException;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.servererrors.UnauthorizedException;
import com.datastax.oss.driver.api.core.type.DataTypes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

class DdlSchemaReconcilerTest {
  private static final String TOPIC = "orders-topic";
  private static final CqlIdentifier ORDERS = CqlIdentifier.fromInternal("orders");
  private static final String TIMESTAMP = "org.apache.kafka.connect.data.Timestamp";

  private final CqlSession session = mock(CqlSession.class);
  private final CassandraSinkConfig config = mock(CassandraSinkConfig.class);
  private final KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
  private final TableMetadata table = mock(TableMetadata.class);
  private final AbstractSchema keySchema = struct("id", prim(AbstractSchema.Type.INT64, null));
  private final AbstractSchema valueSchema =
      struct(
          "name",
          prim(AbstractSchema.Type.STRING, null),
          "ts",
          prim(AbstractSchema.Type.INT64, TIMESTAMP));
  private TopicState topicState;
  private InstanceState state;
  private ExecutorService executor;

  @BeforeEach
  void setUp() {
    Metadata metadata = mock(Metadata.class);
    when(session.getMetadata()).thenReturn(metadata);
    when(metadata.getKeyspace(CqlIdentifier.fromInternal("shop")))
        .thenReturn(Optional.of(keyspace));
    when(keyspace.getTable(ORDERS)).thenReturn(Optional.empty(), Optional.of(table));
    when(session.prepareAsync(anyString()))
        .thenAnswer(i -> CompletableFuture.completedFuture(mock(PreparedStatement.class)));
    when(config.isCreateTable()).thenReturn(true);

    Map<CqlIdentifier, ColumnMetadata> columns = new LinkedHashMap<>();
    for (String name : new String[] {"id", "name", "ts"}) {
      ColumnMetadata col = mock(ColumnMetadata.class);
      CqlIdentifier ident = CqlIdentifier.fromInternal(name);
      when(col.getName()).thenReturn(ident);
      when(col.getType()).thenReturn(DataTypes.TEXT);
      when(table.getColumn(ident)).thenReturn(Optional.of(col));
      columns.put(ident, col);
    }
    when(table.getColumns()).thenReturn(columns);
    when(table.getPrimaryKey())
        .thenReturn(Collections.singletonList(columns.get(CqlIdentifier.fromInternal("id"))));

    topicState = new TopicState(null);
    Map<String, TopicState> topicStates = new HashMap<>();
    topicStates.put(TOPIC, topicState);
    state = new InstanceState(mock(CassandraSinkConfig.class), session, topicStates, registry());
  }

  @AfterEach
  void tearDown() {
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  private static MetricRegistry registry() {
    return new MetricRegistry();
  }

  private TableConfig pending(String mapping) {
    TableConfig tableConfig =
        new TableConfigBuilder(TOPIC, "shop", "orders", false)
            .addSimpleSetting(MAPPING_OPT, mapping)
            .build();
    topicState.addPendingTable(tableConfig);
    return tableConfig;
  }

  private AbstractSinkRecord record(AbstractSchema key, AbstractSchema value) {
    AbstractSinkRecord record = mock(AbstractSinkRecord.class);
    when(record.topic()).thenReturn(TOPIC);
    when(record.keySchema()).thenReturn(key);
    when(record.valueSchema()).thenReturn(value);
    return record;
  }

  private DdlSchemaReconciler reconciler() {
    return new DdlSchemaReconciler(session, config, state);
  }

  @Test
  void should_create_table_on_first_record_with_key_columns_as_partition_key() {
    TableConfig tableConfig = pending("id=key.id, name=value.name, ts=value.ts");

    RecordMapper mapper = reconciler().mapperFor(tableConfig, record(keySchema, valueSchema));

    ArgumentCaptor<SimpleStatement> stmt = ArgumentCaptor.forClass(SimpleStatement.class);
    InOrder order = inOrder(session);
    order.verify(session).execute(stmt.capture());
    order.verify(session).checkSchemaAgreement();
    order.verify(session).refreshSchema();
    order.verify(session, atLeastOnce()).prepareAsync(anyString());
    assertThat(stmt.getValue().getQuery())
        .isEqualTo(
            "CREATE TABLE IF NOT EXISTS shop.orders (id bigint,name text,ts timestamp,PRIMARY KEY(id))");
    assertThat(mapper).isNotNull().isSameAs(state.getRecordMapper(tableConfig));
  }

  @Test
  void should_not_issue_ddl_on_second_record_with_same_schema_identity() {
    TableConfig tableConfig = pending("id=key.id, name=value.name, ts=value.ts");
    DdlSchemaReconciler reconciler = reconciler();

    RecordMapper first = reconciler.mapperFor(tableConfig, record(keySchema, valueSchema));
    RecordMapper second = reconciler.mapperFor(tableConfig, record(keySchema, valueSchema));

    verify(session, times(1)).execute(any(SimpleStatement.class));
    assertThat(second).isSameAs(first);
  }

  @Test
  void should_issue_ddl_once_under_concurrent_first_records() throws Exception {
    TableConfig tableConfig = pending("id=key.id, name=value.name, ts=value.ts");
    DdlSchemaReconciler reconciler = reconciler();
    executor = Executors.newFixedThreadPool(8);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<RecordMapper>> results = new ArrayList<>();
    for (int i = 0; i < 16; i++) {
      AbstractSinkRecord record = record(keySchema, valueSchema);
      results.add(
          executor.submit(
              () -> {
                start.await();
                return reconciler.mapperFor(tableConfig, record);
              }));
    }
    start.countDown();

    RecordMapper first = results.get(0).get(10, TimeUnit.SECONDS);
    for (Future<RecordMapper> result : results) {
      assertThat(result.get(10, TimeUnit.SECONDS)).isNotNull().isSameAs(first);
    }
    verify(session, times(1)).execute(any(SimpleStatement.class));
  }

  @Test
  void should_return_existing_mapper_without_ddl() {
    TableConfig tableConfig =
        new TableConfigBuilder(TOPIC, "shop", "orders", false)
            .addSimpleSetting(MAPPING_OPT, "id=key.id, name=value.name")
            .build();
    topicState.createRecordMapper(tableConfig, Collections.emptyList(), null, null);
    RecordMapper existing = state.getRecordMapper(tableConfig);

    assertThat(reconciler().mapperFor(tableConfig, record(keySchema, valueSchema)))
        .isSameAs(existing);
    verify(session, never()).execute(any(Statement.class));
  }

  @Test
  void should_reject_table_without_key_sourced_column() {
    TableConfig tableConfig = pending("name=value.name, ts=value.ts");

    assertThatThrownBy(() -> reconciler().mapperFor(tableConfig, record(keySchema, valueSchema)))
        .isInstanceOf(SchemaMismatchException.class)
        .hasMessageContaining("primary key");
    verify(session, never()).execute(any(Statement.class));
  }

  @Test
  void should_reject_schemaless_record_when_create_table_on() {
    TableConfig tableConfig = pending("id=key.id, name=value.name, ts=value.ts");

    assertThatThrownBy(() -> reconciler().mapperFor(tableConfig, record(keySchema, null)))
        .isInstanceOf(SchemaMismatchException.class)
        .hasMessageContaining("structured schema");
    verify(session, never()).execute(any(Statement.class));
  }

  @Test
  void should_reject_header_sourced_column() {
    TableConfig tableConfig = pending("id=key.id, h=header.h");

    assertThatThrownBy(() -> reconciler().mapperFor(tableConfig, record(keySchema, valueSchema)))
        .isInstanceOf(SchemaMismatchException.class)
        .hasMessageContaining("header.h");
  }

  @Test
  void should_map_now_function_to_timeuuid_and_skip_pseudo_columns() {
    TableConfig tableConfig =
        pending("id=key.id, name=value.name, ts=value.ts, uid=now(), __ttl=value.ts");
    when(table.getColumn(CqlIdentifier.fromInternal("uid")))
        .thenReturn(Optional.of(mock(ColumnMetadata.class)));

    reconciler().mapperFor(tableConfig, record(keySchema, valueSchema));

    ArgumentCaptor<SimpleStatement> stmt = ArgumentCaptor.forClass(SimpleStatement.class);
    verify(session).execute(stmt.capture());
    assertThat(stmt.getValue().getQuery())
        .isEqualTo(
            "CREATE TABLE IF NOT EXISTS shop.orders (id bigint,name text,ts timestamp,uid timeuuid,PRIMARY KEY(id))");
  }

  @Test
  void should_report_create_table_cql_when_create_table_off() {
    when(config.isCreateTable()).thenReturn(false);
    TableConfig tableConfig = pending("id=key.id, name=value.name, ts=value.ts");

    assertThatThrownBy(() -> reconciler().mapperFor(tableConfig, record(keySchema, valueSchema)))
        .isInstanceOf(SchemaMismatchException.class)
        .hasMessageContaining(
            "CREATE TABLE IF NOT EXISTS shop.orders (id bigint,name text,ts timestamp,PRIMARY KEY(id))");
    verify(session, never()).execute(any(Statement.class));
  }

  @Test
  void should_be_fatal_on_unauthorized_create() {
    TableConfig tableConfig = pending("id=key.id, name=value.name, ts=value.ts");
    when(session.execute(any(Statement.class)))
        .thenThrow(new UnauthorizedException(null, "no permission"));

    assertThatThrownBy(() -> reconciler().mapperFor(tableConfig, record(keySchema, valueSchema)))
        .isInstanceOf(FatalSchemaException.class)
        .hasMessageContaining("CREATE TABLE on shop.orders");
  }

  private TableConfig implicitPending(String topic) {
    TableConfig tableConfig = TableConfig.implicit(topic, "shop", false);
    TopicState implicitTopic = new TopicState(null);
    implicitTopic.addPendingTable(tableConfig);
    Map<String, TopicState> topicStates = new HashMap<>();
    topicStates.put(topic, implicitTopic);
    state = new InstanceState(mock(CassandraSinkConfig.class), session, topicStates, registry());
    return tableConfig;
  }

  private void assertImplicitCreate(String topic, String qualified) {
    when(keyspace.getTable(CqlIdentifier.fromInternal(topic)))
        .thenReturn(Optional.empty(), Optional.of(table));
    TableConfig tableConfig = implicitPending(topic);

    RecordMapper mapper = reconciler().mapperFor(tableConfig, record(keySchema, valueSchema));

    ArgumentCaptor<SimpleStatement> stmt = ArgumentCaptor.forClass(SimpleStatement.class);
    verify(session).execute(stmt.capture());
    assertThat(stmt.getValue().getQuery())
        .isEqualTo(
            "CREATE TABLE IF NOT EXISTS "
                + qualified
                + " (id bigint,name text,ts timestamp,PRIMARY KEY(id))");
    verify(session).prepareAsync(startsWith("INSERT INTO " + qualified + "(id,name,ts) VALUES"));
    assertThat(mapper).isNotNull().isSameAs(state.getRecordMapper(tableConfig));
  }

  @Test
  void should_create_table_named_exactly_like_topic() {
    assertImplicitCreate("orders-v1.raw", "shop.\"orders-v1.raw\"");
  }

  @Test
  void should_create_case_sensitive_table_named_like_topic() {
    assertImplicitCreate("Orders", "shop.\"Orders\"");
  }

  @Test
  void should_create_single_key_column_when_key_field_also_in_value() {
    TableConfig tableConfig = implicitPending("orders");
    AbstractSchema value =
        struct(
            "id",
            prim(AbstractSchema.Type.INT64, null),
            "name",
            prim(AbstractSchema.Type.STRING, null));

    reconciler().mapperFor(tableConfig, record(keySchema, value));

    ArgumentCaptor<SimpleStatement> stmt = ArgumentCaptor.forClass(SimpleStatement.class);
    verify(session).execute(stmt.capture());
    assertThat(stmt.getValue().getQuery())
        .isEqualTo("CREATE TABLE IF NOT EXISTS shop.orders (id bigint,name text,PRIMARY KEY(id))");
    verify(session).prepareAsync(startsWith("INSERT INTO shop.orders(id,name) VALUES (:id,:name)"));
  }

  @Test
  void should_reject_non_struct_key_for_implicit_create() {
    TableConfig tableConfig = implicitPending("orders");

    assertThatThrownBy(
            () ->
                reconciler()
                    .mapperFor(
                        tableConfig, record(prim(AbstractSchema.Type.INT64, null), valueSchema)))
        .isInstanceOf(SchemaMismatchException.class)
        .hasMessageContaining("implicit table creation needs a struct record key");
    verify(session, never()).execute(any(Statement.class));
  }

  @Test
  void should_reject_schemaless_value_in_implicit_mode() {
    TableConfig tableConfig = implicitPending("orders");

    assertThatThrownBy(() -> reconciler().mapperFor(tableConfig, record(keySchema, null)))
        .isInstanceOf(SchemaMismatchException.class)
        .hasMessageContaining("struct record value");
    verify(session, never()).execute(any(Statement.class));
  }

  @Test
  void should_report_implicit_create_table_cql_when_create_table_off() {
    when(config.isCreateTable()).thenReturn(false);
    TableConfig tableConfig = implicitPending("orders");

    assertThatThrownBy(() -> reconciler().mapperFor(tableConfig, record(keySchema, valueSchema)))
        .isInstanceOf(SchemaMismatchException.class)
        .hasMessageContaining("Table shop.orders does not exist")
        .hasMessageContaining(
            "CREATE TABLE IF NOT EXISTS shop.orders (id bigint,name text,ts timestamp,PRIMARY KEY(id))");
    verify(session, never()).execute(any(Statement.class));
  }

  @Test
  void should_prepare_implicit_mapping_against_existing_table_without_ddl() {
    when(keyspace.getTable(ORDERS)).thenReturn(Optional.of(table));
    TableConfig tableConfig = implicitPending("orders");

    RecordMapper mapper = reconciler().mapperFor(tableConfig, record(keySchema, valueSchema));

    verify(session, never()).execute(any(Statement.class));
    verify(session).prepareAsync(startsWith("INSERT INTO shop.orders(id,name,ts) VALUES"));
    assertThat(mapper).isNotNull();
  }

  @Test
  void should_report_alter_table_for_unknown_value_field_when_evolve_off() {
    when(keyspace.getTable(ORDERS)).thenReturn(Optional.of(table));
    TableConfig tableConfig = implicitPending("orders");
    AbstractSchema value =
        struct(
            "name",
            prim(AbstractSchema.Type.STRING, null),
            "email",
            prim(AbstractSchema.Type.STRING, null));

    assertThatThrownBy(() -> reconciler().mapperFor(tableConfig, record(keySchema, value)))
        .isInstanceOf(SchemaMismatchException.class)
        .hasMessageContaining("ALTER TABLE shop.orders ADD email text");
    verify(session, never()).execute(any(Statement.class));
    verify(session, never()).prepareAsync(anyString());
  }

  @Test
  void should_replace_implicit_mapper_when_value_schema_changes() {
    when(keyspace.getTable(ORDERS)).thenReturn(Optional.of(table));
    TableConfig tableConfig = implicitPending("orders");
    DdlSchemaReconciler reconciler = reconciler();

    RecordMapper first = reconciler.mapperFor(tableConfig, record(keySchema, valueSchema));
    RecordMapper second =
        reconciler.mapperFor(
            tableConfig, record(keySchema, struct("name", prim(AbstractSchema.Type.STRING, null))));

    verify(session).prepareAsync(startsWith("INSERT INTO shop.orders(id,name) VALUES"));
    assertThat(second).isNotSameAs(first).isSameAs(state.getRecordMapper(tableConfig));
  }
}
