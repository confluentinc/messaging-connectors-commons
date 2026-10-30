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
import static org.slf4j.event.Level.WARN;

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
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.UnauthorizedException;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.dsbulk.tests.logging.LogCapture;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptingExtension;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

@ExtendWith(LogInterceptingExtension.class)
class DdlSchemaReconcilerTest {
  private static final String TOPIC = "orders-topic";
  private static final CqlIdentifier ORDERS = CqlIdentifier.fromInternal("orders");
  private static final String TIMESTAMP = "org.apache.kafka.connect.data.Timestamp";
  private static final Pattern ALTER =
      Pattern.compile("ALTER TABLE shop\\.orders ADD (\\w+) (\\w+)");

  private final CqlSession session = mock(CqlSession.class);
  private final CassandraSinkConfig config = mock(CassandraSinkConfig.class);
  private final KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
  private final TableMetadata table = mock(TableMetadata.class);
  private final Metadata metadata = mock(Metadata.class);
  private final Map<CqlIdentifier, ColumnMetadata> serverColumns = new ConcurrentHashMap<>();
  private final Map<CqlIdentifier, ColumnMetadata> visibleColumns = new ConcurrentHashMap<>();
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
    when(record.keySchemaIdentity()).thenReturn(key == null ? null : key.schemaIdentity());
    when(record.valueSchemaIdentity()).thenReturn(value == null ? null : value.schemaIdentity());
    when(record.value()).thenReturn(new Object());
    return record;
  }

  private AbstractSinkRecord tombstone() {
    AbstractSinkRecord record = mock(AbstractSinkRecord.class);
    when(record.topic()).thenReturn(TOPIC);
    when(record.keySchema()).thenReturn(keySchema);
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

  @Test
  void should_return_resolved_mapper_for_tombstone_without_ddl() {
    when(keyspace.getTable(ORDERS)).thenReturn(Optional.of(table));
    TableConfig tableConfig = implicitPending("orders");
    DdlSchemaReconciler reconciler = reconciler();
    RecordMapper first = reconciler.mapperFor(tableConfig, record(keySchema, valueSchema));

    assertThat(reconciler.mapperFor(tableConfig, tombstone())).isSameAs(first);
    verify(session, never()).execute(any(Statement.class));
    verify(session, times(1)).prepareAsync(startsWith("INSERT"));
  }

  @Test
  void should_reject_tombstone_before_first_record_in_implicit_mode() {
    TableConfig tableConfig = implicitPending("orders");

    assertThatThrownBy(() -> reconciler().mapperFor(tableConfig, tombstone()))
        .isInstanceOf(SchemaMismatchException.class)
        .hasMessageContaining(
            "cannot apply a delete to shop.orders before its first non-null record");
    verify(session, never()).execute(any(Statement.class));
    verify(session, never()).prepareAsync(anyString());
  }

  @Test
  void should_reject_tombstone_for_pending_explicit_binding() {
    TableConfig tableConfig = pending("id=key.id, name=value.name, ts=value.ts");

    assertThatThrownBy(() -> reconciler().mapperFor(tableConfig, tombstone()))
        .isInstanceOf(SchemaMismatchException.class)
        .hasMessageContaining(
            "cannot apply a delete to shop.orders before its first non-null record");
    verify(session, never()).execute(any(Statement.class));
    verify(session, never()).getMetadata();
  }

  @Test
  void should_report_key_mismatch_without_alter_for_missing_key_column() {
    when(keyspace.getTable(ORDERS)).thenReturn(Optional.of(table));
    TableConfig tableConfig = implicitPending("orders");
    AbstractSchema key = struct("tenant", prim(AbstractSchema.Type.STRING, null));

    assertThatThrownBy(() -> reconciler().mapperFor(tableConfig, record(key, valueSchema)))
        .isInstanceOf(SchemaMismatchException.class)
        .hasMessageContaining("primary key of table shop.orders does not match the record key")
        .hasMessageContaining("tenant")
        .satisfies(e -> assertThat(e.getMessage()).doesNotContain("ALTER TABLE"));
    verify(session, never()).execute(any(Statement.class));
  }

  private static ColumnMetadata column(CqlIdentifier name, DataType type) {
    ColumnMetadata col = mock(ColumnMetadata.class);
    when(col.getName()).thenReturn(name);
    when(col.getType()).thenReturn(type);
    return col;
  }

  /** ALTER adds a column on the server; refreshSchema makes it visible in the table metadata. */
  private void evolvingTable(String... names) {
    when(config.isEvolveTable()).thenReturn(true);
    when(session.checkSchemaAgreement()).thenReturn(true);
    when(keyspace.getTable(ORDERS)).thenReturn(Optional.of(table));
    for (String name : names) {
      CqlIdentifier ident = CqlIdentifier.fromInternal(name);
      when(table.getColumn(ident)).thenAnswer(i -> Optional.ofNullable(visibleColumns.get(ident)));
    }
    when(session.execute(any(Statement.class)))
        .thenAnswer(
            i -> {
              Matcher m = ALTER.matcher(((SimpleStatement) i.getArgument(0)).getQuery());
              if (m.matches()) {
                CqlIdentifier ident = CqlIdentifier.fromInternal(m.group(1));
                DataType type = m.group(2).equals("int") ? DataTypes.INT : DataTypes.TEXT;
                if (serverColumns.putIfAbsent(ident, column(ident, type)) != null) {
                  throw new InvalidQueryException(
                      null,
                      "Invalid column name "
                          + m.group(1)
                          + " because it conflicts with an existing column");
                }
              }
              return null;
            });
    when(session.refreshSchema())
        .thenAnswer(
            i -> {
              visibleColumns.putAll(serverColumns);
              return metadata;
            });
  }

  private static AbstractSchema valueWith(Object... extraFields) {
    Object[] pairs = new Object[4 + extraFields.length];
    pairs[0] = "name";
    pairs[1] = prim(AbstractSchema.Type.STRING, null);
    pairs[2] = "ts";
    pairs[3] = prim(AbstractSchema.Type.INT64, TIMESTAMP);
    System.arraycopy(extraFields, 0, pairs, 4, extraFields.length);
    return struct(pairs);
  }

  private static String alterQuery(Statement<?> statement) {
    return ((SimpleStatement) statement).getQuery();
  }

  @Test
  void should_add_new_value_field_with_one_alter_per_column() {
    evolvingTable("email", "age");
    TableConfig tableConfig = implicitPending("orders");
    DdlSchemaReconciler reconciler = reconciler();
    RecordMapper first = reconciler.mapperFor(tableConfig, record(keySchema, valueSchema));

    AbstractSchema v2 =
        valueWith(
            "email",
            prim(AbstractSchema.Type.STRING, null),
            "age",
            prim(AbstractSchema.Type.INT32, null));
    RecordMapper second = reconciler.mapperFor(tableConfig, record(keySchema, v2));

    ArgumentCaptor<Statement> stmt = ArgumentCaptor.forClass(Statement.class);
    InOrder order = inOrder(session);
    order.verify(session, times(2)).execute(stmt.capture());
    order.verify(session).checkSchemaAgreement();
    order
        .verify(session)
        .prepareAsync(startsWith("INSERT INTO shop.orders(id,name,ts,email,age) VALUES"));
    assertThat(stmt.getAllValues())
        .extracting(DdlSchemaReconcilerTest::alterQuery)
        .containsExactly(
            "ALTER TABLE shop.orders ADD email text", "ALTER TABLE shop.orders ADD age int");
    assertThat(second).isNotSameAs(first).isSameAs(state.getRecordMapper(tableConfig));
  }

  @Test
  void should_treat_already_exists_as_success_and_reread_metadata() {
    evolvingTable("email");
    serverColumns.put(
        CqlIdentifier.fromInternal("email"),
        column(CqlIdentifier.fromInternal("email"), DataTypes.TEXT));
    TableConfig tableConfig = implicitPending("orders");

    RecordMapper mapper =
        reconciler()
            .mapperFor(
                tableConfig,
                record(keySchema, valueWith("email", prim(AbstractSchema.Type.STRING, null))));

    verify(session, times(1)).execute(any(Statement.class));
    verify(session, atLeastOnce()).refreshSchema();
    verify(session).prepareAsync(startsWith("INSERT INTO shop.orders(id,name,ts,email) VALUES"));
    assertThat(mapper).isNotNull().isSameAs(state.getRecordMapper(tableConfig));
  }

  @Test
  void should_not_compare_types_when_column_already_exists() {
    evolvingTable("age");
    serverColumns.put(
        CqlIdentifier.fromInternal("age"),
        column(CqlIdentifier.fromInternal("age"), DataTypes.TEXT));
    TableConfig tableConfig = implicitPending("orders");

    RecordMapper mapper =
        reconciler()
            .mapperFor(
                tableConfig,
                record(keySchema, valueWith("age", prim(AbstractSchema.Type.INT32, null))));

    verify(session, times(1)).execute(any(Statement.class));
    verify(session).prepareAsync(startsWith("INSERT INTO shop.orders(id,name,ts,age) VALUES"));
    assertThat(mapper).isNotNull().isSameAs(state.getRecordMapper(tableConfig));
  }

  @Test
  void should_rethrow_alter_error_when_column_did_not_appear() {
    evolvingTable("email");
    when(session.execute(any(Statement.class)))
        .thenThrow(new InvalidQueryException(null, "some other problem"));
    TableConfig tableConfig = implicitPending("orders");

    assertThatThrownBy(
            () ->
                reconciler()
                    .mapperFor(
                        tableConfig,
                        record(
                            keySchema, valueWith("email", prim(AbstractSchema.Type.STRING, null)))))
        .isInstanceOf(SchemaMismatchException.class)
        .hasMessageContaining("ALTER TABLE shop.orders ADD email text")
        .hasCauseInstanceOf(InvalidQueryException.class);
    verify(session, never()).prepareAsync(anyString());
  }

  @Test
  void should_be_fatal_on_unauthorized_alter() {
    evolvingTable("email");
    when(session.execute(any(Statement.class)))
        .thenThrow(new UnauthorizedException(null, "no permission"));
    TableConfig tableConfig = implicitPending("orders");

    assertThatThrownBy(
            () ->
                reconciler()
                    .mapperFor(
                        tableConfig,
                        record(
                            keySchema, valueWith("email", prim(AbstractSchema.Type.STRING, null)))))
        .isInstanceOf(FatalSchemaException.class)
        .hasMessageContaining("ALTER TABLE on shop.orders");
  }

  @Test
  void should_issue_single_alter_under_concurrency() throws Exception {
    evolvingTable("email");
    TableConfig tableConfig = implicitPending("orders");
    DdlSchemaReconciler reconciler = reconciler();
    executor = Executors.newFixedThreadPool(2);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<RecordMapper>> results = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      // Each task has its own, equal schema instance.
      AbstractSinkRecord record =
          record(keySchema, valueWith("email", prim(AbstractSchema.Type.STRING, null)));
      results.add(
          executor.submit(
              () -> {
                start.await();
                return reconciler.mapperFor(tableConfig, record);
              }));
    }
    start.countDown();

    for (Future<RecordMapper> result : results) {
      assertThat(result.get(10, TimeUnit.SECONDS)).isNotNull();
    }
    ArgumentCaptor<Statement> stmt = ArgumentCaptor.forClass(Statement.class);
    verify(session, times(1)).execute(stmt.capture());
    assertThat(alterQuery(stmt.getValue())).isEqualTo("ALTER TABLE shop.orders ADD email text");
  }

  @Test
  void explicit_mode_adds_mapped_but_missing_column_when_evolve_on() {
    evolvingTable("email");
    TableConfig tableConfig = pending("id=key.id, name=value.name, email=value.email");

    RecordMapper mapper =
        reconciler()
            .mapperFor(
                tableConfig,
                record(keySchema, valueWith("email", prim(AbstractSchema.Type.STRING, null))));

    ArgumentCaptor<Statement> stmt = ArgumentCaptor.forClass(Statement.class);
    verify(session).execute(stmt.capture());
    assertThat(alterQuery(stmt.getValue())).isEqualTo("ALTER TABLE shop.orders ADD email text");
    verify(session).prepareAsync(startsWith("INSERT INTO shop.orders(id,name,email) VALUES"));
    assertThat(mapper).isNotNull().isSameAs(state.getRecordMapper(tableConfig));
  }

  @Test
  void should_never_alter_key_column_when_evolve_on() {
    evolvingTable("tenant");
    TableConfig tableConfig = implicitPending("orders");
    AbstractSchema key = struct("tenant", prim(AbstractSchema.Type.STRING, null));

    assertThatThrownBy(() -> reconciler().mapperFor(tableConfig, record(key, valueSchema)))
        .isInstanceOf(SchemaMismatchException.class)
        .hasMessageContaining("primary key of table shop.orders does not match the record key")
        .satisfies(e -> assertThat(e.getMessage()).doesNotContain("ALTER TABLE"));
    verify(session, never()).execute(any(Statement.class));
  }

  @Test
  void should_warn_and_continue_without_schema_agreement(
      @LogCapture(level = WARN, value = DdlSchemaReconciler.class) LogInterceptor logs) {
    evolvingTable("email");
    when(session.checkSchemaAgreement()).thenReturn(false);
    TableConfig tableConfig = implicitPending("orders");

    RecordMapper mapper =
        reconciler()
            .mapperFor(
                tableConfig,
                record(keySchema, valueWith("email", prim(AbstractSchema.Type.STRING, null))));

    assertThat(mapper).isNotNull();
    assertThat(logs.getLoggedMessages())
        .contains("Schema agreement not reached after altering table shop.orders");
  }

  @Test
  void should_reuse_mappers_when_alternating_between_schema_versions() {
    when(keyspace.getTable(ORDERS)).thenReturn(Optional.of(table));
    TableConfig tableConfig = implicitPending("orders");
    DdlSchemaReconciler reconciler = reconciler();
    AbstractSchema v2 = struct("name", prim(AbstractSchema.Type.STRING, null));

    RecordMapper m1 = reconciler.mapperFor(tableConfig, record(keySchema, valueSchema));
    RecordMapper m2 = reconciler.mapperFor(tableConfig, record(keySchema, v2));

    assertThat(reconciler.mapperFor(tableConfig, record(keySchema, valueSchema))).isSameAs(m1);
    assertThat(reconciler.mapperFor(tableConfig, record(keySchema, v2))).isSameAs(m2);
    verify(session, times(2)).prepareAsync(startsWith("INSERT"));
  }

  @Test
  void should_evict_oldest_schema_version_beyond_cache_capacity() {
    when(keyspace.getTable(ORDERS)).thenReturn(Optional.of(table));
    TableConfig tableConfig = implicitPending("orders");
    DdlSchemaReconciler reconciler = reconciler();
    List<AbstractSchema> versions = new ArrayList<>();
    for (int i = 0; i < 9; i++) {
      versions.add(struct("name", prim(AbstractSchema.Type.STRING, null)));
      reconciler.mapperFor(tableConfig, record(keySchema, versions.get(i)));
    }

    reconciler.mapperFor(tableConfig, record(keySchema, versions.get(8)));
    verify(session, times(9)).prepareAsync(startsWith("INSERT"));
    reconciler.mapperFor(tableConfig, record(keySchema, versions.get(0)));
    verify(session, times(10)).prepareAsync(startsWith("INSERT"));
  }

  @Test
  void should_not_build_schema_wrappers_on_fast_path() {
    when(keyspace.getTable(ORDERS)).thenReturn(Optional.of(table));
    TableConfig tableConfig = implicitPending("orders");
    DdlSchemaReconciler reconciler = reconciler();
    reconciler.mapperFor(tableConfig, record(keySchema, valueSchema));
    AbstractSinkRecord second = record(keySchema, valueSchema);

    reconciler.mapperFor(tableConfig, second);

    verify(second, never()).keySchema();
    verify(second, never()).valueSchema();
  }
}
