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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.common.sink.config.CassandraSinkConfig;
import com.datastax.oss.common.sink.config.MappingMode;
import com.datastax.oss.common.sink.config.TableConfig;
import com.datastax.oss.common.sink.config.TableConfigBuilder;
import com.datastax.oss.common.sink.config.TopicConfig;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.Test;

class InstanceStateTest {
  private CassandraSinkConfig config = mock(CassandraSinkConfig.class);
  private CqlSession session = mock(CqlSession.class);

  private Map<String, TopicState> topicStates = new HashMap<>();
  private InstanceState instanceState =
      new InstanceState(config, session, topicStates, new MetricRegistry());

  @Test
  void getTopicConfig_fail() {
    assertTopicNotFound(() -> instanceState.getTopicConfig("unknown"));
  }

  @Test
  void getBatchSizeHistogram_fail() {
    assertTopicNotFound(() -> instanceState.getBatchSizeHistogram("unknown", "unknown"));
  }

  @Test
  void getRecordMapper_fail() {
    TableConfig config = mock(TableConfig.class);
    when(config.getTopicName()).thenReturn("unknown");
    assertTopicNotFound(() -> instanceState.getRecordMapper(config));
  }

  private void assertTopicNotFound(ThrowableAssert.ThrowingCallable callable) {
    assertThatThrownBy(callable)
        .isInstanceOf(RuntimeException.class)
        .hasMessage(
            "Connector has no configuration for record topic 'unknown'. "
                + "Please update the configuration and restart.");
  }

  @Test
  void should_not_reset_counter_when_create_new_instance_state() {
    // given
    MetricRegistry metricRegistry = new MetricRegistry();
    TopicState topicState = new TopicState(null);
    topicState.initializeMetrics(metricRegistry);
    topicState.createRecordMapper(
        new TableConfigBuilder("t1", "ks", "tb", false)
            .addSimpleSetting(MAPPING_OPT, "v=key.v")
            .build(),
        ImmutableList.of(),
        null,
        null);

    Map<String, TopicState> topicStates = ImmutableMap.of("t1", topicState);
    InstanceState instanceState =
        new InstanceState(
            new CassandraSinkConfig(ImmutableMap.of("name", "instance-a")),
            mock(CqlSession.class),
            topicStates,
            metricRegistry);

    // when
    instanceState.incrementRecordCounter("t1", "ks.tb", 1);

    // then
    assertEquals(instanceState.getRecordCounter("t1", "ks.tb"), 1);

    // when create new instanceState
    InstanceState instanceState2 =
        new InstanceState(
            new CassandraSinkConfig(ImmutableMap.of("name", "instance-b")),
            mock(CqlSession.class),
            topicStates,
            metricRegistry);

    // then metrics should not reset
    assertEquals(instanceState2.getRecordCounter("t1", "ks.tb"), 1);
  }

  @Test
  void pass_through_is_used_when_all_flags_off() {
    CassandraSinkConfig sinkConfig = new CassandraSinkConfig(ImmutableMap.of("name", "inst"));
    InstanceState state = new InstanceState(sinkConfig, session, topicStates, new MetricRegistry());

    assertThat(state.getSchemaReconciler()).isEmpty();
    verify(session, never()).execute(any(Statement.class));
    verify(session, never()).getMetadata();
  }

  @Test
  void pass_through_is_used_when_only_create_keyspace_on() {
    when(config.isCreateKeyspace()).thenReturn(true);
    when(config.isSchemaManagementEnabled()).thenReturn(true);

    assertThat(
            new InstanceState(config, session, topicStates, new MetricRegistry())
                .getSchemaReconciler())
        .isEmpty();
  }

  @Test
  void ddl_reconciler_is_used_when_create_table_evolve_table_or_implicit() {
    CassandraSinkConfig createTable = mock(CassandraSinkConfig.class);
    when(createTable.isCreateTable()).thenReturn(true);
    CassandraSinkConfig evolveTable = mock(CassandraSinkConfig.class);
    when(evolveTable.isEvolveTable()).thenReturn(true);
    CassandraSinkConfig implicit = mock(CassandraSinkConfig.class);
    when(implicit.getMappingMode()).thenReturn(MappingMode.IMPLICIT);

    for (CassandraSinkConfig c : ImmutableList.of(createTable, evolveTable, implicit)) {
      assertThat(
              new InstanceState(c, session, topicStates, new MetricRegistry())
                  .getSchemaReconciler())
          .containsInstanceOf(DdlSchemaReconciler.class);
    }
  }

  @Test
  void should_create_topic_lazily_for_regex_subscription_in_implicit_mode() throws Exception {
    MetricRegistry metricRegistry = new MetricRegistry();
    CassandraSinkConfig sinkConfig =
        new CassandraSinkConfig(
            ImmutableMap.of(
                "name",
                "inst",
                "topics.regex",
                "orders.*",
                "mappingMode",
                "IMPLICIT",
                "keyspace",
                "shop"));
    InstanceState state = new InstanceState(sinkConfig, session, new HashMap<>(), metricRegistry);
    ExecutorService executor = Executors.newFixedThreadPool(8);
    try {
      CountDownLatch start = new CountDownLatch(1);
      List<Future<TopicConfig>> results = new ArrayList<>();
      for (int i = 0; i < 16; i++) {
        results.add(
            executor.submit(
                () -> {
                  start.await();
                  return state.getTopicConfig("orders-v1.raw");
                }));
      }
      start.countDown();
      TopicConfig first = results.get(0).get(10, TimeUnit.SECONDS);
      for (Future<TopicConfig> result : results) {
        assertThat(result.get(10, TimeUnit.SECONDS)).isSameAs(first);
      }
      TableConfig table = first.getTableConfigs().iterator().next();
      assertThat(table.isImplicit()).isTrue();
      assertThat(table.getKeyspaceAndTable()).isEqualTo("shop.\"orders-v1.raw\"");
      assertThat(state.getRecordMapper(table)).isNull();

      state.incrementRecordCounter("orders-v1.raw", "shop.\"orders-v1.raw\"", 2);
      assertEquals(2, state.getRecordCounter("orders-v1.raw", "shop.\"orders-v1.raw\""));
      assertThat(metricRegistry.getNames())
          .anyMatch(n -> n.contains("orders-v1.raw") && n.contains("recordCount"));
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void should_not_create_unknown_topic_in_explicit_mode() {
    InstanceState state =
        new InstanceState(
            new CassandraSinkConfig(ImmutableMap.of("name", "inst")),
            session,
            new HashMap<>(),
            new MetricRegistry());
    assertThatThrownBy(() -> state.getTopicConfig("unknown"))
        .hasMessageContaining("no configuration for record topic 'unknown'");
  }
}
