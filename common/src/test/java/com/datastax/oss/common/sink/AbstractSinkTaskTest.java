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
package com.datastax.oss.common.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.common.sink.config.TableConfig;
import com.datastax.oss.common.sink.config.TopicConfig;
import com.datastax.oss.common.sink.schema.FatalSchemaException;
import com.datastax.oss.common.sink.schema.SchemaMismatchException;
import com.datastax.oss.common.sink.state.InstanceState;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AbstractSinkTaskTest {
  private final List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
  private final InstanceState instanceState = mock(InstanceState.class);
  private AbstractSinkTask task;

  @BeforeEach
  void setUp() throws Exception {
    TableConfig tableConfig = mock(TableConfig.class);
    when(tableConfig.getKeyspaceAndTable()).thenReturn("ks.tb");
    TopicConfig topicConfig = mock(TopicConfig.class);
    when(topicConfig.getTableConfigs()).thenReturn(Collections.singletonList(tableConfig));
    when(instanceState.getTopicConfig("mytopic")).thenReturn(topicConfig);
    when(instanceState.getMappingExecutor()).thenReturn(Runnable::run);
    when(instanceState.getMaxNumberOfRecordsInBatch()).thenReturn(32);

    // Mirrors an errors.tolerance=all task: every failure is swallowed.
    task =
        new AbstractSinkTask() {
          @Override
          protected void handleFailure(
              AbstractSinkRecord record, Throwable e, String cql, Runnable failCounter) {
            failures.add(e);
          }

          @Override
          public String version() {
            return "test";
          }

          @Override
          public String applicationName() {
            return "test";
          }
        };
    task.setInstanceState(instanceState);
    Field stateManager = AbstractSinkTask.class.getDeclaredField("taskStateManager");
    stateManager.setAccessible(true);
    stateManager.set(task, new TaskStateManager());
  }

  private static AbstractSinkRecord record() {
    return new SinkRecordImpl("mytopic", 0, null, "key", null, "value", 1L);
  }

  @Test
  void mapAndQueueRecord_rethrows_fatal_schema_exception_even_when_ignore_errors_all() {
    when(instanceState.getSchemaReconciler())
        .thenReturn(
            Optional.of(
                (tableConfig, record) -> {
                  throw new FatalSchemaException("Not authorized");
                }));

    assertThatThrownBy(() -> task.put(Collections.singletonList(record())))
        .hasRootCauseInstanceOf(FatalSchemaException.class);
    assertThat(failures).isEmpty();
  }

  @Test
  void schema_mismatch_goes_to_failure_handling() {
    when(instanceState.getSchemaReconciler())
        .thenReturn(
            Optional.of(
                (tableConfig, record) -> {
                  throw new SchemaMismatchException("bad shape");
                }));

    task.put(Collections.singletonList(record()));

    assertThat(failures).hasSize(1);
    assertThat(failures.get(0)).isInstanceOf(SchemaMismatchException.class);
  }
}
