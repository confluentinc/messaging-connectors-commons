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
package com.datastax.oss.common.sink.schema;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class AstraKeyspaceCreatorTest {
  private static final CqlIdentifier SHOP = CqlIdentifier.fromInternal("shop");

  private AstraDevOpsClient client;
  private CqlSession session;
  private Metadata metadata;

  @BeforeEach
  void setUp() {
    client = mock(AstraDevOpsClient.class);
    session = mock(CqlSession.class);
    metadata = mock(Metadata.class);
    when(session.getMetadata()).thenReturn(metadata);
  }

  @Test
  void should_post_then_poll_until_active() {
    when(metadata.getKeyspace(SHOP))
        .thenReturn(Optional.empty(), Optional.empty(), Optional.of(mock(KeyspaceMetadata.class)));
    when(client.getDatabaseStatus("db1")).thenReturn("MAINTENANCE", "MAINTENANCE", "ACTIVE");

    new AstraKeyspaceCreator(client, "db1", 1, 5000).ensureKeyspace(session, SHOP);

    InOrder order = inOrder(client);
    order.verify(client).createKeyspace("db1", "shop");
    order.verify(client, times(3)).getDatabaseStatus("db1");
    verify(session, atLeastOnce()).refreshSchema();
  }

  @Test
  void should_skip_when_keyspace_present() {
    when(metadata.getKeyspace(SHOP)).thenReturn(Optional.of(mock(KeyspaceMetadata.class)));

    new AstraKeyspaceCreator(client, "db1", 1, 5000).ensureKeyspace(session, SHOP);

    verify(client, never()).createKeyspace(any(), any());
  }

  @Test
  void should_be_fatal_when_database_never_active() {
    when(metadata.getKeyspace(SHOP)).thenReturn(Optional.empty());
    when(client.getDatabaseStatus("db1")).thenReturn("MAINTENANCE");

    assertThatThrownBy(
            () -> new AstraKeyspaceCreator(client, "db1", 1, 30).ensureKeyspace(session, SHOP))
        .isInstanceOf(FatalSchemaException.class)
        .hasMessageContaining("ACTIVE");
  }

  @Test
  void should_be_fatal_when_keyspace_never_visible() {
    when(metadata.getKeyspace(SHOP)).thenReturn(Optional.empty());
    when(client.getDatabaseStatus("db1")).thenReturn("ACTIVE");

    assertThatThrownBy(
            () -> new AstraKeyspaceCreator(client, "db1", 1, 30).ensureKeyspace(session, SHOP))
        .isInstanceOf(FatalSchemaException.class)
        .hasMessageContaining("shop");
  }

  @Test
  void should_wait_for_keyspace_within_full_budget_after_early_active() {
    long visibleAt = System.nanoTime() + 200_000_000L;
    when(metadata.getKeyspace(SHOP))
        .thenAnswer(
            i ->
                System.nanoTime() > visibleAt
                    ? Optional.of(mock(KeyspaceMetadata.class))
                    : Optional.empty());
    when(client.getDatabaseStatus("db1")).thenReturn("ACTIVE");

    new AstraKeyspaceCreator(client, "db1", 5, 5000).ensureKeyspace(session, SHOP);

    verify(client, times(1)).getDatabaseStatus("db1");
  }
}
