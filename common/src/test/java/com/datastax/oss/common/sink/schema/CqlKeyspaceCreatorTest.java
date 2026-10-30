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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.slf4j.event.Level.WARN;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.servererrors.UnauthorizedException;
import com.datastax.oss.dsbulk.tests.logging.LogCapture;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptingExtension;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

@ExtendWith(LogInterceptingExtension.class)
class CqlKeyspaceCreatorTest {
  private static final CqlIdentifier SHOP = CqlIdentifier.fromInternal("shop");

  private CqlSession session;
  private Metadata metadata;

  @BeforeEach
  void setUp() {
    session = mock(CqlSession.class);
    metadata = mock(Metadata.class);
    when(session.getMetadata()).thenReturn(metadata);
  }

  @Test
  void should_create_with_simple_strategy_when_missing() {
    when(metadata.getKeyspace(SHOP)).thenReturn(Optional.empty());

    new CqlKeyspaceCreator(2).ensureKeyspace(session, SHOP);

    ArgumentCaptor<SimpleStatement> stmt = ArgumentCaptor.forClass(SimpleStatement.class);
    InOrder order = inOrder(session);
    order.verify(session).execute(stmt.capture());
    order.verify(session).checkSchemaAgreement();
    order.verify(session).refreshSchema();
    assertThat(stmt.getValue().getQuery())
        .isEqualTo(
            "CREATE KEYSPACE IF NOT EXISTS shop WITH replication={'class':'SimpleStrategy','replication_factor':2}");
  }

  @Test
  void should_do_nothing_when_present() {
    when(metadata.getKeyspace(SHOP)).thenReturn(Optional.of(mock(KeyspaceMetadata.class)));

    new CqlKeyspaceCreator(2).ensureKeyspace(session, SHOP);

    verify(session, never()).execute(any(Statement.class));
  }

  @Test
  void should_be_fatal_on_unauthorized() {
    when(metadata.getKeyspace(SHOP)).thenReturn(Optional.empty());
    when(session.execute(any(Statement.class)))
        .thenThrow(new UnauthorizedException(null, "no permission"));

    assertThatThrownBy(() -> new CqlKeyspaceCreator(2).ensureKeyspace(session, SHOP))
        .isInstanceOf(FatalSchemaException.class)
        .hasMessageContaining("CREATE")
        .hasMessageContaining("shop");
  }

  @Test
  void should_warn_without_schema_agreement(
      @LogCapture(level = WARN, value = CqlKeyspaceCreator.class) LogInterceptor logs) {
    when(metadata.getKeyspace(SHOP)).thenReturn(Optional.empty());
    when(session.checkSchemaAgreement()).thenReturn(false);

    new CqlKeyspaceCreator(2).ensureKeyspace(session, SHOP);

    assertThat(logs.getLoggedMessages())
        .contains("Schema agreement not reached after creating keyspace shop");
  }
}
