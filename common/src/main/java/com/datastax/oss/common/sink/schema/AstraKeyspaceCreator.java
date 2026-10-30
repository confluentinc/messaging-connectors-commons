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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;

/** Creates keyspaces through the Astra DevOps API. */
public class AstraKeyspaceCreator implements KeyspaceCreator {
  private static final long POLL_INTERVAL_MILLIS = 2_000;
  private static final long ACTIVE_TIMEOUT_MILLIS = 5 * 60_000;
  private static final long METADATA_TIMEOUT_MILLIS = 60_000;

  private final AstraDevOpsClient client;
  private final String databaseId;
  private final long pollIntervalMillis;
  private final long activeTimeoutMillis;
  private final long metadataTimeoutMillis;

  public AstraKeyspaceCreator(AstraDevOpsClient client, String databaseId) {
    this(client, databaseId, POLL_INTERVAL_MILLIS, ACTIVE_TIMEOUT_MILLIS, METADATA_TIMEOUT_MILLIS);
  }

  AstraKeyspaceCreator(
      AstraDevOpsClient client,
      String databaseId,
      long pollIntervalMillis,
      long activeTimeoutMillis,
      long metadataTimeoutMillis) {
    this.client = client;
    this.databaseId = databaseId;
    this.pollIntervalMillis = pollIntervalMillis;
    this.activeTimeoutMillis = activeTimeoutMillis;
    this.metadataTimeoutMillis = metadataTimeoutMillis;
  }

  @Override
  public void ensureKeyspace(CqlSession session, CqlIdentifier keyspace) {
    if (session.getMetadata().getKeyspace(keyspace).isPresent()) {
      return;
    }
    client.createKeyspace(databaseId, keyspace.asInternal());

    long deadline = System.nanoTime() + activeTimeoutMillis * 1_000_000;
    while (!"ACTIVE".equals(client.getDatabaseStatus(databaseId))) {
      if (System.nanoTime() > deadline) {
        throw new FatalSchemaException(
            String.format(
                "Database %s did not become ACTIVE after creating keyspace %s",
                databaseId, keyspace.asInternal()));
      }
      pause();
    }

    deadline = System.nanoTime() + metadataTimeoutMillis * 1_000_000;
    while (true) {
      session.refreshSchema();
      if (session.getMetadata().getKeyspace(keyspace).isPresent()) {
        return;
      }
      if (System.nanoTime() > deadline) {
        throw new FatalSchemaException(
            String.format(
                "Keyspace %s did not become visible after creation", keyspace.asInternal()));
      }
      pause();
    }
  }

  private void pause() {
    try {
      Thread.sleep(pollIntervalMillis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new FatalSchemaException("Interrupted while waiting for keyspace creation", e);
    }
  }
}
