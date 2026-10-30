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
  private static final long TIMEOUT_MILLIS = 5 * 60_000;

  private final AstraDevOpsClient client;
  private final String databaseId;
  private final long pollIntervalMillis;
  private final long timeoutMillis;

  public AstraKeyspaceCreator(AstraDevOpsClient client, String databaseId) {
    this(client, databaseId, POLL_INTERVAL_MILLIS, TIMEOUT_MILLIS);
  }

  AstraKeyspaceCreator(
      AstraDevOpsClient client, String databaseId, long pollIntervalMillis, long timeoutMillis) {
    this.client = client;
    this.databaseId = databaseId;
    this.pollIntervalMillis = pollIntervalMillis;
    this.timeoutMillis = timeoutMillis;
  }

  @Override
  public void ensureKeyspace(CqlSession session, CqlIdentifier keyspace) {
    if (session.getMetadata().getKeyspace(keyspace).isPresent()) {
      return;
    }
    client.createKeyspace(databaseId, keyspace.asInternal());

    // ACTIVE may be reported before the database enters MAINTENANCE, so the keyspace showing up
    // in the driver metadata is what ends the wait.
    long deadline = System.nanoTime() + timeoutMillis * 1_000_000;
    boolean active = false;
    while (true) {
      active = active || "ACTIVE".equals(client.getDatabaseStatus(databaseId));
      if (active) {
        session.refreshSchema();
        if (session.getMetadata().getKeyspace(keyspace).isPresent()) {
          return;
        }
      }
      if (System.nanoTime() > deadline) {
        throw new FatalSchemaException(
            active
                ? String.format(
                    "Keyspace %s did not become visible after creation", keyspace.asInternal())
                : String.format(
                    "Database %s did not become ACTIVE after creating keyspace %s",
                    databaseId, keyspace.asInternal()));
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
