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
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.UnauthorizedException;

/** Creates keyspaces with CQL, using SimpleStrategy. */
public class CqlKeyspaceCreator implements KeyspaceCreator {
  private final int replicationFactor;

  public CqlKeyspaceCreator(int replicationFactor) {
    this.replicationFactor = replicationFactor;
  }

  @Override
  public void ensureKeyspace(CqlSession session, CqlIdentifier keyspace) {
    if (session.getMetadata().getKeyspace(keyspace).isPresent()) {
      return;
    }
    String cql = CqlDdl.createKeyspace(keyspace, replicationFactor);
    try {
      session.execute(SimpleStatement.newInstance(cql));
    } catch (UnauthorizedException e) {
      throw new FatalSchemaException(
          String.format(
              "Not authorized to run %s; grant CREATE on ALL KEYSPACES or create keyspace %s manually",
              cql, keyspace.asInternal()),
          e);
    }
    session.checkSchemaAgreement();
    session.refreshSchema();
  }
}
