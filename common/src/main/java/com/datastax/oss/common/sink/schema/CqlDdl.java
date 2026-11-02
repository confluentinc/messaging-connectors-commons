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
import com.datastax.oss.driver.api.core.type.DataType;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/** Renders the DDL statements issued by schema management. */
public final class CqlDdl {

  private CqlDdl() {}

  public static String createKeyspace(CqlIdentifier keyspace, int replicationFactor) {
    return String.format(
        "CREATE KEYSPACE IF NOT EXISTS %s WITH replication={'class':'SimpleStrategy','replication_factor':%d}",
        keyspace.asCql(true), replicationFactor);
  }

  public static String createTable(
      CqlIdentifier keyspace,
      CqlIdentifier table,
      LinkedHashMap<CqlIdentifier, DataType> partitionKey,
      LinkedHashMap<CqlIdentifier, DataType> columns) {
    StringBuilder cql =
        new StringBuilder("CREATE TABLE IF NOT EXISTS ")
            .append(qualified(keyspace, table))
            .append(" (");
    for (Map.Entry<CqlIdentifier, DataType> column : partitionKey.entrySet()) {
      appendColumn(cql, column.getKey(), CqlTypeMapper.frozen(column.getValue()));
    }
    for (Map.Entry<CqlIdentifier, DataType> column : columns.entrySet()) {
      appendColumn(cql, column.getKey(), column.getValue());
    }
    String pk =
        partitionKey.keySet().stream().map(c -> c.asCql(true)).collect(Collectors.joining(","));
    if (partitionKey.size() > 1) {
      pk = "(" + pk + ")";
    }
    return cql.append("PRIMARY KEY(").append(pk).append("))").toString();
  }

  public static String addColumn(
      CqlIdentifier keyspace, CqlIdentifier table, CqlIdentifier column, DataType type) {
    return String.format(
        "ALTER TABLE %s ADD %s %s",
        qualified(keyspace, table), column.asCql(true), type.asCql(true, true));
  }

  private static void appendColumn(StringBuilder cql, CqlIdentifier name, DataType type) {
    cql.append(name.asCql(true)).append(' ').append(type.asCql(true, true)).append(',');
  }

  private static String qualified(CqlIdentifier keyspace, CqlIdentifier table) {
    return keyspace.asCql(true) + '.' + table.asCql(true);
  }
}
