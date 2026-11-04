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

import com.datastax.oss.common.sink.AbstractSchema;
import com.datastax.oss.common.sink.ConfigException;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/** Maps Connect schemas to CQL types and renders the DDL statements of schema management. */
public final class CqlDdl {

  public static final String DECIMAL = "org.apache.kafka.connect.data.Decimal";
  public static final String DATE = "org.apache.kafka.connect.data.Date";
  public static final String TIME = "org.apache.kafka.connect.data.Time";
  public static final String TIMESTAMP = "org.apache.kafka.connect.data.Timestamp";

  private CqlDdl() {}

  public static DataType toCql(AbstractSchema schema) {
    String name = schema.name();
    if (name != null) {
      switch (name) {
        case DECIMAL:
          return DataTypes.DECIMAL;
        case DATE:
          return DataTypes.DATE;
        case TIME:
          return DataTypes.TIME;
        case TIMESTAMP:
          return DataTypes.TIMESTAMP;
        default:
          break;
      }
    }
    switch (schema.type()) {
      case INT8:
        return DataTypes.TINYINT;
      case INT16:
        return DataTypes.SMALLINT;
      case INT32:
        return DataTypes.INT;
      case INT64:
        return DataTypes.BIGINT;
      case FLOAT32:
        return DataTypes.FLOAT;
      case FLOAT64:
        return DataTypes.DOUBLE;
      case BOOLEAN:
        return DataTypes.BOOLEAN;
      case STRING:
        return DataTypes.TEXT;
      case BYTES:
        return DataTypes.BLOB;
      case ARRAY:
        return DataTypes.listOf(frozen(toCql(schema.valueSchema())));
      case MAP:
        return DataTypes.mapOf(
            frozen(toCql(schema.keySchema())), frozen(toCql(schema.valueSchema())));
      default:
        throw new ConfigException("Unsupported schema type " + schema.type());
    }
  }

  /** Collections nested in collections or used as key columns must be frozen. */
  static DataType frozen(DataType type) {
    if (type instanceof ListType) {
      return DataTypes.frozenListOf(((ListType) type).getElementType());
    }
    if (type instanceof SetType) {
      return DataTypes.frozenSetOf(((SetType) type).getElementType());
    }
    if (type instanceof MapType) {
      MapType map = (MapType) type;
      return DataTypes.frozenMapOf(map.getKeyType(), map.getValueType());
    }
    return type;
  }

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
      appendColumn(cql, column.getKey(), frozen(column.getValue()));
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
