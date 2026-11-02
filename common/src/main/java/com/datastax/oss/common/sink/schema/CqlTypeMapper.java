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
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;

/** Maps Connect schemas to CQL types. */
public final class CqlTypeMapper {

  public static final String DECIMAL = "org.apache.kafka.connect.data.Decimal";
  public static final String DATE = "org.apache.kafka.connect.data.Date";
  public static final String TIME = "org.apache.kafka.connect.data.Time";
  public static final String TIMESTAMP = "org.apache.kafka.connect.data.Timestamp";

  private CqlTypeMapper() {}

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
        throw new SchemaMismatchException("Unsupported schema type " + schema.type());
    }
  }

  /** Collections nested in collections or used as key columns must be frozen. */
  public static DataType frozen(DataType type) {
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
}
