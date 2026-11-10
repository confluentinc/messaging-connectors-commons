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
package com.datastax.oss.common.sink.record;

import com.datastax.oss.common.sink.AbstractSchema;
import com.datastax.oss.common.sink.AbstractStruct;
import com.datastax.oss.common.sink.schema.CqlDdl;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.math.BigDecimal;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StructDataMetadataSupport {
  private static final ImmutableMap<AbstractSchema.Type, GenericType<?>> TYPE_MAP =
      ImmutableMap.<AbstractSchema.Type, GenericType<?>>builder()
          .put(AbstractSchema.Type.BOOLEAN, GenericType.BOOLEAN)
          .put(AbstractSchema.Type.FLOAT64, GenericType.DOUBLE)
          .put(AbstractSchema.Type.INT64, GenericType.LONG)
          .put(AbstractSchema.Type.FLOAT32, GenericType.FLOAT)
          .put(AbstractSchema.Type.INT8, GenericType.BYTE)
          .put(AbstractSchema.Type.INT16, GenericType.SHORT)
          .put(AbstractSchema.Type.INT32, GenericType.INTEGER)
          .put(AbstractSchema.Type.STRING, GenericType.STRING)
          .put(AbstractSchema.Type.BYTES, GenericType.BYTE_BUFFER)
          .build();

  @NonNull
  static GenericType<?> getGenericType(@NonNull AbstractSchema fieldType) {
    // Logical values arrive as BigDecimal/Date; Date and Time are bound as LocalDate/LocalTime
    // (see toJavaValue).
    String logicalName = fieldType.name();
    if (logicalName != null) {
      switch (logicalName) {
        case CqlDdl.DECIMAL:
          return GenericType.of(BigDecimal.class);
        case CqlDdl.DATE:
          return GenericType.LOCAL_DATE;
        case CqlDdl.TIME:
          return GenericType.LOCAL_TIME;
        case CqlDdl.TIMESTAMP:
          return GenericType.of(Date.class);
        default:
          break;
      }
    }
    GenericType<?> result = TYPE_MAP.get(fieldType.type());
    if (result != null) {
      return result;
    }
    // This is a complex type.
    // TODO: PERF: Consider caching these results and check the cache before creating
    // new entries.

    switch (fieldType.type()) {
      case ARRAY:
        return GenericType.listOf(getGenericType(fieldType.valueSchema()));
      case MAP:
        return GenericType.mapOf(
            getGenericType(fieldType.keySchema()), getGenericType(fieldType.valueSchema()));
      case STRUCT:
        return GenericType.of(AbstractStruct.class);
      default:
        throw new IllegalArgumentException(
            String.format("Unrecognized Kafka field type: %s", fieldType.type().getName()));
    }
  }

  /**
   * Connect Date and Time values are java.util.Date instants in UTC. Converting them here keeps the
   * topic's codec time zone from shifting them.
   */
  @Nullable
  static Object toJavaValue(@NonNull AbstractSchema schema, @Nullable Object value) {
    if (value instanceof Date) {
      String logicalName = schema.name();
      if (CqlDdl.DATE.equals(logicalName)) {
        return ((Date) value).toInstant().atOffset(ZoneOffset.UTC).toLocalDate();
      }
      if (CqlDdl.TIME.equals(logicalName)) {
        return ((Date) value).toInstant().atOffset(ZoneOffset.UTC).toLocalTime();
      }
    } else if (value instanceof List && schema.type() == AbstractSchema.Type.ARRAY) {
      AbstractSchema element = schema.valueSchema();
      return ((List<?>) value)
          .stream()
          .map(v -> toJavaValue(element, v))
          .collect(Collectors.toList());
    } else if (value instanceof Map && schema.type() == AbstractSchema.Type.MAP) {
      Map<Object, Object> converted = new LinkedHashMap<>();
      ((Map<?, ?>) value)
          .forEach(
              (k, v) ->
                  converted.put(
                      toJavaValue(schema.keySchema(), k), toJavaValue(schema.valueSchema(), v)));
      return converted;
    }
    return value;
  }
}
