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
package com.datastax.oss.common.sink.state;

import com.datastax.oss.common.sink.AbstractField;
import com.datastax.oss.common.sink.AbstractSchema;
import com.datastax.oss.common.sink.ConfigException;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Derives a column mapping from record field names: column = field name. */
public final class ImplicitMapping {

  private ImplicitMapping() {}

  /** Maps key fields to key.f and the remaining value fields to value.f, keys first. */
  public static Map<CqlIdentifier, CqlIdentifier> forSchemas(
      @Nullable AbstractSchema keySchema, @Nullable AbstractSchema valueSchema) {
    Map<CqlIdentifier, CqlIdentifier> mapping = new LinkedHashMap<>();
    addFields(mapping, keySchema, "key.");
    addFields(mapping, valueSchema, "value.");
    return mapping;
  }

  private static void addFields(
      Map<CqlIdentifier, CqlIdentifier> mapping, AbstractSchema schema, String prefix) {
    if (schema == null || schema.type() != AbstractSchema.Type.STRUCT) {
      return;
    }
    for (AbstractField field : schema.fields()) {
      mapping.putIfAbsent(
          CqlIdentifier.fromInternal(field.name()),
          CqlIdentifier.fromInternal(prefix + field.name()));
    }
  }

  /** Binds each column to the table's exact column, else to its only case-insensitive match. */
  public static Map<CqlIdentifier, CqlIdentifier> resolveColumns(
      Map<CqlIdentifier, CqlIdentifier> mapping, TableMetadata table, String tableName) {
    Set<CqlIdentifier> taken = new HashSet<>();
    mapping.keySet().stream().filter(c -> table.getColumn(c).isPresent()).forEach(taken::add);
    Map<CqlIdentifier, CqlIdentifier> resolved = new LinkedHashMap<>();
    for (Map.Entry<CqlIdentifier, CqlIdentifier> entry : mapping.entrySet()) {
      CqlIdentifier column = entry.getKey();
      if (!taken.contains(column)) {
        List<CqlIdentifier> matches =
            table
                .getColumns()
                .keySet()
                .stream()
                .filter(c -> c.asInternal().equalsIgnoreCase(column.asInternal()))
                .collect(Collectors.toList());
        if (matches.size() > 1) {
          throw new ConfigException(
              String.format(
                  "Field %s matches several columns of table %s ignoring case: %s",
                  column.asInternal(),
                  tableName,
                  matches.stream().map(c -> c.asCql(true)).collect(Collectors.joining(", "))));
        }
        if (matches.size() == 1 && taken.add(matches.get(0))) {
          resolved.put(matches.get(0), entry.getValue());
          continue;
        }
      }
      resolved.put(column, entry.getValue());
    }
    return resolved;
  }
}
