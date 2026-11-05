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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/** Derives a column mapping from record field names: column = lower-cased field name. */
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
      CqlIdentifier column = CqlIdentifier.fromInternal(field.name().toLowerCase(Locale.ROOT));
      CqlIdentifier previous = mapping.get(column);
      if (previous == null) {
        mapping.put(column, CqlIdentifier.fromInternal(prefix + field.name()));
      } else if (previous.asInternal().startsWith(prefix)) {
        throw new ConfigException(
            String.format(
                "Fields %s and %s both map to column %s",
                previous.asInternal().substring(prefix.length()),
                field.name(),
                column.asCql(true)));
      }
      // Otherwise a key field already provides this column.
    }
  }

  /**
   * Binds each column to the table's same-named column, else to its only case-insensitive match.
   */
  public static Map<CqlIdentifier, CqlIdentifier> resolveColumns(
      Map<CqlIdentifier, CqlIdentifier> mapping, TableMetadata table, String tableName) {
    Map<CqlIdentifier, CqlIdentifier> resolved = new LinkedHashMap<>();
    mapping.forEach(
        (column, source) -> resolved.put(resolveColumn(column, source, table, tableName), source));
    return resolved;
  }

  private static CqlIdentifier resolveColumn(
      CqlIdentifier column, CqlIdentifier source, TableMetadata table, String tableName) {
    if (table.getColumn(column).isPresent()) {
      return column;
    }
    List<CqlIdentifier> matches =
        table
            .getColumns()
            .keySet()
            .stream()
            .filter(c -> c.asInternal().equalsIgnoreCase(column.asInternal()))
            .collect(Collectors.toList());
    if (matches.size() > 1) {
      String field = source.asInternal().substring(source.asInternal().indexOf('.') + 1);
      throw new ConfigException(
          String.format(
              "Field %s matches several columns of table %s ignoring case: %s",
              field,
              tableName,
              matches.stream().map(c -> c.asCql(true)).collect(Collectors.joining(", "))));
    }
    return matches.isEmpty() ? column : matches.get(0);
  }
}
