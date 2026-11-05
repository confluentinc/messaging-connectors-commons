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

import static com.datastax.oss.common.sink.schema.FakeSchema.prim;
import static com.datastax.oss.common.sink.schema.FakeSchema.struct;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.common.sink.AbstractSchema;
import com.datastax.oss.common.sink.ConfigException;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ImplicitMappingTest {
  private static final AbstractSchema LONG = prim(AbstractSchema.Type.INT64, null);
  private static final AbstractSchema TEXT = prim(AbstractSchema.Type.STRING, null);

  private static CqlIdentifier id(String name) {
    return CqlIdentifier.fromInternal(name);
  }

  @Test
  void should_map_fields_to_lower_case_columns() {
    Map<CqlIdentifier, CqlIdentifier> mapping =
        ImplicitMapping.forSchemas(struct("Id", LONG), struct("name", TEXT, "emailAddr", TEXT));

    assertThat(mapping)
        .containsExactly(
            entry(id("id"), id("key.Id")),
            entry(id("name"), id("value.name")),
            entry(id("emailaddr"), id("value.emailAddr")));
  }

  @Test
  void should_source_overlapping_field_from_key() {
    Map<CqlIdentifier, CqlIdentifier> mapping =
        ImplicitMapping.forSchemas(struct("id", LONG), struct("ID", LONG, "name", TEXT));

    assertThat(mapping)
        .containsExactly(entry(id("id"), id("key.id")), entry(id("name"), id("value.name")));
  }

  @Test
  void should_reject_fields_differing_only_in_case() {
    assertThatThrownBy(
            () ->
                ImplicitMapping.forSchemas(
                    struct("id", LONG), struct("userid", TEXT, "userId", TEXT)))
        .isInstanceOf(ConfigException.class)
        .hasMessage("Fields userid and userId both map to column userid");
  }

  @Test
  void should_map_only_value_fields_when_key_is_not_a_struct() {
    assertThat(ImplicitMapping.forSchemas(LONG, struct("id", LONG)))
        .containsExactly(entry(id("id"), id("value.id")));
    assertThat(ImplicitMapping.forSchemas(null, struct("id", LONG)))
        .containsExactly(entry(id("id"), id("value.id")));
  }

  private static TableMetadata table(String... columns) {
    TableMetadata table = mock(TableMetadata.class);
    Map<CqlIdentifier, ColumnMetadata> byName = new LinkedHashMap<>();
    for (String column : columns) {
      ColumnMetadata col = mock(ColumnMetadata.class);
      byName.put(id(column), col);
      when(table.getColumn(id(column))).thenReturn(Optional.of(col));
    }
    when(table.getColumns()).thenReturn(byName);
    return table;
  }

  @Test
  void should_prefer_same_named_column() {
    Map<CqlIdentifier, CqlIdentifier> mapping =
        ImplicitMapping.forSchemas(struct("id", LONG), struct("Email", TEXT));

    assertThat(ImplicitMapping.resolveColumns(mapping, table("id", "Email", "email"), "shop.t"))
        .containsExactly(entry(id("id"), id("key.id")), entry(id("email"), id("value.Email")));
  }

  @Test
  void should_resolve_unique_case_insensitive_match() {
    Map<CqlIdentifier, CqlIdentifier> mapping =
        ImplicitMapping.forSchemas(struct("id", LONG), struct("userId", TEXT, "other", TEXT));

    assertThat(ImplicitMapping.resolveColumns(mapping, table("id", "UserId"), "shop.t"))
        .containsExactly(
            entry(id("id"), id("key.id")),
            entry(id("UserId"), id("value.userId")),
            entry(id("other"), id("value.other")));
  }

  @Test
  void should_reject_ambiguous_case_insensitive_match() {
    Map<CqlIdentifier, CqlIdentifier> mapping =
        ImplicitMapping.forSchemas(struct("id", LONG), struct("userId", TEXT));

    assertThatThrownBy(
            () ->
                ImplicitMapping.resolveColumns(mapping, table("id", "UserId", "USERID"), "shop.t"))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Field userId")
        .hasMessageContaining("shop.t")
        .hasMessageContaining("\"UserId\", \"USERID\"");
  }
}
