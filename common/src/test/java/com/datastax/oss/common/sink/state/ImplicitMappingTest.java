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
import static org.assertj.core.api.Assertions.entry;

import com.datastax.oss.common.sink.AbstractSchema;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ImplicitMappingTest {
  private static final AbstractSchema LONG = prim(AbstractSchema.Type.INT64, null);
  private static final AbstractSchema TEXT = prim(AbstractSchema.Type.STRING, null);

  private static CqlIdentifier id(String name) {
    return CqlIdentifier.fromInternal(name);
  }

  @Test
  void should_map_value_and_key_fields_by_name() {
    Map<CqlIdentifier, CqlIdentifier> mapping =
        ImplicitMapping.forSchemas(struct("id", LONG), struct("name", TEXT, "Email", TEXT));

    assertThat(mapping)
        .containsExactly(
            entry(id("id"), id("key.id")),
            entry(id("name"), id("value.name")),
            entry(id("Email"), id("value.Email")));
  }

  @Test
  void should_source_overlapping_field_from_key() {
    Map<CqlIdentifier, CqlIdentifier> mapping =
        ImplicitMapping.forSchemas(struct("id", LONG), struct("id", LONG, "name", TEXT));

    assertThat(mapping)
        .containsExactly(entry(id("id"), id("key.id")), entry(id("name"), id("value.name")));
  }

  @Test
  void should_map_only_value_fields_when_key_is_not_a_struct() {
    assertThat(ImplicitMapping.forSchemas(LONG, struct("id", LONG)))
        .containsExactly(entry(id("id"), id("value.id")));
    assertThat(ImplicitMapping.forSchemas(null, struct("id", LONG)))
        .containsExactly(entry(id("id"), id("value.id")));
  }
}
