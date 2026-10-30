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
package com.datastax.oss.common.sink;

import static com.datastax.oss.common.sink.schema.FakeSchema.prim;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import org.junit.jupiter.api.Test;

class AbstractSinkRecordTest {

  private static AbstractSinkRecord record(AbstractSchema key, AbstractSchema value) {
    return new AbstractSinkRecord() {
      @Override
      public Iterable<AbstractSinkRecordHeader> headers() {
        return Collections.emptyList();
      }

      @Override
      public Object key() {
        return null;
      }

      @Override
      public Object value() {
        return null;
      }

      @Override
      public Long timestamp() {
        return null;
      }

      @Override
      public String topic() {
        return "t";
      }

      @Override
      public AbstractSchema keySchema() {
        return key;
      }

      @Override
      public AbstractSchema valueSchema() {
        return value;
      }
    };
  }

  @Test
  void should_derive_schema_identities_from_schemas() {
    AbstractSchema key = prim(AbstractSchema.Type.INT64, null);
    AbstractSchema value = prim(AbstractSchema.Type.STRING, null);

    AbstractSinkRecord record = record(key, value);

    assertThat(record.keySchemaIdentity()).isSameAs(key.schemaIdentity());
    assertThat(record.valueSchemaIdentity()).isSameAs(value.schemaIdentity());
    assertThat(record(null, null).keySchemaIdentity()).isNull();
    assertThat(record(null, null).valueSchemaIdentity()).isNull();
  }
}
