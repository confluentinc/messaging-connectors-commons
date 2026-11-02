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

import static com.datastax.oss.common.sink.AbstractSchema.Type.*;
import static com.datastax.oss.common.sink.schema.FakeSchema.array;
import static com.datastax.oss.common.sink.schema.FakeSchema.map;
import static com.datastax.oss.common.sink.schema.FakeSchema.prim;
import static com.datastax.oss.common.sink.schema.FakeSchema.struct;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.common.sink.AbstractSchema;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CqlTypeMapperTest {

  private static final String DECIMAL = "org.apache.kafka.connect.data.Decimal";
  private static final String DATE = "org.apache.kafka.connect.data.Date";
  private static final String TIME = "org.apache.kafka.connect.data.Time";
  private static final String TIMESTAMP = "org.apache.kafka.connect.data.Timestamp";

  static Stream<Arguments> mappings() {
    return Stream.of(
        Arguments.of(prim(BYTES, DECIMAL), DataTypes.DECIMAL),
        Arguments.of(prim(INT32, DATE), DataTypes.DATE),
        Arguments.of(prim(INT32, TIME), DataTypes.TIME),
        Arguments.of(prim(INT64, TIMESTAMP), DataTypes.TIMESTAMP),
        Arguments.of(prim(INT8, null), DataTypes.TINYINT),
        Arguments.of(prim(INT16, null), DataTypes.SMALLINT),
        Arguments.of(prim(INT32, null), DataTypes.INT),
        Arguments.of(prim(INT64, null), DataTypes.BIGINT),
        Arguments.of(prim(FLOAT32, null), DataTypes.FLOAT),
        Arguments.of(prim(FLOAT64, null), DataTypes.DOUBLE),
        Arguments.of(prim(BOOLEAN, null), DataTypes.BOOLEAN),
        Arguments.of(prim(STRING, null), DataTypes.TEXT),
        Arguments.of(prim(BYTES, null), DataTypes.BLOB),
        Arguments.of(prim(STRING, "some.other.Name"), DataTypes.TEXT),
        Arguments.of(array(prim(STRING, null)), DataTypes.listOf(DataTypes.TEXT)),
        Arguments.of(
            array(array(prim(INT32, DATE))),
            DataTypes.listOf(DataTypes.frozenListOf(DataTypes.DATE))),
        Arguments.of(
            map(prim(STRING, null), array(prim(INT32, null))),
            DataTypes.mapOf(DataTypes.TEXT, DataTypes.frozenListOf(DataTypes.INT))),
        Arguments.of(
            array(map(prim(STRING, null), array(prim(INT32, null)))),
            DataTypes.listOf(
                DataTypes.frozenMapOf(DataTypes.TEXT, DataTypes.frozenListOf(DataTypes.INT)))));
  }

  @ParameterizedTest
  @MethodSource("mappings")
  void should_map_connect_type_to_cql(AbstractSchema schema, DataType expected) {
    assertThat(CqlTypeMapper.toCql(schema).asCql(true, true)).isEqualTo(expected.asCql(true, true));
  }

  @Test
  void should_reject_struct() {
    assertThatThrownBy(() -> CqlTypeMapper.toCql(struct()))
        .isInstanceOf(SchemaMismatchException.class)
        .hasMessageContaining("STRUCT");
  }

  @Test
  void should_reject_nested_struct() {
    assertThatThrownBy(() -> CqlTypeMapper.toCql(array(struct("a", prim(INT32, null)))))
        .isInstanceOf(SchemaMismatchException.class)
        .hasMessageContaining("STRUCT");
  }

  @Test
  void exceptions_should_keep_message_and_cause() {
    Throwable cause = new RuntimeException("c");
    assertThat(new SchemaMismatchException("m", cause)).hasMessage("m").hasCause(cause);
    assertThat(new FatalSchemaException("f", cause)).hasMessage("f").hasCause(cause);
    assertThat(new FatalSchemaException("f")).hasMessage("f");
  }
}
