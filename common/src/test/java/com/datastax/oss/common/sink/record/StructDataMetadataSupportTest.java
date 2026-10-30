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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.common.sink.AbstractField;
import com.datastax.oss.common.sink.AbstractSchema;
import com.datastax.oss.common.sink.schema.CqlTypeMapper;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import com.datastax.oss.dsbulk.codecs.api.ConversionContext;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.text.TextConversionContext;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import org.junit.jupiter.api.Test;

class StructDataMetadataSupportTest {

  private static AbstractSchema schema(AbstractSchema.Type type, String name) {
    return new AbstractSchema() {
      @Override
      public AbstractSchema valueSchema() {
        return null;
      }

      @Override
      public AbstractSchema keySchema() {
        return null;
      }

      @Override
      public Type type() {
        return type;
      }

      @Override
      public List<? extends AbstractField> fields() {
        return null;
      }

      @Override
      public AbstractField field(String fieldName) {
        return null;
      }

      @Override
      public String name() {
        return name;
      }
    };
  }

  private static ConvertingCodecFactory codecFactory() {
    ConversionContext context = new TextConversionContext().setTimeZone(ZoneOffset.UTC);
    return new ConvertingCodecFactory(new DefaultCodecRegistry("test"), context);
  }

  @SuppressWarnings("unchecked")
  private static Object convert(DataType cqlType, Object value) {
    GenericType<Object> javaType = (GenericType<Object>) GenericType.of(value.getClass());
    return codecFactory()
        .<Object, Object>createConvertingCodec(cqlType, javaType, true)
        .externalToInternal(value);
  }

  @Test
  void should_map_logical_types_to_their_java_values() {
    assertThat(
            StructDataMetadataSupport.getGenericType(
                schema(AbstractSchema.Type.BYTES, CqlTypeMapper.DECIMAL)))
        .isEqualTo(GenericType.of(BigDecimal.class));
    for (String name :
        new String[] {CqlTypeMapper.DATE, CqlTypeMapper.TIME, CqlTypeMapper.TIMESTAMP}) {
      AbstractSchema.Type physical =
          name.equals(CqlTypeMapper.TIMESTAMP)
              ? AbstractSchema.Type.INT64
              : AbstractSchema.Type.INT32;
      assertThat(StructDataMetadataSupport.getGenericType(schema(physical, name)))
          .isEqualTo(GenericType.of(Date.class));
    }
  }

  @Test
  void should_keep_physical_types_for_plain_schemas() {
    assertThat(StructDataMetadataSupport.getGenericType(schema(AbstractSchema.Type.INT32, null)))
        .isEqualTo(GenericType.INTEGER);
    assertThat(StructDataMetadataSupport.getGenericType(schema(AbstractSchema.Type.INT64, null)))
        .isEqualTo(GenericType.LONG);
    assertThat(StructDataMetadataSupport.getGenericType(schema(AbstractSchema.Type.BYTES, null)))
        .isEqualTo(GenericType.BYTE_BUFFER);
  }

  @Test
  void should_keep_physical_type_for_non_logical_name() {
    assertThat(
            StructDataMetadataSupport.getGenericType(
                schema(AbstractSchema.Type.STRING, "com.acme.Foo")))
        .isEqualTo(GenericType.STRING);
    assertThat(
            StructDataMetadataSupport.getGenericType(
                schema(AbstractSchema.Type.BYTES, "com.acme.Foo")))
        .isEqualTo(GenericType.of(ByteBuffer.class));
  }

  @Test
  void should_convert_date_to_timestamp() {
    Date date = new Date(1_700_000_000_123L);
    assertThat(convert(DataTypes.TIMESTAMP, date))
        .isEqualTo(Instant.ofEpochMilli(1_700_000_000_123L));
  }

  @Test
  void should_convert_date_to_cql_date() {
    Date date = Date.from(LocalDate.of(2024, 3, 15).atStartOfDay().toInstant(ZoneOffset.UTC));
    assertThat(convert(DataTypes.DATE, date)).isEqualTo(LocalDate.of(2024, 3, 15));
  }

  @Test
  void should_convert_date_to_cql_time() {
    Date date = new Date(((13L * 60 + 45) * 60 + 30) * 1000 + 250);
    assertThat(convert(DataTypes.TIME, date)).isEqualTo(LocalTime.of(13, 45, 30, 250_000_000));
  }

  @Test
  void should_convert_big_decimal_to_decimal() {
    BigDecimal value = new BigDecimal("12345.6700");
    Object converted = convert(DataTypes.DECIMAL, value);
    assertThat(converted).isEqualTo(value);
    assertThat(((BigDecimal) converted).scale()).isEqualTo(4);
  }
}
