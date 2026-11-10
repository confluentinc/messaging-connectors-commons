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
import com.datastax.oss.common.sink.schema.CqlDdl;
import com.datastax.oss.common.sink.schema.FakeSchema;
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
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collections;
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

  private static ConvertingCodecFactory codecFactory(ZoneId timeZone) {
    ConversionContext context = new TextConversionContext().setTimeZone(timeZone);
    return new ConvertingCodecFactory(new DefaultCodecRegistry("test"), context);
  }

  private static Object convert(DataType cqlType, Object value) {
    return convert(cqlType, value, ZoneOffset.UTC);
  }

  @SuppressWarnings("unchecked")
  private static Object convert(DataType cqlType, Object value, ZoneId timeZone) {
    GenericType<Object> javaType = (GenericType<Object>) GenericType.of(value.getClass());
    return codecFactory(timeZone)
        .<Object, Object>createConvertingCodec(cqlType, javaType, true)
        .externalToInternal(value);
  }

  @Test
  void should_map_logical_types_to_their_java_values() {
    assertThat(
            StructDataMetadataSupport.getGenericType(
                schema(AbstractSchema.Type.BYTES, CqlDdl.DECIMAL)))
        .isEqualTo(GenericType.of(BigDecimal.class));
    assertThat(
            StructDataMetadataSupport.getGenericType(
                schema(AbstractSchema.Type.INT32, CqlDdl.DATE)))
        .isEqualTo(GenericType.LOCAL_DATE);
    assertThat(
            StructDataMetadataSupport.getGenericType(
                schema(AbstractSchema.Type.INT32, CqlDdl.TIME)))
        .isEqualTo(GenericType.LOCAL_TIME);
    assertThat(
            StructDataMetadataSupport.getGenericType(
                schema(AbstractSchema.Type.INT64, CqlDdl.TIMESTAMP)))
        .isEqualTo(GenericType.of(Date.class));
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
  void should_bind_connect_date_and_time_independently_of_the_codec_time_zone() {
    Date midnight = Date.from(LocalDate.of(2026, 11, 10).atStartOfDay().toInstant(ZoneOffset.UTC));
    Date oneAm = new Date(3_600_000L);
    Object day =
        StructDataMetadataSupport.toJavaValue(
            schema(AbstractSchema.Type.INT32, CqlDdl.DATE), midnight);
    Object time =
        StructDataMetadataSupport.toJavaValue(
            schema(AbstractSchema.Type.INT32, CqlDdl.TIME), oneAm);
    assertThat(day).isEqualTo(LocalDate.of(2026, 11, 10));
    assertThat(time).isEqualTo(LocalTime.of(1, 0));

    ZoneId newYork = ZoneId.of("America/New_York");
    // Binding the raw Date would shift it into the codec time zone.
    assertThat(convert(DataTypes.DATE, midnight, newYork)).isEqualTo(LocalDate.of(2026, 11, 9));
    assertThat(convert(DataTypes.DATE, day, newYork)).isEqualTo(LocalDate.of(2026, 11, 10));
    assertThat(convert(DataTypes.TIME, time, newYork)).isEqualTo(LocalTime.of(1, 0));
  }

  @Test
  void should_convert_dates_inside_collections() {
    Date midnight = Date.from(LocalDate.of(2024, 3, 15).atStartOfDay().toInstant(ZoneOffset.UTC));
    AbstractSchema date = FakeSchema.prim(AbstractSchema.Type.INT32, CqlDdl.DATE);
    assertThat(
            StructDataMetadataSupport.toJavaValue(
                FakeSchema.array(date), Collections.singletonList(midnight)))
        .isEqualTo(Collections.singletonList(LocalDate.of(2024, 3, 15)));
    assertThat(
            StructDataMetadataSupport.toJavaValue(
                FakeSchema.map(FakeSchema.prim(AbstractSchema.Type.STRING, null), date),
                Collections.singletonMap("d", midnight)))
        .isEqualTo(Collections.singletonMap("d", LocalDate.of(2024, 3, 15)));
  }

  @Test
  void should_leave_other_values_unchanged() {
    Date instant = new Date(1_700_000_000_123L);
    assertThat(
            StructDataMetadataSupport.toJavaValue(
                schema(AbstractSchema.Type.INT64, CqlDdl.TIMESTAMP), instant))
        .isSameAs(instant);
    assertThat(StructDataMetadataSupport.toJavaValue(schema(AbstractSchema.Type.INT32, null), 7))
        .isEqualTo(7);
    assertThat(
            StructDataMetadataSupport.toJavaValue(
                schema(AbstractSchema.Type.INT32, CqlDdl.DATE), null))
        .isNull();
  }

  @Test
  void should_convert_big_decimal_to_decimal() {
    BigDecimal value = new BigDecimal("12345.6700");
    Object converted = convert(DataTypes.DECIMAL, value);
    assertThat(converted).isEqualTo(value);
    assertThat(((BigDecimal) converted).scale()).isEqualTo(4);
  }
}
