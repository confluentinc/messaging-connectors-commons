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

import com.datastax.oss.common.sink.AbstractField;
import com.datastax.oss.common.sink.AbstractSchema;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Minimal in-memory schema for tests. */
public class FakeSchema implements AbstractSchema {

  private final Type type;
  private final String name;
  private final AbstractSchema keySchema;
  private final AbstractSchema valueSchema;
  private final List<AbstractField> fields;

  private FakeSchema(
      Type type,
      String name,
      AbstractSchema keySchema,
      AbstractSchema valueSchema,
      List<AbstractField> fields) {
    this.type = type;
    this.name = name;
    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
    this.fields = fields;
  }

  public static FakeSchema prim(Type type, String logicalName) {
    return new FakeSchema(type, logicalName, null, null, Collections.emptyList());
  }

  public static FakeSchema array(AbstractSchema element) {
    return new FakeSchema(Type.ARRAY, null, null, element, Collections.emptyList());
  }

  public static FakeSchema map(AbstractSchema key, AbstractSchema value) {
    return new FakeSchema(Type.MAP, null, key, value, Collections.emptyList());
  }

  /** Alternating field names and schemas, e.g. struct("a", prim(...), "b", prim(...)). */
  public static FakeSchema struct(Object... nameSchemaPairs) {
    List<AbstractField> fields = new ArrayList<>();
    for (int i = 0; i < nameSchemaPairs.length; i += 2) {
      String fieldName = (String) nameSchemaPairs[i];
      AbstractSchema fieldSchema = (AbstractSchema) nameSchemaPairs[i + 1];
      fields.add(
          new AbstractField() {
            @Override
            public String name() {
              return fieldName;
            }

            @Override
            public AbstractSchema schema() {
              return fieldSchema;
            }
          });
    }
    return new FakeSchema(Type.STRUCT, null, null, null, fields);
  }

  @Override
  public AbstractSchema valueSchema() {
    return valueSchema;
  }

  @Override
  public AbstractSchema keySchema() {
    return keySchema;
  }

  @Override
  public Type type() {
    return type;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public List<? extends AbstractField> fields() {
    return fields;
  }

  @Override
  public AbstractField field(String fieldName) {
    return fields.stream().filter(f -> f.name().equals(fieldName)).findAny().orElse(null);
  }
}
