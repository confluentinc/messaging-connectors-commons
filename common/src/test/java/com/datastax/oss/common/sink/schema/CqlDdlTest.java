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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import java.util.LinkedHashMap;
import org.junit.jupiter.api.Test;

class CqlDdlTest {
  private static final CqlIdentifier SHOP = CqlIdentifier.fromInternal("shop");
  private static final CqlIdentifier ORDERS = CqlIdentifier.fromInternal("orders");

  private static CqlIdentifier id(String name) {
    return CqlIdentifier.fromInternal(name);
  }

  @Test
  void should_render_create_table_with_single_partition_key() {
    LinkedHashMap<CqlIdentifier, DataType> pk = new LinkedHashMap<>();
    pk.put(id("id"), DataTypes.BIGINT);
    LinkedHashMap<CqlIdentifier, DataType> cols = new LinkedHashMap<>();
    cols.put(id("name"), DataTypes.TEXT);
    cols.put(id("ts"), DataTypes.TIMESTAMP);

    assertThat(CqlDdl.createTable(SHOP, ORDERS, pk, cols))
        .isEqualTo(
            "CREATE TABLE IF NOT EXISTS shop.orders (id bigint,name text,ts timestamp,PRIMARY KEY(id))");
  }

  @Test
  void should_render_create_table_with_composite_partition_key_and_quoting() {
    LinkedHashMap<CqlIdentifier, DataType> pk = new LinkedHashMap<>();
    pk.put(id("a"), DataTypes.INT);
    pk.put(id("B"), DataTypes.INT);
    LinkedHashMap<CqlIdentifier, DataType> cols = new LinkedHashMap<>();
    cols.put(id("tags"), DataTypes.listOf(DataTypes.TEXT));

    assertThat(CqlDdl.createTable(SHOP, id("Orders-v1"), pk, cols))
        .isEqualTo(
            "CREATE TABLE IF NOT EXISTS shop.\"Orders-v1\" (a int,\"B\" int,tags list<text>,PRIMARY KEY((a,\"B\")))");
  }

  @Test
  void should_freeze_collection_partition_key_columns() {
    LinkedHashMap<CqlIdentifier, DataType> pk = new LinkedHashMap<>();
    pk.put(id("ids"), DataTypes.listOf(DataTypes.INT));
    pk.put(id("attrs"), DataTypes.mapOf(DataTypes.TEXT, DataTypes.frozenListOf(DataTypes.INT)));
    LinkedHashMap<CqlIdentifier, DataType> cols = new LinkedHashMap<>();
    cols.put(id("tags"), DataTypes.listOf(DataTypes.frozenListOf(DataTypes.TEXT)));

    assertThat(CqlDdl.createTable(SHOP, ORDERS, pk, cols))
        .isEqualTo(
            "CREATE TABLE IF NOT EXISTS shop.orders (ids frozen<list<int>>,"
                + "attrs frozen<map<text, frozen<list<int>>>>,tags list<frozen<list<text>>>,"
                + "PRIMARY KEY((ids,attrs)))");
  }

  @Test
  void should_render_add_column() {
    assertThat(
            CqlDdl.addColumn(
                SHOP, ORDERS, id("attrs"), DataTypes.mapOf(DataTypes.TEXT, DataTypes.INT)))
        .isEqualTo("ALTER TABLE shop.orders ADD attrs map<text, int>");
  }

  @Test
  void should_render_create_keyspace() {
    assertThat(CqlDdl.createKeyspace(SHOP, 3))
        .isEqualTo(
            "CREATE KEYSPACE IF NOT EXISTS shop WITH replication={'class':'SimpleStrategy','replication_factor':3}");
  }
}
