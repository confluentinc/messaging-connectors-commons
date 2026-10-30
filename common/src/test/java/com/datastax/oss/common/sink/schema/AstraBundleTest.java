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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AstraBundleTest {
  private static final String DB_ID = "0d2f67cc-7a69-4637-b059-0604c05e3766";

  @TempDir Path dir;

  private Path bundle(String configJson) throws IOException {
    Path zip = dir.resolve("scb.zip");
    try (OutputStream out = Files.newOutputStream(zip);
        ZipOutputStream zos = new ZipOutputStream(out)) {
      zos.putNextEntry(new ZipEntry("config.json"));
      zos.write(configJson.getBytes(StandardCharsets.UTF_8));
      zos.closeEntry();
    }
    return zip;
  }

  @Test
  void should_extract_db_id_from_host() throws IOException {
    Path zip = bundle("{\"host\":\"" + DB_ID + "-us-west-2.db.astra.datastax.com\"}");

    assertThat(AstraBundle.databaseId(zip)).isEqualTo(DB_ID);
  }

  @Test
  void should_extract_db_id_from_file_url_location() throws IOException {
    Path zip = bundle("{\"host\":\"" + DB_ID + "-us-west-2.db.astra.datastax.com\"}");

    assertThat(AstraBundle.databaseId(zip.toUri().toString())).isEqualTo(DB_ID);
    assertThat(AstraBundle.databaseId(zip.toString())).isEqualTo(DB_ID);
  }

  @Test
  void should_be_fatal_when_host_has_no_db_id() throws IOException {
    Path zip = bundle("{\"host\":\"localhost\"}");

    assertThatThrownBy(() -> AstraBundle.databaseId(zip))
        .isInstanceOf(FatalSchemaException.class)
        .hasMessageContaining("database id");
  }

  @Test
  void should_not_expose_url_query_string_in_errors() throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          exchange.sendResponseHeaders(404, -1);
          exchange.close();
        });
    server.start();
    try {
      String base = "http://127.0.0.1:" + server.getAddress().getPort() + "/scb.zip";

      assertThatThrownBy(() -> AstraBundle.databaseId(base + "?X-Amz-Signature=secret"))
          .isInstanceOf(FatalSchemaException.class)
          .hasMessageContaining(base)
          .satisfies(
              e -> {
                for (Throwable t = e; t != null; t = t.getCause()) {
                  assertThat(String.valueOf(t.getMessage())).doesNotContain("secret");
                }
              });
    } finally {
      server.stop(0);
    }
  }
}
