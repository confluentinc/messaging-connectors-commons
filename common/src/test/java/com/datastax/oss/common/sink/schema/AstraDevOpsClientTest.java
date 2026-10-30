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
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AstraDevOpsClientTest {
  private static final String TOKEN = "tok";

  private HttpServer server;
  private final AtomicReference<String> method = new AtomicReference<>();
  private final AtomicReference<String> path = new AtomicReference<>();
  private final AtomicReference<String> auth = new AtomicReference<>();
  private volatile int status = 201;
  private volatile String body = "";

  @BeforeEach
  void setUp() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          method.set(exchange.getRequestMethod());
          path.set(exchange.getRequestURI().getPath());
          auth.set(exchange.getRequestHeaders().getFirst("Authorization"));
          byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
          if (bytes.length > 0) {
            exchange.getResponseBody().write(bytes);
          }
          exchange.close();
        });
    server.start();
  }

  @AfterEach
  void tearDown() {
    server.stop(0);
  }

  private AstraDevOpsClient client() {
    return new AstraDevOpsClient("http://127.0.0.1:" + server.getAddress().getPort(), TOKEN, 2000);
  }

  @Test
  void should_post_keyspace_with_bearer_token() {
    client().createKeyspace("db1", "shop");

    assertThat(method.get()).isEqualTo("POST");
    assertThat(path.get()).isEqualTo("/v2/databases/db1/keyspaces/shop");
    assertThat(auth.get()).isEqualTo("Bearer tok");
  }

  @Test
  void should_treat_conflict_as_already_exists() {
    status = 409;

    client().createKeyspace("db1", "shop");
  }

  @Test
  void should_be_fatal_on_401_and_403_without_leaking_token() {
    for (int code : new int[] {401, 403}) {
      status = code;
      assertThatThrownBy(() -> client().createKeyspace("db1", "shop"))
          .isInstanceOf(FatalSchemaException.class)
          .hasMessageContaining("Database Administrator")
          .satisfies(e -> assertThat(e.getMessage()).doesNotContain(TOKEN));
    }
  }

  @Test
  void should_be_fatal_on_other_error_status() {
    status = 500;

    assertThatThrownBy(() -> client().createKeyspace("db1", "shop"))
        .isInstanceOf(FatalSchemaException.class)
        .hasMessageContaining("500")
        .satisfies(e -> assertThat(e.getMessage()).doesNotContain(TOKEN));
  }

  @Test
  void should_be_fatal_mentioning_private_link_when_unreachable() {
    int port = server.getAddress().getPort();
    server.stop(0);
    AstraDevOpsClient closed = new AstraDevOpsClient("http://127.0.0.1:" + port, TOKEN, 500);

    assertThatThrownBy(() -> closed.createKeyspace("db1", "shop"))
        .isInstanceOf(FatalSchemaException.class)
        .hasMessageContaining("PrivateLink")
        .satisfies(e -> assertThat(e.getMessage()).doesNotContain(TOKEN));
  }

  @Test
  void should_read_database_status() {
    status = 200;
    body = "{\"id\":\"db1\",\"status\":\"MAINTENANCE\"}";

    assertThat(client().getDatabaseStatus("db1")).isEqualTo("MAINTENANCE");
    assertThat(method.get()).isEqualTo("GET");
    assertThat(path.get()).isEqualTo("/v2/databases/db1");
  }
}
