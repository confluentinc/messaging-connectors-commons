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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.common.sink.schema.AstraKeyspaceCreator.DevOpsClient;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;

class AstraKeyspaceCreatorTest {

  @Nested
  class EnsureKeyspace {
    private final CqlIdentifier shop = CqlIdentifier.fromInternal("shop");

    private DevOpsClient client;
    private CqlSession session;
    private Metadata metadata;

    @BeforeEach
    void setUp() {
      client = mock(DevOpsClient.class);
      session = mock(CqlSession.class);
      metadata = mock(Metadata.class);
      when(session.getMetadata()).thenReturn(metadata);
    }

    @Test
    void should_post_then_poll_until_active() {
      when(metadata.getKeyspace(shop))
          .thenReturn(
              Optional.empty(), Optional.empty(), Optional.of(mock(KeyspaceMetadata.class)));
      when(client.getDatabaseStatus("db1")).thenReturn("MAINTENANCE", "MAINTENANCE", "ACTIVE");

      new AstraKeyspaceCreator(client, "db1", 1, 5000).ensureKeyspace(session, shop);

      InOrder order = inOrder(client);
      order.verify(client).createKeyspace("db1", "shop");
      order.verify(client, times(3)).getDatabaseStatus("db1");
      verify(session, atLeastOnce()).refreshSchema();
    }

    @Test
    void should_skip_when_keyspace_present() {
      when(metadata.getKeyspace(shop)).thenReturn(Optional.of(mock(KeyspaceMetadata.class)));

      new AstraKeyspaceCreator(client, "db1", 1, 5000).ensureKeyspace(session, shop);

      verify(client, never()).createKeyspace(any(), any());
    }

    @Test
    void should_be_fatal_when_database_never_active() {
      when(metadata.getKeyspace(shop)).thenReturn(Optional.empty());
      when(client.getDatabaseStatus("db1")).thenReturn("MAINTENANCE");

      assertThatThrownBy(
              () -> new AstraKeyspaceCreator(client, "db1", 1, 30).ensureKeyspace(session, shop))
          .isInstanceOf(FatalSchemaException.class)
          .hasMessageContaining("ACTIVE");
    }

    @Test
    void should_be_fatal_when_keyspace_never_visible() {
      when(metadata.getKeyspace(shop)).thenReturn(Optional.empty());
      when(client.getDatabaseStatus("db1")).thenReturn("ACTIVE");

      assertThatThrownBy(
              () -> new AstraKeyspaceCreator(client, "db1", 1, 30).ensureKeyspace(session, shop))
          .isInstanceOf(FatalSchemaException.class)
          .hasMessageContaining("shop");
    }

    @Test
    void should_wait_for_keyspace_within_full_budget_after_early_active() {
      long visibleAt = System.nanoTime() + 200_000_000L;
      when(metadata.getKeyspace(shop))
          .thenAnswer(
              i ->
                  System.nanoTime() > visibleAt
                      ? Optional.of(mock(KeyspaceMetadata.class))
                      : Optional.empty());
      when(client.getDatabaseStatus("db1")).thenReturn("ACTIVE");

      new AstraKeyspaceCreator(client, "db1", 5, 5000).ensureKeyspace(session, shop);

      verify(client, times(1)).getDatabaseStatus("db1");
    }

    @Test
    void should_retry_transient_status_failures_and_reset_after_success() {
      when(metadata.getKeyspace(shop))
          .thenReturn(
              Optional.empty(), Optional.empty(), Optional.of(mock(KeyspaceMetadata.class)));
      when(client.getDatabaseStatus("db1"))
          .thenThrow(transientFailure())
          .thenReturn("MAINTENANCE")
          .thenThrow(transientFailure(), transientFailure(), transientFailure())
          .thenReturn("ACTIVE");

      new AstraKeyspaceCreator(client, "db1", 1, 5000).ensureKeyspace(session, shop);

      verify(client, times(6)).getDatabaseStatus("db1");
    }

    @Test
    void should_be_fatal_after_too_many_consecutive_transient_failures() {
      when(metadata.getKeyspace(shop)).thenReturn(Optional.empty());
      when(client.getDatabaseStatus("db1")).thenThrow(transientFailure());

      assertThatThrownBy(
              () -> new AstraKeyspaceCreator(client, "db1", 1, 5000).ensureKeyspace(session, shop))
          .isInstanceOf(FatalSchemaException.class)
          .hasMessage(
              "Astra DevOps API failed to read the status of database db1 (HTTP 503) after 4 attempts; restart the connector to retry");
      verify(client, times(4)).getDatabaseStatus("db1");
    }

    @Test
    void should_be_fatal_immediately_on_non_transient_status_failure() {
      when(metadata.getKeyspace(shop)).thenReturn(Optional.empty());
      when(client.getDatabaseStatus("db1"))
          .thenThrow(new FatalSchemaException("Astra DevOps API failed (HTTP 404)"));

      assertThatThrownBy(
              () -> new AstraKeyspaceCreator(client, "db1", 1, 5000).ensureKeyspace(session, shop))
          .isInstanceOf(FatalSchemaException.class)
          .hasMessage("Astra DevOps API failed (HTTP 404)");
      verify(client, times(1)).getDatabaseStatus("db1");
    }

    private AstraKeyspaceCreator.TransientApiException transientFailure() {
      return new AstraKeyspaceCreator.TransientApiException(
          "Astra DevOps API failed to read the status of database db1 (HTTP 503)", null);
    }
  }

  @Nested
  class SecureConnectBundle {
    private final String dbId = "11111111-2222-3333-4444-555555555555";

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
      Path zip = bundle("{\"host\":\"" + dbId + "-us-west-2.db.astra.datastax.com\"}");

      assertThat(AstraKeyspaceCreator.databaseId(zip)).isEqualTo(dbId);
    }

    @Test
    void should_extract_db_id_from_file_url_location() throws IOException {
      Path zip = bundle("{\"host\":\"" + dbId + "-us-west-2.db.astra.datastax.com\"}");

      assertThat(AstraKeyspaceCreator.databaseId(zip.toUri().toString())).isEqualTo(dbId);
      assertThat(AstraKeyspaceCreator.databaseId(zip.toString())).isEqualTo(dbId);
    }

    @Test
    void should_be_fatal_when_host_has_no_db_id() throws IOException {
      Path zip = bundle("{\"host\":\"localhost\"}");

      assertThatThrownBy(() -> AstraKeyspaceCreator.databaseId(zip))
          .isInstanceOf(FatalSchemaException.class)
          .hasMessageContaining("database id");
    }

    @Test
    void should_not_expose_invalid_config_json_in_errors() throws IOException {
      Path zip = bundle("{\"host\":\"x\",\"keyStorePassword\": canary-secret}");

      assertThatThrownBy(() -> AstraKeyspaceCreator.databaseId(zip))
          .isInstanceOf(FatalSchemaException.class)
          .hasMessageContaining("not valid JSON")
          .satisfies(
              e -> {
                for (Throwable t = e; t != null; t = t.getCause()) {
                  assertThat(String.valueOf(t.getMessage())).doesNotContain("canary-secret");
                }
              });
    }

    @Test
    void should_not_expose_url_user_info_in_errors() throws IOException {
      HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      server.createContext(
          "/",
          exchange -> {
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
          });
      server.start();
      try {
        int port = server.getAddress().getPort();
        String location = "http://user:secret@127.0.0.1:" + port + "/scb.zip";

        assertThatThrownBy(() -> AstraKeyspaceCreator.databaseId(location))
            .isInstanceOf(FatalSchemaException.class)
            .hasMessageContaining("http://127.0.0.1:" + port + "/scb.zip")
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

        assertThatThrownBy(() -> AstraKeyspaceCreator.databaseId(base + "?X-Amz-Signature=secret"))
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

  @Nested
  class DevOpsApi {
    private final String token = "tok";

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

    private DevOpsClient client() {
      return new DevOpsClient("http://127.0.0.1:" + server.getAddress().getPort(), token, 2000);
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
            .satisfies(e -> assertThat(e.getMessage()).doesNotContain(token));
      }
    }

    @Test
    void should_be_fatal_on_other_error_status() {
      status = 500;

      assertThatThrownBy(() -> client().createKeyspace("db1", "shop"))
          .isInstanceOf(FatalSchemaException.class)
          .hasMessageContaining("500")
          .satisfies(e -> assertThat(e.getMessage()).doesNotContain(token));
    }

    @Test
    void should_be_fatal_with_cause_and_hint_when_unreachable() {
      int port = server.getAddress().getPort();
      server.stop(0);
      DevOpsClient closed = new DevOpsClient("http://127.0.0.1:" + port, token, 500);

      assertThatThrownBy(() -> closed.createKeyspace("db1", "shop"))
          .isInstanceOf(FatalSchemaException.class)
          .hasMessageContaining("Cannot reach the Astra DevOps API")
          .hasMessageContaining("ConnectException")
          .hasMessageContaining("network access")
          .satisfies(e -> assertThat(e.getMessage()).doesNotContain(token));
    }

    @Test
    void should_read_database_status() {
      status = 200;
      body = "{\"id\":\"db1\",\"status\":\"MAINTENANCE\"}";

      assertThat(client().getDatabaseStatus("db1")).isEqualTo("MAINTENANCE");
      assertThat(method.get()).isEqualTo("GET");
      assertThat(path.get()).isEqualTo("/v2/databases/db1");
    }

    @Test
    void should_classify_only_429_5xx_and_unreachable_as_transient() {
      for (int code : new int[] {429, 500, 503}) {
        status = code;
        assertThatThrownBy(() -> client().getDatabaseStatus("db1"))
            .isInstanceOf(AstraKeyspaceCreator.TransientApiException.class)
            .hasMessageContaining(String.valueOf(code));
      }
      for (int code : new int[] {400, 401, 404}) {
        status = code;
        assertThatThrownBy(() -> client().getDatabaseStatus("db1"))
            .isInstanceOf(FatalSchemaException.class)
            .isNotInstanceOf(AstraKeyspaceCreator.TransientApiException.class);
      }
      int port = server.getAddress().getPort();
      server.stop(0);
      assertThatThrownBy(
              () ->
                  new DevOpsClient("http://127.0.0.1:" + port, token, 500).getDatabaseStatus("db1"))
          .isInstanceOf(AstraKeyspaceCreator.TransientApiException.class);
    }

    @Test
    void should_report_unexpected_response_when_status_body_is_not_json() {
      status = 200;
      body = "canary-body";

      assertThatThrownBy(() -> client().getDatabaseStatus("db1"))
          .isInstanceOf(FatalSchemaException.class)
          .hasMessageContaining("unexpected response from the Astra DevOps API")
          .hasNoCause()
          .isNotInstanceOf(AstraKeyspaceCreator.TransientApiException.class)
          .satisfies(e -> assertThat(e.getMessage()).doesNotContain("Cannot reach"));
    }
  }
}
