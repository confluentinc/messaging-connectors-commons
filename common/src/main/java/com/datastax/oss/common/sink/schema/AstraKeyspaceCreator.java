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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/** Creates keyspaces through the Astra DevOps API. */
public class AstraKeyspaceCreator implements KeyspaceCreator {
  static final String DEFAULT_API_URL = "https://api.astra.datastax.com";
  private static final long POLL_INTERVAL_MILLIS = 2_000;
  private static final long TIMEOUT_MILLIS = 5 * 60_000;
  private static final Pattern DB_ID =
      Pattern.compile("^([0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12})");

  private final DevOpsClient client;
  private final String databaseId;
  private final long pollIntervalMillis;
  private final long timeoutMillis;

  /** Targets the database of the secure connect bundle (file path or URL). */
  public AstraKeyspaceCreator(String secureConnectBundle, String token, int apiTimeoutMillis) {
    this(
        new DevOpsClient(DEFAULT_API_URL, token, apiTimeoutMillis),
        databaseId(secureConnectBundle),
        POLL_INTERVAL_MILLIS,
        TIMEOUT_MILLIS);
  }

  AstraKeyspaceCreator(
      DevOpsClient client, String databaseId, long pollIntervalMillis, long timeoutMillis) {
    this.client = client;
    this.databaseId = databaseId;
    this.pollIntervalMillis = pollIntervalMillis;
    this.timeoutMillis = timeoutMillis;
  }

  @Override
  public void ensureKeyspace(CqlSession session, CqlIdentifier keyspace) {
    if (session.getMetadata().getKeyspace(keyspace).isPresent()) {
      return;
    }
    client.createKeyspace(databaseId, keyspace.asInternal());

    // ACTIVE may be reported before the database enters MAINTENANCE, so the keyspace showing up
    // in the driver metadata is what ends the wait.
    long deadline = System.nanoTime() + timeoutMillis * 1_000_000;
    boolean active = false;
    while (true) {
      active = active || "ACTIVE".equals(client.getDatabaseStatus(databaseId));
      if (active) {
        session.refreshSchema();
        if (session.getMetadata().getKeyspace(keyspace).isPresent()) {
          return;
        }
      }
      if (System.nanoTime() > deadline) {
        throw new FatalSchemaException(
            active
                ? String.format(
                    "Keyspace %s did not become visible after creation", keyspace.asInternal())
                : String.format(
                    "Database %s did not become ACTIVE after creating keyspace %s",
                    databaseId, keyspace.asInternal()));
      }
      pause();
    }
  }

  private void pause() {
    try {
      Thread.sleep(pollIntervalMillis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new FatalSchemaException("Interrupted while waiting for keyspace creation", e);
    }
  }

  /** Reads the database id from a secure connect bundle given as a file path or a URL. */
  static String databaseId(String location) {
    URL url;
    try {
      url = new URL(location);
    } catch (MalformedURLException e) {
      return databaseId(Paths.get(location));
    }
    try (InputStream in = url.openStream()) {
      return databaseId(in);
    } catch (IOException e) {
      if (url.getUserInfo() == null && url.getQuery() == null) {
        throw new FatalSchemaException("Cannot read secure connect bundle " + location, e);
      }
      // The user info may hold a password and the query string a signature, and the cause message
      // may repeat the full URL.
      String shown =
          url.getProtocol()
              + "://"
              + url.getHost()
              + (url.getPort() < 0 ? "" : ":" + url.getPort())
              + url.getPath();
      throw new FatalSchemaException(
          String.format(
              "Cannot read secure connect bundle %s (%s)", shown, e.getClass().getSimpleName()));
    }
  }

  static String databaseId(Path secureConnectBundle) {
    try (InputStream in = Files.newInputStream(secureConnectBundle)) {
      return databaseId(in);
    } catch (IOException e) {
      throw new FatalSchemaException("Cannot read secure connect bundle " + secureConnectBundle, e);
    }
  }

  private static String databaseId(InputStream in) throws IOException {
    ZipInputStream zip = new ZipInputStream(in);
    for (ZipEntry entry = zip.getNextEntry(); entry != null; entry = zip.getNextEntry()) {
      if (entry.getName().equals("config.json")) {
        JsonNode root;
        try {
          root = new ObjectMapper().readTree(zip);
        } catch (JsonProcessingException e) {
          // config.json holds keystore passwords, and the parse error may quote its content.
          throw new FatalSchemaException("Secure connect bundle config.json is not valid JSON");
        }
        JsonNode host = root == null ? null : root.get("host");
        Matcher m = DB_ID.matcher(host == null ? "" : host.asText());
        if (m.find()) {
          return m.group(1);
        }
        throw new FatalSchemaException(
            "Cannot determine the database id from the secure connect bundle host");
      }
    }
    throw new FatalSchemaException("Secure connect bundle has no config.json");
  }

  /** Minimal client for the Astra DevOps API. */
  static class DevOpsClient {
    private final String baseUrl;
    private final String bearerToken;
    private final int timeoutMillis;

    DevOpsClient(String baseUrl, String bearerToken, int timeoutMillis) {
      this.baseUrl = baseUrl;
      this.bearerToken = bearerToken;
      this.timeoutMillis = timeoutMillis;
    }

    /** Creates the keyspace; an already-existing keyspace is not an error. */
    void createKeyspace(String databaseId, String keyspace) {
      String path = "/v2/databases/" + databaseId + "/keyspaces/" + keyspace;
      String action = "create keyspace " + keyspace;
      HttpURLConnection conn = open("POST", path, action);
      try {
        int code = status(conn, action);
        if (code != 409 && (code < 200 || code >= 300)) {
          throw unexpected(code, action);
        }
      } finally {
        conn.disconnect();
      }
    }

    String getDatabaseStatus(String databaseId) {
      String action = "read the status of database " + databaseId;
      HttpURLConnection conn = open("GET", "/v2/databases/" + databaseId, action);
      try {
        int code = status(conn, action);
        if (code < 200 || code >= 300) {
          throw unexpected(code, action);
        }
        try (InputStream in = conn.getInputStream()) {
          JsonNode root;
          try {
            root = new ObjectMapper().readTree(in);
          } catch (JsonProcessingException e) {
            // The parse error may quote the response body.
            throw new FatalSchemaException(
                String.format(
                    "Received an unexpected response from the Astra DevOps API trying to %s",
                    action));
          }
          JsonNode status = root == null ? null : root.get("status");
          if (status == null) {
            throw new FatalSchemaException("Astra DevOps API returned no status for " + databaseId);
          }
          return status.asText();
        }
      } catch (IOException e) {
        throw unreachable(action, e);
      } finally {
        conn.disconnect();
      }
    }

    private HttpURLConnection open(String method, String path, String action) {
      try {
        HttpURLConnection conn = (HttpURLConnection) new URL(baseUrl + path).openConnection();
        conn.setRequestMethod(method);
        conn.setConnectTimeout(timeoutMillis);
        conn.setReadTimeout(timeoutMillis);
        conn.setRequestProperty("Authorization", "Bearer " + bearerToken);
        conn.setRequestProperty("Accept", "application/json");
        return conn;
      } catch (IOException e) {
        throw unreachable(action, e);
      }
    }

    private int status(HttpURLConnection conn, String action) {
      try {
        int code = conn.getResponseCode();
        if (code == 401 || code == 403) {
          throw new FatalSchemaException(
              String.format(
                  "Astra DevOps API rejected the request to %s (HTTP %d); the credential in auth.password needs the Database Administrator role",
                  action, code));
        }
        return code;
      } catch (IOException e) {
        throw unreachable(action, e);
      }
    }

    private static FatalSchemaException unexpected(int code, String action) {
      return new FatalSchemaException(
          String.format("Astra DevOps API failed to %s (HTTP %d)", action, code));
    }

    private FatalSchemaException unreachable(String action, IOException cause) {
      return new FatalSchemaException(
          String.format(
              "Cannot reach the Astra DevOps API at %s to %s (%s: %s); make sure the connector has network access to it, or create the keyspace manually",
              baseUrl, action, cause.getClass().getSimpleName(), cause.getMessage()),
          cause);
    }
  }
}
