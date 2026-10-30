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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/** Minimal client for the Astra DevOps API. */
public class AstraDevOpsClient {
  public static final String DEFAULT_BASE_URL = "https://api.astra.datastax.com";

  private final String baseUrl;
  private final String bearerToken;
  private final int timeoutMillis;

  public AstraDevOpsClient(String baseUrl, String bearerToken, int timeoutMillis) {
    this.baseUrl = baseUrl;
    this.bearerToken = bearerToken;
    this.timeoutMillis = timeoutMillis;
  }

  /** Creates the keyspace; an already-existing keyspace is not an error. */
  public void createKeyspace(String databaseId, String keyspace) {
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

  public String getDatabaseStatus(String databaseId) {
    String action = "read the status of database " + databaseId;
    HttpURLConnection conn = open("GET", "/v2/databases/" + databaseId, action);
    try {
      int code = status(conn, action);
      if (code < 200 || code >= 300) {
        throw unexpected(code, action);
      }
      try (InputStream in = conn.getInputStream()) {
        JsonNode status = new ObjectMapper().readTree(in).get("status");
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
            "Cannot reach the Astra DevOps API at %s to %s; with private networking (PrivateLink) the connector must be able to reach it, or create the keyspace manually",
            baseUrl, action),
        cause);
  }
}
