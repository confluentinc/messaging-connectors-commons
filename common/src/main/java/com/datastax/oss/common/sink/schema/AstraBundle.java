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
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/** Reads the database id from an Astra secure connect bundle. */
public final class AstraBundle {
  private static final Pattern DB_ID =
      Pattern.compile("^([0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12})");

  private AstraBundle() {}

  public static String databaseId(Path secureConnectBundle) {
    try (InputStream in = Files.newInputStream(secureConnectBundle)) {
      return databaseId(in);
    } catch (IOException e) {
      throw new FatalSchemaException("Cannot read secure connect bundle " + secureConnectBundle, e);
    }
  }

  /** Accepts a file path or a URL. */
  public static String databaseId(String location) {
    try {
      URL url;
      try {
        url = new URL(location);
      } catch (MalformedURLException e) {
        return databaseId(Paths.get(location));
      }
      try (InputStream in = url.openStream()) {
        return databaseId(in);
      }
    } catch (IOException e) {
      int query = location.indexOf('?');
      if (query < 0) {
        throw new FatalSchemaException("Cannot read secure connect bundle " + location, e);
      }
      // The query string may hold a signature, and the cause message may repeat the full URL.
      throw new FatalSchemaException(
          String.format(
              "Cannot read secure connect bundle %s (%s)",
              location.substring(0, query), e.getClass().getSimpleName()));
    }
  }

  private static String databaseId(InputStream in) throws IOException {
    ZipInputStream zip = new ZipInputStream(in);
    for (ZipEntry entry = zip.getNextEntry(); entry != null; entry = zip.getNextEntry()) {
      if (entry.getName().equals("config.json")) {
        JsonNode host = new ObjectMapper().readTree(zip).get("host");
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
}
