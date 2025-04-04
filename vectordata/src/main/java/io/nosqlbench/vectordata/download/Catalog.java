package io.nosqlbench.vectordata.download;

/*
 * Copyright (c) nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.nosqlbench.vectordata.VectorSources;
import okhttp3.Headers;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/// a catalog of datasets
public record Catalog(List<DatasetEntry> datasets) {

  private static final OkHttpClient httpClient = new OkHttpClient();

  /// create a catalog from a config
  /// @param config
  ///     the config to use
  /// @return a catalog
  public static Catalog of(VectorSources config) {
    List<DatasetEntry> entries = new ArrayList<>();
    Gson gson = new Gson();

    Type entryType = new TypeToken<List<DatasetEntry>>() {
    }.getType();

    for (URL location : config.locations()) {
      URL fileUrl = fileFor(location);
      try {
        String content;
        if (fileUrl.getProtocol().startsWith("http")) {
          // Use OkHttp client for remote files
          Request.Builder requestBuilder = new Request.Builder()
              .url(fileUrl)
              .header("Accept", "application/yaml, application/json");

          // Add authorization if token is present in environment
          String token = System.getenv("HF_TOKEN");
          if (token != null && !token.isEmpty()) {
            requestBuilder.header("Authorization", "Bearer " + token);
          }

          Request request = requestBuilder.build();

          try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
              throw new IOException("Unexpected code " + response);
            }
            content = response.body().string();
          }
        } else {
          // Use direct file access for local files
          try (InputStream stream = fileUrl.openStream();
               InputStreamReader reader = new InputStreamReader(stream)) {
            StringBuilder builder = new StringBuilder();
            char[] buffer = new char[8192];
            int read;
            while ((read = reader.read(buffer, 0, buffer.length)) > 0) {
              builder.append(buffer, 0, read);
            }
            content = builder.toString();
          }
        }

        // Handle both YAML and JSON formats
        List<DatasetEntry> catalogEntries;
        catalogEntries = gson.fromJson(content, entryType);
        ListIterator<DatasetEntry> li = catalogEntries.listIterator();
        while (li.hasNext()) {
          DatasetEntry entry = li.next();
          li.set(new DatasetEntry(
              entry.name(),
              new URL(location, entry.name()),
              entry.attributes(),
              entry.datasets(),
              entry.tokens(),
              entry.tags()
          ));
        }
        entries.addAll(catalogEntries);
      } catch (IOException e) {
        throw new RuntimeException(
            "Failed to load catalog from " + fileUrl + ": " + e.getMessage(),
            e
        );
      }
    }

    return new Catalog(entries);
  }

  private static URL fileFor(URL location) {
    if (location.toString().endsWith("/catalog.json")) {
      return location;
    }
    String withFile = location.toString();
    withFile = withFile.endsWith("/") ? withFile : withFile + "/";
    withFile = withFile + "catalog.json";
    try {
      return new URL(withFile);
    } catch (Exception e) {
      throw new RuntimeException("Invalid catalog URL: " + withFile, e);
    }
  }
}