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
import io.nosqlbench.vectordata.TestDataSources;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;
import java.util.regex.Pattern;

/// a catalog of datasets
public record Catalog(List<DatasetEntry> datasets) {

  private static final OkHttpClient httpClient = new OkHttpClient();

  /// create a catalog from a config
  /// @param config
  ///     the config to use
  /// @return a catalog
  public static Catalog of(TestDataSources config) {
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
          Request.Builder requestBuilder = new Request.Builder().url(fileUrl)
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
               InputStreamReader reader = new InputStreamReader(stream))
          {
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

    /// Find a dataset by a specific name, case insensitive
    /// @param name the name of the dataset to find
    /// @return the dataset, if found
    public Optional<DatasetEntry> findExact(String name) {
      List<DatasetEntry> found =
          datasets.stream().filter(e -> e.name().equalsIgnoreCase(name)).toList();
      if (found.size() == 1) {
        return Optional.of(found.getFirst());
      } else if (found.size() > 1) {
        throw new RuntimeException("Found multiple datasets matching " + name + ": " + found);
      } else {
        return Optional.empty();
      }
    }

  //  /// Find a dataset by a partial name, case insensitive
  //  /// @param name the name of the dataset to find
  //  /// @return the dataset, if found
  //  public Optional<DatasetEntry> findSubstring(String name) {
  //    return datasets.stream().filter(e -> e.name().toLowerCase().contains(name.toLowerCase())).findFirst();
  //  }

  public List<DatasetEntry> matchGlob(String glob) {
    PathMatcher pathMatcher = FileSystems.getDefault().getPathMatcher("glob:" + glob);
    return datasets.stream().filter(e -> pathMatcher.matches(Path.of(e.name()))).toList();
  }

  public List<DatasetEntry> matchRegex(String regex) {
    Pattern p = Pattern.compile(regex);
    return datasets.stream().filter(e -> p.matcher(e.name()).matches()).toList();
  }

  public Optional<DatasetEntry> matchOne(String regex) {
    Pattern p = Pattern.compile(regex);
    List<DatasetEntry> found =
        datasets.stream().filter(e -> p.matcher(e.name()).matches()).toList();
    if (found.size() == 1) {
      return Optional.of(found.getFirst());
    } else if (found.size() > 1) {
      throw new RuntimeException(
          "Found multiple datasets matching " + regex + ": " + found + ":" + datasets);
    } else {
      return Optional.empty();
    }
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