package io.nosqlbench.vectordata.downloader;

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


import com.google.gson.reflect.TypeToken;
import io.nosqlbench.vectordata.discovery.TestDataSources;
import io.nosqlbench.vectordata.utils.SHARED;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/// A catalog of vector datasets available for download.
///
/// This class provides methods to search and retrieve dataset entries from various sources,
/// supporting both local and remote catalogs in JSON format.
/// @param datasets
///     The list of dataset entries in the catalog
public record Catalog(List<DatasetEntry> datasets) {

  /// HTTP client for downloading catalog files from remote sources
  private static final OkHttpClient httpClient = new OkHttpClient();

  /// Creates a catalog by loading and parsing catalog files from the provided sources.
  ///
  /// This method fetches catalog.json files from each location in the config,
  /// parses them, and combines all dataset entries into a single catalog.
  /// Supports both HTTP and file URLs.
  /// @param config
  ///     The configuration containing catalog source locations
  /// @return A new catalog containing all dataset entries from the sources
  /// @throws RuntimeException
  ///     If any required catalog file cannot be loaded or parsed
  public static Catalog of(TestDataSources config) {
    List<DatasetEntry> entries = new ArrayList<>();

    // Process required catalogs first - errors will cause failure
    for (URL location : config.locations()) {
      loadCatalogEntries(location, entries, true);
    }

    // Process optional catalogs - errors will be logged but won't cause failure
    for (URL location : config.optionalLocations()) {
      loadCatalogEntries(location, entries, false);
    }

    return new Catalog(entries);
  }

  /// Loads catalog entries from a single location
  /// @param location
  ///     The catalog location URL
  /// @param entries
  ///     The list to add entries to
  /// @param required
  ///     Whether this catalog is required (throws on error) or optional (logs on error)
  /// @throws RuntimeException
  ///     If required is true and the catalog cannot be loaded
  private static void loadCatalogEntries(URL location, List<DatasetEntry> entries, boolean required) {
      location = sanitizeUrlPath(location);
      if (location.getPath().endsWith("/")) {

      }
      URL fileUrl = fileFor(location);
      try {
        String content;
        if (fileUrl.getProtocol().startsWith("http")) {
          // Use OkHttp client for remote files
          Request.Builder requestBuilder = new Request.Builder().url(fileUrl)
              .header("Accept", "application/yaml, application/json");

          //          // Add authorization if token is present in environment
          //          String token = System.getenv("HF_TOKEN");
          //          if (token != null && !token.isEmpty()) {
          //            requestBuilder.header("Authorization", "Bearer " + token);
          //          }

          Request request = requestBuilder.build();

          try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
              String errorBody = "";
              try {
                if (response.body() != null) {
                  errorBody = response.body().string();
                }
              } catch (Exception ignored) {
                // Ignore any errors reading the body
              }
              throw new IOException("Unexpected code " + response + (errorBody.isEmpty() ? "" : ": " + errorBody));
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


        //        Type entryType = new TypeToken<List<DatasetEntry>>() {
        //        }.getType();
        List<Map<String, Object>> entryList =
            SHARED.gson.fromJson(
                content, new TypeToken<List<Map<String, Object>>>() {
                }.getType()
            );
        for (Map<String, Object> entry : entryList) {
          Map<String,Object> remapped = new LinkedHashMap<>();
          if (entry.containsKey("layout")) {
            URL url = new URL(location, entry.get("path").toString());
            String name = dirNameOfPath(url);
            remapped.put("name",name);
            remapped.put("url", url);
            Map<String,Object> layout = (Map<String, Object>) entry.get("layout");
            remapped.put("attributes",layout.get("attributes"));
            remapped.put("profiles",layout.get("profiles"));
            remapped.put("tags",layout.get("tags"));
          } else {
            remapped.putAll(entry); // name, attributes, datasets, tokens, tags?
          }
          DatasetEntry dsEntry = DatasetEntry.fromData(remapped);
          entries.add(dsEntry);
        }
//        catalogEntries = SHARED.gson.fromJson(
//            content, new TypeToken<List<DatasetEntry>>() {
//            }.getType()
//        );
//        ListIterator<DatasetEntry> li = catalogEntries.listIterator();
//        while (li.hasNext()) {
//          DatasetEntry entry = li.next();
//          li.set(new DatasetEntry(
//              entry.name(),
//              new URL(location, entry.name()),
//              entry.attributes(),
//              entry.datasets(),
//              entry.tokens(),
//              entry.tags()
//          ));
//        }
//        entries.addAll(catalogEntries);
      } catch (IOException e) {
        if (required) {
          throw new RuntimeException(
              "Failed to load catalog from " + fileUrl + ": " + e.getMessage(),
              e
          );
        } else {
          // Log warning for optional catalogs but continue processing
          System.err.println("Warning: Could not load optional catalog from " + fileUrl + ": " + e.getMessage());
        }
      }
  }

  private static String dirNameOfPath(URL path) {
    String[] parts = path.toString().split("/");
    if (parts[parts.length - 1].equalsIgnoreCase("dataset.yaml")) {
      return parts[parts.length - 2];
    }
    return parts[parts.length - 1];
  }

  private static URL sanitizeUrlPath(URL path) {
    if (!path.getPath().endsWith("/")) {
      try {
        path = URI.create(path.toString()+"/").toURL();
      } catch (MalformedURLException e) {
        throw new RuntimeException(e);
      }
    }
    return path;
  }

  /// Finds a dataset by its exact name (case insensitive).
  /// @param name
  ///     The name of the dataset to find
  /// @return An Optional containing the dataset if found, or empty if not found
  /// @throws RuntimeException
  ///     If multiple datasets match the same name
  public Optional<DatasetEntry> findExact(String name) {
    List<DatasetEntry> found =
        datasets.stream().filter(e -> e.name().equalsIgnoreCase(name)).toList();
    if (found.size() == 1) {
      return Optional.of(found.getFirst());
    } else if (found.size() > 1) {
      throw new RuntimeException("Found multiple datasets matching " + name + ": " + found);
    } else {
      // No exact match found - provide helpful suggestions
      printDatasetSuggestions(name);
      return Optional.empty();
    }
  }

  /// Prints helpful suggestions to stderr when a dataset is not found.
  /// Lists all available datasets with their profiles and highlights any that contain the search term as a substring.
  /// @param searchName
  ///     The name that was searched for
  private void printDatasetSuggestions(String searchName) {
    System.err.println("Dataset '" + searchName + "' not found.");
    
    if (datasets.isEmpty()) {
      System.err.println("No datasets are available in the catalog.");
      return;
    }
    
    // Find datasets that contain the search term as a substring (case insensitive)
    List<DatasetEntry> substringMatches = datasets.stream()
        .filter(e -> e.name().toLowerCase().contains(searchName.toLowerCase()))
        .toList();
    
    if (!substringMatches.isEmpty()) {
      System.err.println("Did you mean one of these datasets?");
      for (DatasetEntry entry : substringMatches) {
        printDatasetWithProfiles(entry);
      }
      System.err.println();
    }
    
    // Always show all available datasets
    System.err.println("Available datasets (" + datasets.size() + " total):");
    for (DatasetEntry entry : datasets) {
      printDatasetWithProfiles(entry);
    }
  }

  /// Prints a dataset name along with its available profiles
  /// @param entry
  ///     The dataset entry to print
  private void printDatasetWithProfiles(DatasetEntry entry) {
    StringBuilder sb = new StringBuilder();
    sb.append("  - ").append(entry.name());
    
    if (entry.profiles() != null && !entry.profiles().isEmpty()) {
      sb.append(" (profiles: ");
      sb.append(String.join(", ", entry.profiles().keySet()));
      sb.append(")");
    } else {
      sb.append(" (no profiles)");
    }
    
    System.err.println(sb.toString());
  }

  //  /// Find a dataset by a partial name, case insensitive
  //  /// @param name the name of the dataset to find
  //  /// @return the dataset, if found
  //  public Optional<DatasetEntry> findSubstring(String name) {
  //    return datasets.stream().filter(e -> e.name().toLowerCase().contains(name.toLowerCase())).findFirst();
  //  }

  /// Matches datasets using a file glob pattern.
  ///
  /// Uses the same glob syntax as {@link java.nio.file.FileSystem#getPathMatcher(String)}.
  /// @param glob
  ///     The glob pattern to match against dataset names
  /// @return A list of matching dataset entries (may be empty)
  public List<DatasetEntry> matchGlob(String glob) {
    PathMatcher pathMatcher = FileSystems.getDefault().getPathMatcher("glob:" + glob);
    return datasets.stream().filter(e -> pathMatcher.matches(Path.of(e.name()))).toList();
  }

  /// Matches datasets using a regular expression pattern.
  /// @param regex
  ///     The regular expression to match against dataset names
  /// @return A list of matching dataset entries (may be empty)
  public List<DatasetEntry> matchRegex(String regex) {
    Pattern p = Pattern.compile(regex);
    return datasets.stream().filter(e -> p.matcher(e.name()).matches()).toList();
  }

  /// Matches a single dataset using a regular expression pattern.
  /// @param regex
  ///     The regular expression to match against dataset names
  /// @return An Optional containing the matching dataset if exactly one is found, or empty if none found
  /// @throws RuntimeException
  ///     If multiple datasets match the pattern
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


  /// Resolves the catalog.json file URL for a given location.
  ///
  /// If the location already ends with catalog.json, it is returned as is.
  /// Otherwise, catalog.json is appended to the location.
  /// @param location
  ///     The base location URL
  /// @return The resolved catalog.json file URL
  /// @throws RuntimeException
  ///     If the resulting URL is invalid
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
