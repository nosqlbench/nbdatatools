package io.nosqlbench.vectordata;

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


import io.nosqlbench.vectordata.download.Catalog;
import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/// configuration for the datasets command
/// @param locations
///     the locations to search for datasets
public record TestDataSources(List<URL> locations) {

  /// Creates a TestDataSources with default configuration from ~/.config/nbvectors/catalogs.yaml
  public TestDataSources() {
    this(loadConfig(Path.of("~/.config/nbvectors")));
  }

  /// Creates a TestDataSources by combining two lists of locations
  ///
  /// @param locations The first list of catalog locations
  /// @param locations2 The second list of catalog locations to append
  private TestDataSources(List<URL> locations, List<URL> locations2) {
    this(new ArrayList<>() {{
      addAll(locations);
      addAll(locations2);
    }});
  }

  /// Loads catalog URLs from a configuration directory
  ///
  /// @param configdir The directory containing catalogs.yaml
  /// @return A list of catalog URLs
  /// @throws RuntimeException If the configuration file cannot be read or is invalid
  private static List<URL> loadConfig(Path configdir) {
    configdir = Path.of(configdir.toString().replace("~", System.getProperty("user" + ".home")));
    List<URL> clocations = new ArrayList<>();
    Path catalogCatalog = configdir.resolve("catalogs.yaml");
    if (Files.exists(catalogCatalog)) {
      LoadSettings loadSettings = LoadSettings.builder().build();
      Load yaml = new Load(loadSettings);
      try {
        Object configs = yaml.loadFromString(Files.readString(catalogCatalog));
        if (configs instanceof List<?> list) {
          list.forEach(c -> clocations.add(createUrl((String) c)));
        } else {
          throw new RuntimeException("catalogs.yaml must be a list of strings");
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return clocations;
    } else {
      throw new RuntimeException(
          "no catalogs specified, and no catalogs.yaml found in " + configdir);
    }
  }

  /// Creates a new TestDataSources by adding catalogs from a configuration directory
  ///
  /// @param configdir The directory containing catalogs.yaml
  /// @return A new TestDataSources with additional catalogs
  public TestDataSources configure(Path configdir) {
    return new TestDataSources(loadConfig(configdir), this.locations);
  }


  /// load a list of catalog locations
  /// @param basepaths
  ///     the base paths of catalogs to add
  /// @return a data sources config
  public TestDataSources addCatalogs(String... basepaths) {
    List<URL> clocations = new ArrayList<>(this.locations);
    for (String basepath : basepaths) {
      clocations.add(createUrl(basepath));
    }
    return new TestDataSources(clocations);
  }

  /// Creates a new TestDataSources by adding catalogs from a list of base paths
  ///
  /// @param basepaths The list of catalog base paths to add
  /// @return A new TestDataSources with additional catalogs
  public TestDataSources addCatalogs(List<String> basepaths) {
    return addCatalogs(basepaths.toArray(new String[0]));
  }


  /// Creates a Catalog from this TestDataSources
  ///
  /// @return A new Catalog
  /// @throws RuntimeException If no catalogs are specified
  public Catalog find() {
    if (this.locations.isEmpty()) {
      throw new RuntimeException("no catalogs specified");
    }
    return Catalog.of(this);
  }

  /// Creates a TestDataSources with a single URL
  ///
  /// @param url The catalog URL
  /// @return A new TestDataSources with the specified URL
  /// @throws RuntimeException If the URL is malformed
  public static TestDataSources ofUrl(String url) {
    try {
      return new TestDataSources(List.of(new URL(url)));
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  /// The default TestDataSources pointing to the public jvector datasets
  public static TestDataSources DEFAULT =
      TestDataSources.ofUrl("https://jvector-datasets-public.s3.us-east-1.amazonaws.com/");

  /// Creates a URL from a catalog string, handling both HTTP URLs and file paths
  ///
  /// @param catalog The catalog string (URL or file path)
  /// @return The created URL
  /// @throws RuntimeException If the URL cannot be created
  private static URL createUrl(String catalog) {
    if (catalog.startsWith("http")) {
      try {
        return new URL(catalog);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      try {
        return Path.of(catalog).toUri().toURL();
      } catch (MalformedURLException e) {
        throw new RuntimeException(e);
      }
    }
  }

}
