package io.nosqlbench.vectordata.discovery;

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


import io.nosqlbench.vectordata.downloader.Catalog;
import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/// configuration for the datasets command
/// @param locations
///     the locations to search for datasets
/// @param optionalLocations
///     optional catalog locations that will be loaded if available but won't cause errors if missing
public record TestDataSources(List<URL> locations, List<URL> optionalLocations) {

  /// Creates a TestDataSources with default configuration from ~/.config/nbvectors/catalogs.yaml
  public TestDataSources() {
    this(new ArrayList<URL>(), new ArrayList<URL>());
    //    this(loadConfig(Path.of("~/.config/nbvectors")));
  }

  /// Creates a TestDataSources by combining locations and optional locations
  /// @param requiredLocations
  ///     The list of required catalog locations
  /// @param optionalLocations
  ///     The list of optional catalog locations
  /// @param additionalRequired
  ///     Additional required locations to append
  /// @param additionalOptional
  ///     Additional optional locations to append
  private TestDataSources(List<URL> requiredLocations, List<URL> optionalLocations, List<URL> additionalRequired, List<URL> additionalOptional) {
    this(new ArrayList<>() {{
      addAll(requiredLocations);
      addAll(additionalRequired);
    }}, new ArrayList<>() {{
      addAll(optionalLocations);
      addAll(additionalOptional);
    }});
  }

  /// Loads catalog URLs from a configuration directory
  /// @param configdir
  ///     The directory containing catalogs.yaml
  /// @return A list of catalog URLs
  /// @throws RuntimeException
  ///     If the configuration file cannot be read or is invalid
  private static List<URL> loadConfig(Path configdir) {
    return loadConfig(configdir, false);
  }

  /// Loads catalog URLs from a configuration directory with optional behavior
  /// @param configdir
  ///     The directory containing catalogs.yaml
  /// @param optional
  ///     If true, return empty list when config file doesn't exist instead of throwing
  /// @return A list of catalog URLs
  /// @throws RuntimeException
  ///     If optional is false and the configuration file cannot be read or is invalid
  private static List<URL> loadConfig(Path configdir, boolean optional) {
    configdir = expandTilde(configdir);
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
      if (optional) {
        return new ArrayList<>();
      } else {
        throw new RuntimeException(
            "no catalogs specified, and no catalogs.yaml found in " + configdir);
      }
    }
  }

  /// Creates a new TestDataSources by adding catalogs from a configuration directory
  /// @param configdir
  ///     The directory containing catalogs.yaml
  /// @return A new TestDataSources with additional catalogs
  public TestDataSources configure(Path configdir) {
    return new TestDataSources(this.locations, this.optionalLocations, loadConfig(configdir), new ArrayList<>());
  }

  /// Configure the test data catalog with the default configuration from ~/.config/nbvectors/catalogs.yaml
  /// @return A new TestDataSources with the default configuration
  public TestDataSources configure() {
    return new TestDataSources(loadConfig(Path.of(expandTilde("~/.config/nbvectors"))), new ArrayList<>());
  }

  /// Creates a new TestDataSources by adding optional catalogs from a configuration directory
  /// @param configdir
  ///     The directory containing catalogs.yaml
  /// @return A new TestDataSources with additional optional catalogs
  public TestDataSources configureOptional(Path configdir) {
    return new TestDataSources(this.locations, this.optionalLocations, new ArrayList<>(), loadConfig(configdir, true));
  }

  /// Configure optional catalogs with the default configuration from ~/.config/nbvectors/catalogs.yaml
  /// @return A new TestDataSources with optional default configuration
  public TestDataSources configureOptional() {
    return new TestDataSources(this.locations, this.optionalLocations, new ArrayList<>(), loadConfig(Path.of(expandTilde("~/.config/nbvectors")), true));
  }


  /// load a list of catalog locations
  /// @param basepaths
  ///     the base paths of catalogs to add
  /// @return a data sources config
  public TestDataSources addCatalogs(String... basepaths) {
    List<URL> clocations = new ArrayList<>();
    for (String basepath : basepaths) {
      String expandedPath = expandTilde(basepath);
      Path path = Path.of(expandedPath);
      
      // If it's a directory with catalogs.yaml, or a YAML file, load and expand it
      if ((Files.isDirectory(path) && Files.exists(path.resolve("catalogs.yaml"))) ||
          (Files.isRegularFile(path) && path.toString().endsWith(".yaml"))) {
        // Load the catalog URLs from the YAML file
        clocations.addAll(loadCatalogUrls(path));
      } else {
        // For regular catalog directories or other files, use createUrl
        clocations.add(createUrl(expandedPath));
      }
    }
    return new TestDataSources(this.locations, this.optionalLocations, clocations, new ArrayList<>());
  }

  /// Loads catalog URLs from a YAML file or directory containing catalogs.yaml
  /// @param path
  ///     The path to a YAML file or directory containing catalogs.yaml
  /// @return A list of catalog URLs
  /// @throws RuntimeException
  ///     If the YAML file cannot be read or is invalid
  private static List<URL> loadCatalogUrls(Path path) {
    Path yamlFile = Files.isDirectory(path) ? path.resolve("catalogs.yaml") : path;
    
    if (!Files.exists(yamlFile)) {
      throw new RuntimeException("YAML file not found: " + yamlFile);
    }
    
    List<URL> clocations = new ArrayList<>();
    LoadSettings loadSettings = LoadSettings.builder().build();
    Load yaml = new Load(loadSettings);
    try {
      Object configs = yaml.loadFromString(Files.readString(yamlFile));
      if (configs instanceof List<?> list) {
        list.forEach(c -> clocations.add(createUrl((String) c)));
      } else {
        throw new RuntimeException("YAML file must contain a list of strings: " + yamlFile);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read YAML file: " + yamlFile, e);
    }
    return clocations;
  }

  /// Creates a new TestDataSources by adding catalogs from a list of base paths
  /// @param basepaths
  ///     The list of catalog base paths to add
  /// @return A new TestDataSources with additional catalogs
  public TestDataSources addCatalogs(List<String> basepaths) {
    return addCatalogs(basepaths.toArray(new String[0]));
  }

  /// Add optional catalog locations that won't cause errors if missing
  /// @param basepaths
  ///     The base paths of optional catalogs to add
  /// @return A new TestDataSources with additional optional catalogs
  public TestDataSources addOptionalCatalogs(String... basepaths) {
    List<URL> clocations = new ArrayList<>();
    for (String basepath : basepaths) {
      String expandedPath = expandTilde(basepath);
      Path path = Path.of(expandedPath);
      
      // If it's a directory with catalogs.yaml, or a YAML file, load and expand it
      if ((Files.isDirectory(path) && Files.exists(path.resolve("catalogs.yaml"))) ||
          (Files.isRegularFile(path) && path.toString().endsWith(".yaml"))) {
        // Load the catalog URLs from the YAML file
        clocations.addAll(loadCatalogUrls(path));
      } else {
        // For regular catalog directories or other files, use createUrl
        clocations.add(createUrl(expandedPath));
      }
    }
    return new TestDataSources(this.locations, this.optionalLocations, new ArrayList<>(), clocations);
  }

  /// Add optional catalog locations from a list of base paths
  /// @param basepaths
  ///     The list of optional catalog base paths to add
  /// @return A new TestDataSources with additional optional catalogs
  public TestDataSources addOptionalCatalogs(List<String> basepaths) {
    return addOptionalCatalogs(basepaths.toArray(new String[0]));
  }

  /// Add optional catalog locations from a list of paths
  /// @param optionalPaths
  /// The list of optional catalog paths to add
  /// @return A new TestDataSources with additional optional catalogs
  public TestDataSources addOptionalCatalogs(Path... optionalPaths) {
    return addOptionalCatalogs(Arrays.stream(optionalPaths).map(Path::toString).toArray(String[]::new));
  }

  /// Creates a Catalog from this TestDataSources
  /// @return A new Catalog
  /// @throws RuntimeException
  ///     If no catalogs are specified
  public Catalog catalog() {
    if (this.locations.isEmpty() && this.optionalLocations.isEmpty()) {
      throw new RuntimeException("no catalogs specified, call configure() first for default "
                                 + "configuration from \"~/.config/nbvectors/catalogs.yaml\", or "
                                 + "configure(...) to specify a config " + "directory.");
    }
    return Catalog.of(this);
  }

  /// Creates a TestDataSources with a single URL
  /// @param url
  ///     The catalog URL
  /// @return A new TestDataSources with the specified URL
  /// @throws RuntimeException
  ///     If the URL is malformed
  public static TestDataSources ofUrl(String url) {
    try {
      return new TestDataSources(List.of(new URL(url)), new ArrayList<>());
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  /// Creates a URL from a catalog string, handling HTTP URLs, directories, and file paths
  /// @param catalog
  ///     The catalog string (URL, directory path, or file path)
  /// @return The created URL
  /// @throws RuntimeException
  ///     If the URL cannot be created
  private static URL createUrl(String catalog) {
    if (catalog.startsWith("http")) {
      try {
        return new URL(catalog);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      try {
        // Expand tilde first on the string, then create Path
        String expandedPath = expandTilde(catalog);
        Path path = Path.of(expandedPath);
        
        // If it's a directory, try to find catalogs.yaml first, then fall back to catalog.json
        if (Files.isDirectory(path)) {
          Path catalogsYaml = path.resolve("catalogs.yaml");
          if (Files.exists(catalogsYaml)) {
            // This is a configuration directory with catalogs.yaml
            return catalogsYaml.toUri().toURL();
          }
          
          Path catalogJson = path.resolve("catalog.json");
          if (Files.exists(catalogJson)) {
            // This is a catalog directory with catalog.json
            return path.toUri().toURL();
          }
          
          // Neither catalogs.yaml nor catalog.json found
          throw new RuntimeException("Directory " + path + " does not contain catalogs.yaml or catalog.json");
        }
        
        // For files (including YAML files), return the path directly
        return path.toUri().toURL();
      } catch (MalformedURLException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /// Expands tilde (~) in paths to the user's home directory
  /// @param path
  ///     The path that may contain a tilde
  /// @return The path with tilde expanded to user.home
  private static Path expandTilde(Path path) {
    String pathStr = path.toString();
    if (pathStr.startsWith("~")) {
      pathStr = getHomeDirectory() + pathStr.substring(1);
      return Path.of(pathStr);
    }
    return path;
  }

  /// Expands tilde (~) in string paths to the user's home directory
  /// @param pathStr
  ///     The path string that may contain a tilde
  /// @return The path string with tilde expanded to user.home
  private static String expandTilde(String pathStr) {
    if (pathStr.startsWith("~")) {
      return getHomeDirectory() + pathStr.substring(1);
    }
    return pathStr;
  }

  /// Gets the home directory to use for tilde expansion.
  /// This method checks for a system property override first (for testing),
  /// then falls back to the actual user.home.
  /// @return The home directory path
  private static String getHomeDirectory() {
    return System.getProperty("test.home.override", System.getProperty("user.home"));
  }

}
