package io.nosqlbench.nbvectors.commands.catalog_hdf5;

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
import com.google.gson.GsonBuilder;
import io.nosqlbench.nbvectors.commands.export_json.Hdf5JsonSummarizer;
import io.nosqlbench.vectordata.layout.TestGroupLayout;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.snakeyaml.engine.v2.api.Dump;
import org.snakeyaml.engine.v2.api.DumpSettings;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/// Catalog of data for HDF5 files
public class CatalogOut extends ArrayList<Map<String, Object>> {
  private final static Logger logger = LogManager.getLogger(CatalogOut.class);
  private final static Gson gson = new GsonBuilder().setPrettyPrinting().create();
  private final static Dump dump = new Dump(DumpSettings.builder().build());
  private final static Hdf5JsonSummarizer jsonSummarizer = new Hdf5JsonSummarizer();

  /// create a catalog
  /// @param entries
  ///     the entries to add
  public CatalogOut(List<Map<String, Object>> entries) {
    super(entries);
    //    if (mode==CatalogMode.update) {
    //      if (Files.exists(path)) {
    //        try {
    //          this.putAll(gson.fromJson(Files.newBufferedReader(path), Map.class));
    //        } catch (IOException e) {
    //          throw new RuntimeException(e);
    //        }
    //      } else {
    //        logger.warn("catalog file does not exist for update mode, will create a new one: " + path);
    //      }
    //    }
  }

  /// load all files and directories into the catalog
  /// @param paths
  ///     the files and directories to load
  /// @param commonParent
  ///     the common parent directory to use for path relativization
  /// @param basename
  ///     the basename to use for catalog files (default: "catalog")
  /// @return a catalog
  public static CatalogOut loadAll(List<Path> paths, Path commonParent, String basename) {
    if (basename == null || basename.isEmpty()) {
      basename = "catalog";
    }

    // Map to store catalog entries by directory
    Map<Path, List<Map<String, Object>>> catalogsByDirectory = new HashMap<>();

    // Process each path
    for (Path path : paths) {
      if (Files.isDirectory(path)) {
        // Process directory recursively
        try {
          processDirectoryRecursively(path, catalogsByDirectory);
        } catch (IOException e) {
          throw new RuntimeException("Error processing directory: " + path, e);
        }
      } else if (Files.isRegularFile(path) && path.toString().endsWith(".hdf5")) {
        // Add HDF5 file to the catalog
        Map<String, Object> fileEntry = loadHdf5File(path);

        // Add to the parent directory's catalog
        Path parentDir = path.getParent();
        catalogsByDirectory.computeIfAbsent(parentDir, k -> new ArrayList<>()).add(fileEntry);

        // Add to all parent directories' catalogs
        addEntryToParentCatalogs(parentDir, fileEntry, catalogsByDirectory);
      } else {
        throw new RuntimeException("not a file or directory: " + path);
      }
    }

    // Create catalog files for each directory
    for (Map.Entry<Path, List<Map<String, Object>>> entry : catalogsByDirectory.entrySet()) {
      Path directory = entry.getKey();
      List<Map<String, Object>> dirEntries = entry.getValue();

      // Create and save catalog for this directory
      CatalogOut dirCatalog = new CatalogOut(dirEntries);
      try {
        // Save with the specified basename
        dirCatalog.save(directory.resolve(basename + ".json"));
      } catch (Exception e) {
        logger.warn("Failed to save catalog for directory: {}", directory, e);
      }
    }

    // Get the entries for the common parent directory
    List<Map<String, Object>> topLevelEntries = catalogsByDirectory.getOrDefault(commonParent, new ArrayList<>());

    // Return the top-level catalog
    return new CatalogOut(topLevelEntries);
  }

  /// Overloaded method for backward compatibility
  public static CatalogOut loadAll(List<Path> paths, Path commonParent) {
    return loadAll(paths, commonParent, "catalog");
  }

  /**
   * Recursively processes a directory to find dataset.yaml files and .hdf5 files.
   * If a directory contains a dataset.yaml file, it's treated as a dataset root and not traversed further.
   * Creates catalog entries for each directory level.
   *
   * @param directory The directory to process
   * @param catalogsByDirectory Map to store catalog entries by directory
   * @throws IOException If an error occurs while traversing the directory
   * @return A list of catalog entries found in this directory
   */
  private static List<Map<String, Object>> processDirectoryRecursively(Path directory,
                                                                     Map<Path, List<Map<String, Object>>> catalogsByDirectory) throws IOException {
    List<Map<String, Object>> entries = new ArrayList<>();

    // Ensure the directory exists
    if (!Files.exists(directory)) {
      throw new RuntimeException("not a file or directory: " + directory);
    }

    // Ensure this directory has an entry in the catalogs map
    catalogsByDirectory.computeIfAbsent(directory, k -> new ArrayList<>());

    // Check if this directory contains a dataset.yaml file
    Path datasetYamlPath = directory.resolve("dataset.yaml");
    if (Files.exists(datasetYamlPath)) {
      // This is a dataset root, add it to the catalog and don't traverse further
      Map<String, Object> datasetEntry = loadDatasetYaml(datasetYamlPath);
      entries.add(datasetEntry);

      // Add to the directory's catalog
      catalogsByDirectory.computeIfAbsent(directory, k -> new ArrayList<>()).add(datasetEntry);

      // Add to parent directories' catalogs
      addEntryToParentCatalogs(directory, datasetEntry, catalogsByDirectory);
    } else {
      // This is not a dataset root, traverse it to find .hdf5 files and subdirectories
      final List<Map<String, Object>> dirEntries = new ArrayList<>();

      Files.walkFileTree(directory, Set.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
          // Skip the root directory as it's handled separately
          if (dir.equals(directory)) {
            return FileVisitResult.CONTINUE;
          }

          // Ensure this directory has an entry in the catalogs map
          catalogsByDirectory.computeIfAbsent(dir, k -> new ArrayList<>());

          // Check if this directory contains a dataset.yaml file
          Path datasetYaml = dir.resolve("dataset.yaml");
          if (Files.exists(datasetYaml)) {
            // This is a dataset root, add it to the catalog and don't traverse further
            Map<String, Object> datasetEntry = loadDatasetYaml(datasetYaml);
            dirEntries.add(datasetEntry);

            // Add to the directory's catalog
            catalogsByDirectory.computeIfAbsent(dir, k -> new ArrayList<>()).add(datasetEntry);

            // Add to parent directories' catalogs
            addEntryToParentCatalogs(dir, datasetEntry, catalogsByDirectory);

            // Don't traverse further into this directory
            return FileVisitResult.SKIP_SUBTREE;
          }
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          if (file.toString().endsWith(".hdf5")) {
            // This is an HDF5 file, add it to the catalog
            Map<String, Object> fileEntry = loadHdf5File(file);
            dirEntries.add(fileEntry);

            // Add to the directory's catalog
            Path parentDir = file.getParent();
            catalogsByDirectory.computeIfAbsent(parentDir, k -> new ArrayList<>()).add(fileEntry);

            // Add to parent directories' catalogs
            addEntryToParentCatalogs(parentDir, fileEntry, catalogsByDirectory);
          }
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
          // Ensure all directories have an entry in the catalogs map, even if empty
          catalogsByDirectory.computeIfAbsent(dir, k -> new ArrayList<>());
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path file, IOException exc) {
          logger.warn("Failed to visit file: {}", file, exc);
          return FileVisitResult.CONTINUE;
        }
      });

      entries.addAll(dirEntries);

      // Add all entries to this directory's catalog
      catalogsByDirectory.computeIfAbsent(directory, k -> new ArrayList<>()).addAll(dirEntries);

      // Also add all entries to parent directories' catalogs
      for (Map<String, Object> entry : dirEntries) {
        addEntryToParentCatalogs(directory, entry, catalogsByDirectory);
      }
    }

    return entries;
  }

  /**
   * Adds an entry to all parent directories' catalogs with properly relativized paths.
   * Adds to all parent directories up to the root.
   *
   * @param entryDir The directory containing the entry
   * @param entry The entry to add to parent catalogs
   * @param catalogsByDirectory Map of catalogs by directory
   */
  private static void addEntryToParentCatalogs(Path entryDir, Map<String, Object> entry,
                                              Map<Path, List<Map<String, Object>>> catalogsByDirectory) {
    // Start with the parent of the entry directory
    Path currentDir = entryDir.getParent();

    // If there's no parent directory, we're done
    if (currentDir == null) {
      return;
    }

    // Determine the target path (what we need to relativize)
    Path targetPath;
    if (entry.containsKey("path")) {
      // If the entry has a path, use it to determine the target
      String pathStr = entry.get("path").toString();
      Path entryPath = Path.of(pathStr);

      if (entryPath.isAbsolute()) {
        // If the path is absolute, use it directly
        targetPath = entryPath;
      } else {
        // If the path is relative, resolve it against the entry directory
        targetPath = entryDir.resolve(entryPath);
      }
    } else {
      // If there's no path, use the entry directory itself
      targetPath = entryDir;
    }

    // Make the target path absolute for consistent relativization
    Path absoluteTargetPath = targetPath.toAbsolutePath().normalize();

    // Add to each parent directory up to the root
    while (currentDir != null) {
      try {
        // Create a copy of the entry for this directory level
        Map<String, Object> relativizedEntry = new HashMap<>(entry);

        // Make the current directory absolute for consistent relativization
        Path absoluteCurrentDir = currentDir.toAbsolutePath().normalize();

        // Relativize the target path against the current directory
        Path relativePath = absoluteCurrentDir.relativize(absoluteTargetPath);
        relativizedEntry.put("path", relativePath.toString());

        // Add the relativized entry to the current directory's catalog
        // Use computeIfAbsent to ensure the directory has an entry in the map
        List<Map<String, Object>> dirEntries = catalogsByDirectory.computeIfAbsent(currentDir, k -> new ArrayList<>());

        // Check if an entry with the same path already exists to avoid duplicates
        boolean entryExists = dirEntries.stream()
            .anyMatch(e -> e.containsKey("path") &&
                     e.get("path").equals(relativizedEntry.get("path")));

        if (!entryExists) {
            dirEntries.add(relativizedEntry);
        }
      } catch (IllegalArgumentException e) {
        // If relativization fails, log and skip this entry for this directory
        logger.debug("Could not relativize path for parent catalog: {} -> {}", currentDir, targetPath, e);
      }

      // Move up to the parent directory
      currentDir = currentDir.getParent();
    }
  }

  /**
   * Finds the common parent directory from a set of paths.
   *
   * @param paths The set of paths to find the common parent for
   * @return The common parent directory, or null if no common parent is found
   */
  private static Path findCommonParent(Set<Path> paths) {
    if (paths.isEmpty()) {
      return null;
    }

    // Convert to list for easier iteration
    List<Path> pathList = new ArrayList<>(paths);

    // Normalize all paths to absolute paths
    List<Path> absolutePaths = pathList.stream()
        .map(p -> p.toAbsolutePath().normalize())
        .toList();

    // Start with the first path
    Path commonParent = absolutePaths.get(0);

    // For each additional path, find the common parent
    for (int i = 1; i < absolutePaths.size(); i++) {
      Path currentPath = absolutePaths.get(i);

      // Find the common parent between the current common parent and this path
      commonParent = findCommonParentBetweenPaths(commonParent, currentPath);
    }

    return commonParent;
  }

  /**
   * Finds the common parent between two paths.
   *
   * @param path1 The first path
   * @param path2 The second path
   * @return The common parent path
   */
  private static Path findCommonParentBetweenPaths(Path path1, Path path2) {
    // Convert paths to strings for easier comparison
    String str1 = path1.toString();
    String str2 = path2.toString();

    // Find the common prefix
    int commonPrefixLength = 0;
    int minLength = Math.min(str1.length(), str2.length());

    for (int i = 0; i < minLength; i++) {
      if (str1.charAt(i) == str2.charAt(i)) {
        commonPrefixLength++;
      } else {
        break;
      }
    }

    // Find the last directory separator in the common prefix
    int lastSeparatorPos = str1.substring(0, commonPrefixLength).lastIndexOf(File.separator);

    if (lastSeparatorPos >= 0) {
      return Path.of(str1.substring(0, lastSeparatorPos));
    } else {
      // If no common parent found, return the root directory
      return Path.of("/");
    }
  }

  /**
   * Checks if a path is a subpath of another path.
   *
   * @param path The path to check
   * @param possibleParent The possible parent path
   * @return true if path is a subpath of possibleParent, false otherwise
   */
  private static boolean isSubPathOf(Path path, Path possibleParent) {
    // Normalize both paths for comparison
    Path normalizedPath = path.toAbsolutePath().normalize();
    Path normalizedParent = possibleParent.toAbsolutePath().normalize();

    // Check if the path starts with the parent path
    return normalizedPath.startsWith(normalizedParent);
  }

  /**
   * Legacy method for loading a directory (non-recursive).
   * Kept for backward compatibility.
   */
  private static List<Map<String, Object>> loadDirectory(Path directory) {
    List<Map<String, Object>> entries = new ArrayList<>();
    Path layoutPath = directory.resolve("dataset.yaml");
    if (Files.exists(layoutPath)) {
      Map<String, Object> map = loadDatasetYaml(layoutPath);
      entries.add(map);
    } else {
      try {
        try (DirectoryStream<Path> dirstream = Files.newDirectoryStream(directory)) {
          for (Path path : dirstream) {
            if (Files.isDirectory(path)) {
              List<Map<String, Object>> subEntries = loadDirectory(path);
              entries.addAll(subEntries);
            } else if (path.toString().endsWith(".hdf5") && Files.isRegularFile(path)) {
              Map<String, Object> map = loadHdf5File(path);
              entries.add(map);
            }
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return entries;
  }

  private static Map<String, Object> loadDatasetYaml(Path layoutPath) {
    TestGroupLayout layout = TestGroupLayout.load(layoutPath);
    // Use the parent directory name as the path, not the full path
    // The path will be properly relativized later in addEntryToParentCatalogs and save methods
    return Map.of("layout",layout.toData(),"path",layoutPath.toString());
  }

  private static Map<String, Object> loadHdf5File(Path path) {
    try {
      Map<String, Object> map = jsonSummarizer.describeFile(path);
      // Add the path to the map for reference
      // The path will be properly relativized later in addEntryToParentCatalogs and save methods
      map.put("path", path.toString());
      return map;
    } catch (Exception e) {
      // For invalid HDF5 files, create a minimal entry instead of failing
      logger.warn("Could not read HDF5 file as valid HDF5: {} ({})", path, e.getMessage());
      Map<String, Object> map = new HashMap<>();
      // The path will be properly relativized later in addEntryToParentCatalogs and save methods
      map.put("path", path.toString());
      map.put("error", "Invalid HDF5 file: " + e.getMessage());
      try {
        map.put("file_size", Files.exists(path) ? Files.size(path) : 0);
      } catch (IOException ioe) {
        logger.warn("Could not get file size for {}: {}", path, ioe.getMessage());
        map.put("file_size", -1);
      }
      return map;
    }
  }

  /// save the catalog to a file
  /// @param path
  ///     the path to the file to save to
  public void save(Path path) {
    try {
      // Make sure the parent directory exists
      Path catalogDir = path.getParent();
      if (catalogDir != null && !Files.exists(catalogDir)) {
        Files.createDirectories(catalogDir);
      }

      // Get the absolute path of the catalog directory
      Path absoluteCatalogDir = catalogDir != null ? catalogDir.toAbsolutePath().normalize() : Path.of(".").toAbsolutePath().normalize();

      // Create a new list for relativized entries
      List<Map<String, Object>> relativeEntries = new ArrayList<>();

      // Process all entries to ensure paths are correctly relativized
      for (Map<String, Object> entry : this) {
        // Create a copy of the entry for relativization
        Map<String, Object> relativizedEntry = new HashMap<>(entry);

        if (entry.containsKey("path")) {
          String pathStr = entry.get("path").toString();
          Path entryPath = Path.of(pathStr);

          // Determine the absolute path of the entry
          Path absoluteEntryPath;
          if (entryPath.isAbsolute()) {
            // If the path is already absolute, use it directly
            absoluteEntryPath = entryPath.normalize();
          } else {
            // If the path is relative, resolve it against the catalog directory
            absoluteEntryPath = absoluteCatalogDir.resolve(entryPath).normalize();
          }

          try {
            // Relativize the absolute entry path against the catalog directory
            Path relativePath = absoluteCatalogDir.relativize(absoluteEntryPath);
            relativizedEntry.put("path", relativePath.toString());
          } catch (IllegalArgumentException e) {
            // If relativization fails, keep the original path but log a warning
            logger.warn("Could not relativize path: {} against catalog directory: {}", absoluteEntryPath, absoluteCatalogDir);
            relativizedEntry.put("path", pathStr);
          }
        }

        // Add the relativized entry to the list
        relativeEntries.add(relativizedEntry);
      }

      // Create a new CatalogOut with the relativized entries
      CatalogOut relativeCatalog = new CatalogOut(relativeEntries);

      // Write the JSON and YAML files
      Files.writeString(path, gson.toJson(relativeCatalog));
      Path yamlPath = path.resolveSibling(path.getFileName().toString().replaceFirst("\\.json$", ".yaml"));
      Files.writeString(yamlPath, dump.dumpToString(relativeCatalog));

      logger.info("Saved catalog to {} and {}", path, yamlPath);
    } catch (IOException e) {
      throw new RuntimeException("Error saving catalog to " + path, e);
    }
  }
}
