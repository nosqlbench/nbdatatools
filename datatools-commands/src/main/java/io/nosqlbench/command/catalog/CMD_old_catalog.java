package io.nosqlbench.command.catalog;

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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/// Create catalog views of vector test data files and dataset directories
@CommandLine.Command(name = "old_catalog",
    headerHeading = "Usage:%n%n",
    synopsisHeading = "%n",
    descriptionHeading = "%nDescription%n%n",
    parameterListHeading = "%nParameters:%n%",
    optionListHeading = "%nOptions:%n",
    header="Create catalog views of vector test data files and dataset directories",
    description = "When given a directory, it will be recursively traversed to find:\n" +
              "1. Directories containing a 'dataset.yaml' file (treated as dataset roots)\n" +
              "2. Individual .hdf5 files\n" +
              "Catalog files (catalog.json and catalog.yaml) will be created at\n" +
              "each directory level, with paths in each catalog being relative\n" +
              "to the location of the catalog file.",
    exitCodeListHeading = "Exit Codes:%n",
    exitCodeList = {
        "0: no errors",
        "1: error processing files or directories"
    },
    subcommands = {CommandLine.HelpCommand.class})
public class CMD_old_catalog implements Callable<Integer> {

  private static final Logger logger = LogManager.getLogger(CMD_old_catalog.class);

  @CommandLine.Parameters(description = "Files and/or directories to catalog; Directories will be recursively traversed "
                                        + "to find dataset.yaml files and .hdf5 files. Catalog files will be created "
                                        + "at each directory level.", defaultValue = ".")
  private List<Path> paths;

  @CommandLine.Option(names = {"--basename"},
      description = "The basename to use for the catalog",
      defaultValue = "catalog")
  private String basename;

  /// run a catalog_hdf5 command
  /// @param args
  ///     command line args
  public static void main(String[] args) {

//    System.setProperty("slf4j.internal.verbosity", "ERROR");
//    System.setProperty(
//        ConfigurationFactory.CONFIGURATION_FACTORY_PROPERTY,
//        CustomConfigurationFactory.class.getCanonicalName()
//    );

    //    System.setProperty("slf4j.internal.verbosity", "DEBUG");
    CMD_old_catalog command = new CMD_old_catalog();
    logger.info("instancing commandline");
    CommandLine commandLine = new CommandLine(command).setCaseInsensitiveEnumValuesAllowed(true)
        .setOptionsCaseInsensitive(true);
    logger.info("executing commandline");
    int exitCode = commandLine.execute(args);
    logger.info("exiting main");
    System.exit(exitCode);
  }

  /**
   * Finds the common parent directory of all paths.
   * If there is no common parent, returns the current directory.
   *
   * @param paths The paths to find the common parent for
   * @return The common parent directory
   */
  private Path findCommonParentDirectory(List<Path> paths) {
    if (paths.isEmpty()) {
      return Path.of(".");
    }

    // Normalize all paths to absolute paths
    List<Path> absolutePaths = paths.stream()
        .map(p -> p.toAbsolutePath().normalize())
        .collect(java.util.stream.Collectors.toList());

    // Start with the first path's parent (or the path itself if it's a directory)
    Path firstPath = absolutePaths.get(0);
    Path commonParent = Files.isDirectory(firstPath) ? firstPath : firstPath.getParent();

    // If there's only one path, return its parent directory (or itself if it's a directory)
    if (absolutePaths.size() == 1) {
      return commonParent;
    }

    // For each additional path, find the common parent
    for (int i = 1; i < absolutePaths.size(); i++) {
      Path currentPath = absolutePaths.get(i);
      Path currentParent = Files.isDirectory(currentPath) ? currentPath : currentPath.getParent();

      // Find the common parent between the current common parent and this path's parent
      commonParent = findCommonParent(commonParent, currentParent);
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
  private Path findCommonParent(Path path1, Path path2) {
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

  @Override
  public Integer call() {
    try {
      if (this.paths.isEmpty()) {
        logger.error("No files or directories specified");
        return 1;
      }

      if (basename.contains(".")) {
        logger.error("Basename must not contain extension (or dot character)");
        return 1;
      }

      logger.info("Processing {} paths: {}", this.paths.size(), this.paths);

      // Determine the common parent directory of all paths
      Path commonParent = findCommonParentDirectory(this.paths);
      logger.info("Using common parent directory: {}", commonParent);

      // Process each path individually
      List<Map<String, Object>> allEntries = new ArrayList<>();
      for (Path path : this.paths) {
        if (!Files.exists(path)) {
          logger.error("Path does not exist: {}", path);
          return 1;
        }

        logger.info("Processing path: {}", path);

        // Create catalog for this path and all subdirectories
        List<Map<String, Object>> entries = CatalogBuilder.buildCatalogs(path, basename, commonParent);
        allEntries.addAll(entries);
      }

      // Create a catalog at the common parent directory
      CatalogBuilder.saveCatalog(allEntries, commonParent, basename);

      return 0;
    } catch (Exception e) {
      logger.error("Error creating catalog: {}", e.getMessage(), e);
      return 1;
    }
  }
}
