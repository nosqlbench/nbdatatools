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


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.nosqlbench.command.json.subcommands.export_json.Hdf5JsonSummarizer;
import io.nosqlbench.vectordata.layout.TestGroupLayout;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.snakeyaml.engine.v2.api.Dump;
import org.snakeyaml.engine.v2.api.DumpSettings;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

 /// Builds catalog files for HDF5 files and dataset.yaml files.
 /// The catalog files are created at each directory level, with paths relative to the catalog location.
public class CatalogBuilder {
    private static final Logger logger = LogManager.getLogger(CatalogBuilder.class);
    private static final Hdf5JsonSummarizer jsonSummarizer = new Hdf5JsonSummarizer();
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private static final Dump dump = new Dump(DumpSettings.builder().build());

    /// Create the default CatalogBuilder instance
    public CatalogBuilder() {}
    /**
     * Builds catalog files for the specified path and all subdirectories.
     * The catalog files are created at each directory level, with paths relative to the catalog location.
     *
     * @param path The path to process
     * @param basename The basename to use for catalog files (default: "catalog")
     * @param commonParent The common parent directory for all paths
     * @return A list of catalog entries for the path
     * @throws IOException If an error occurs while processing the path
     */
    public static List<Map<String, Object>> buildCatalogs(Path path, String basename, Path commonParent) throws IOException {
        if (basename == null || basename.isEmpty()) {
            basename = "catalog";
        }

        List<Map<String, Object>> entries = new ArrayList<>();

        if (Files.isDirectory(path)) {
            // Process directory recursively, starting from the innermost directories
            entries = processDirectoryRecursively(path, basename, commonParent);
        } else if (Files.isRegularFile(path) && path.toString().endsWith(".hdf5")) {
            // For individual HDF5 files, create a catalog in the parent directory
            Path parentDir = path.getParent();
            if (parentDir != null) {
                Map<String, Object> entry = createHdf5Entry(path, commonParent);
                entries.add(entry);

                // Create a catalog in the parent directory
                List<Map<String, Object>> parentEntries = new ArrayList<>();
                parentEntries.add(createHdf5Entry(path, parentDir));
                saveCatalog(parentEntries, parentDir, basename);
            }
        } else {
            throw new IOException("Not a valid file or directory: " + path);
        }

        return entries;
    }

    /**
     * Recursively processes a directory to find dataset.yaml files and .hdf5 files.
     * Creates catalog files at each directory level, with paths relative to the catalog location.
     *
     * @param directory The directory to process
     * @param basename The basename to use for catalog files
     * @param commonParent The common parent directory for all paths
     * @return A list of catalog entries for the directory
     * @throws IOException If an error occurs while processing the directory
     */
    private static List<Map<String, Object>> processDirectoryRecursively(Path directory, String basename, Path commonParent) throws IOException {
        // First, process all subdirectories recursively
        List<Path> subdirectories = new ArrayList<>();
        List<Path> hdf5Files = new ArrayList<>();
        Path datasetYaml = directory.resolve("dataset.yaml");
        boolean isDatasetRoot = Files.exists(datasetYaml);

        // Create a list to collect all entries for this directory and subdirectories
        List<Map<String, Object>> allEntries = new ArrayList<>();

        // If this is a dataset root, don't process subdirectories
        if (!isDatasetRoot) {
            try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(directory)) {
                for (Path path : dirStream) {
                    if (Files.isDirectory(path)) {
                        subdirectories.add(path);
                    } else if (Files.isRegularFile(path) && path.toString().endsWith(".hdf5")) {
                        hdf5Files.add(path);
                    }
                }
            }

            // Process subdirectories first (depth-first)
            for (Path subdir : subdirectories) {
                List<Map<String, Object>> subdirEntries = processDirectoryRecursively(subdir, basename, commonParent);
                allEntries.addAll(subdirEntries);
            }
        }

        // Now create a catalog for this directory
        List<Map<String, Object>> dirEntries = new ArrayList<>();

        // Add dataset.yaml entry if it exists
        if (isDatasetRoot) {
            Map<String, Object> datasetEntry = createDatasetEntry(datasetYaml, directory);
            dirEntries.add(datasetEntry);

            // Also add an entry relative to the common parent
            Map<String, Object> commonParentEntry = createDatasetEntry(datasetYaml, commonParent);
            allEntries.add(commonParentEntry);
        }

        // Add HDF5 file entries
        for (Path hdf5File : hdf5Files) {
            Map<String, Object> dirEntry = createHdf5Entry(hdf5File, directory);
            dirEntries.add(dirEntry);

            // Also add an entry relative to the common parent
            Map<String, Object> commonParentEntry = createHdf5Entry(hdf5File, commonParent);
            allEntries.add(commonParentEntry);
        }

        // Save the catalog for this directory
        saveCatalog(dirEntries, directory, basename);

        // Add all entries from this directory to the result
        allEntries.addAll(dirEntries);

        return allEntries;
    }

    /**
     * Creates an entry for a dataset.yaml file.
     *
     * @param datasetYaml The path to the dataset.yaml file
     * @param catalogDir The directory where the catalog will be saved
     * @return A map representing the dataset entry
     */
    private static Map<String, Object> createDatasetEntry(Path datasetYaml, Path catalogDir) {
        TestGroupLayout layout = TestGroupLayout.load(datasetYaml);
        Map<String, Object> entry = new HashMap<>();
        entry.put("layout", layout.toData());

        // Set the path relative to the catalog directory
        Path relativePath = catalogDir.relativize(datasetYaml);
        entry.put("path", relativePath.toString());

        return entry;
    }

    /**
     * Creates an entry for an HDF5 file.
     *
     * @param hdf5File The path to the HDF5 file
     * @param catalogDir The directory where the catalog will be saved
     * @return A map representing the HDF5 file entry
     */
    private static Map<String, Object> createHdf5Entry(Path hdf5File, Path catalogDir) {
        Map<String, Object> entry = new HashMap<>();

        try {
            // Try to read the HDF5 file
            entry = jsonSummarizer.describeFile(hdf5File);
        } catch (Exception e) {
            // For invalid HDF5 files, create a minimal entry
            logger.warn("Could not read HDF5 file as valid HDF5: {} ({})", hdf5File, e.getMessage());
            entry.put("error", "Invalid HDF5 file: " + e.getMessage());
            try {
                entry.put("file_size", Files.exists(hdf5File) ? Files.size(hdf5File) : 0);
            } catch (IOException ioe) {
                logger.warn("Could not get file size for {}: {}", hdf5File, ioe.getMessage());
                entry.put("file_size", -1);
            }
        }

        // Set the path relative to the catalog directory
        Path relativePath = catalogDir.relativize(hdf5File);
        entry.put("path", relativePath.toString());

        return entry;
    }

    /**
     * Saves a catalog to JSON and YAML files.
     *
     * @param entries The entries to include in the catalog
     * @param directory The directory where to save the catalog
     * @param basename The basename to use for the catalog files
     * @throws IOException If an error occurs while saving the catalog
     */
    public static void saveCatalog(List<Map<String, Object>> entries, Path directory, String basename) throws IOException {
        if (entries.isEmpty()) {
            logger.info("No entries found for directory: {}", directory);
            return;
        }

        // Create the catalog files
        Path jsonPath = directory.resolve(basename + ".json");
        Path yamlPath = directory.resolve(basename + ".yaml");

        // Convert entries to JSON
        String json = gson.toJson(entries);
        Files.writeString(jsonPath, json);

        // Convert entries to YAML
        String yamlContent = dump.dumpToString(entries);
        Files.writeString(yamlPath, yamlContent);

        logger.info("Created catalog at {} and {} with {} entries", jsonPath, yamlPath, entries.size());
    }
}
