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


import io.jhdf.HdfFile;
import io.nosqlbench.vectordata.downloader.DatasetEntry;
import io.nosqlbench.vectordata.downloader.VirtualProfileSelector;
import io.nosqlbench.vectordata.layoutv2.DSProfile;
import io.nosqlbench.vectordata.layoutv2.DSProfileGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/// Unified loader for datasets from various sources (URLs, local paths, HDF5, filesystem-based).
///
/// This class provides a simple API for loading datasets without needing to know the underlying
/// format or location. It automatically:
/// - Detects remote URLs vs local paths
/// - For remote URLs: Uses VirtualProfileSelector with on-demand downloading and Merkle verification
/// - For local paths: Detects HDF5 vs filesystem-based formats and uses appropriate implementation
/// - Returns the appropriate ProfileSelector implementation
///
/// Supported sources:
/// - Remote URLs: https://example.com/dataset/ (uses VirtualProfileSelector with MAFileChannel)
/// - Local HDF5 files: /path/to/dataset.hdf5 (uses TestDataGroup)
/// - Local filesystem datasets: /path/to/dataset/ with dataset.yaml (uses FilesystemTestDataGroup)
/// - Tilde expansion: ~/datasets/my-data
///
/// Example usage:
/// <pre>
/// // Load from remote URL
/// try (ProfileSelector dataset = DatasetLoader.load("https://example.com/datasets/my-vectors/")) {
///     TestDataView profile = dataset.profile("default");
///     // ... use the dataset (downloads on-demand with caching)
/// }
///
/// // Load from local HDF5
/// try (ProfileSelector dataset = DatasetLoader.load("/path/to/dataset.hdf5")) {
///     TestDataView profile = dataset.profile("default");
///     // ... use the dataset
/// }
///
/// // Load from local filesystem with dataset.yaml
/// try (ProfileSelector dataset = DatasetLoader.load("/path/to/dataset/")) {
///     TestDataView profile = dataset.profile("default");
///     // ... use the dataset
/// }
/// </pre>
public class DatasetLoader {
    private static final Logger logger = LogManager.getLogger(DatasetLoader.class);

    /// Loads a dataset from a URL string or local path string.
    ///
    /// @param urlOrPath The URL or local path to the dataset
    /// @return A ProfileSelector for accessing the dataset
    /// @throws IOException If the dataset cannot be loaded
    public static ProfileSelector load(String urlOrPath) throws IOException {
        return load(urlOrPath, null);
    }

    /// Loads a dataset from a URL string or local path string with caching.
    ///
    /// @param urlOrPath The URL or local path to the dataset
    /// @param cacheDir Optional cache directory for downloaded datasets (null for default)
    /// @return A ProfileSelector for accessing the dataset
    /// @throws IOException If the dataset cannot be loaded
    public static ProfileSelector load(String urlOrPath, String cacheDir) throws IOException {
        // Expand tilde in paths
        if (urlOrPath.startsWith("~")) {
            urlOrPath = System.getProperty("user.home") + urlOrPath.substring(1);
        }

        // Detect if it's a URL or local path
        if (urlOrPath.startsWith("http://") || urlOrPath.startsWith("https://")) {
            return loadFromUrl(urlOrPath, cacheDir);
        } else {
            return loadFromPath(Path.of(urlOrPath));
        }
    }

    /// Loads a dataset from a URL object.
    ///
    /// @param url The URL to the dataset
    /// @return A ProfileSelector for accessing the dataset
    /// @throws IOException If the dataset cannot be loaded
    public static ProfileSelector load(URL url) throws IOException {
        return load(url, null);
    }

    /// Loads a dataset from a URL object with caching.
    ///
    /// @param url The URL to the dataset
    /// @param cacheDir Optional cache directory for downloaded datasets (null for default)
    /// @return A ProfileSelector for accessing the dataset
    /// @throws IOException If the dataset cannot be loaded
    public static ProfileSelector load(URL url, String cacheDir) throws IOException {
        return loadFromUrl(url.toString(), cacheDir);
    }

    /// Loads a dataset from a local Path.
    ///
    /// @param path The path to the dataset
    /// @return A ProfileSelector for accessing the dataset
    /// @throws IOException If the dataset cannot be loaded
    public static ProfileSelector load(Path path) throws IOException {
        return loadFromPath(path);
    }

    /// Loads a dataset from a remote URL using the existing VirtualProfileSelector infrastructure.
    ///
    /// This leverages the built-in download support with Merkle tree verification and caching.
    ///
    /// @param urlString The URL string
    /// @param cacheDir Optional cache directory (null for default ~/.cache/vectordata)
    /// @return A ProfileSelector for accessing the dataset
    /// @throws IOException If the dataset cannot be accessed
    private static ProfileSelector loadFromUrl(String urlString, String cacheDir) throws IOException {
        logger.info("Loading dataset from remote URL: {}", urlString);

        try {
            URL url = new URL(urlString);

            // Extract dataset name from URL
            String urlPath = url.getPath();
            String datasetName = urlPath.substring(urlPath.lastIndexOf('/') + 1);
            if (datasetName.isEmpty() || datasetName.equals("/")) {
                datasetName = "remote-dataset";
            }

            // Create a DatasetEntry - the existing infrastructure handles the download automatically
            DSProfileGroup profileGroup = new DSProfileGroup();
            DSProfile defaultProfile = new DSProfile();
            profileGroup.put("default", defaultProfile);

            Map<String, String> attributes = new LinkedHashMap<>();
            Map<String, String> tags = new LinkedHashMap<>();

            DatasetEntry entry = new DatasetEntry(datasetName, url, attributes, profileGroup, tags);

            // Use the existing VirtualProfileSelector which handles remote access with
            // on-demand downloading, Merkle verification, and caching
            VirtualProfileSelector selector = (VirtualProfileSelector) entry.select();
            if (cacheDir != null) {
                selector.setCacheDir(cacheDir);
            }

            return selector;

        } catch (MalformedURLException e) {
            throw new IOException("Invalid URL: " + urlString, e);
        }
    }

    /// Loads a dataset from a local path, auto-detecting the format.
    ///
    /// @param path The local path to the dataset
    /// @return A ProfileSelector for accessing the dataset
    /// @throws IOException If the dataset cannot be loaded
    private static ProfileSelector loadFromPath(Path path) throws IOException {
        if (!Files.exists(path)) {
            throw new IOException("Dataset path does not exist: " + path);
        }

        // Decision tree for determining dataset format:
        // 1. If it's a file and ends with .hdf5 or .h5 -> HDF5
        // 2. If it's a file named dataset.yaml -> Filesystem-based (use parent directory)
        // 3. If it's a directory with dataset.yaml -> Filesystem-based
        // 4. If it's a file that can be opened as HDF5 -> HDF5
        // 5. Otherwise -> error

        if (Files.isDirectory(path)) {
            return loadFromDirectory(path);
        } else {
            return loadFromFile(path);
        }
    }

    /// Loads a dataset from a directory path.
    ///
    /// @param dirPath The directory path
    /// @return A ProfileSelector for accessing the dataset
    /// @throws IOException If the dataset cannot be loaded
    private static ProfileSelector loadFromDirectory(Path dirPath) throws IOException {
        logger.debug("Loading dataset from directory: {}", dirPath);

        // Check for dataset.yaml (filesystem-based)
        Path yamlPath = dirPath.resolve("dataset.yaml");
        if (Files.exists(yamlPath)) {
            logger.info("Detected filesystem-based dataset (dataset.yaml found)");
            return new FilesystemTestDataGroup(dirPath);
        }

        // Check for HDF5 files in the directory
        Path[] hdf5Files = Files.list(dirPath)
            .filter(p -> {
                String name = p.getFileName().toString().toLowerCase();
                return name.endsWith(".hdf5") || name.endsWith(".h5");
            })
            .limit(2)
            .toArray(Path[]::new);

        if (hdf5Files.length == 1) {
            logger.info("Detected single HDF5 file in directory: {}", hdf5Files[0]);
            return new TestDataGroup(hdf5Files[0]);
        } else if (hdf5Files.length > 1) {
            throw new IOException("Multiple HDF5 files found in directory " + dirPath +
                ". Please specify which file to load.");
        }

        throw new IOException("No dataset.yaml or HDF5 files found in directory: " + dirPath);
    }

    /// Loads a dataset from a file path.
    ///
    /// @param filePath The file path
    /// @return A ProfileSelector for accessing the dataset
    /// @throws IOException If the dataset cannot be loaded
    private static ProfileSelector loadFromFile(Path filePath) throws IOException {
        String fileName = filePath.getFileName().toString();
        logger.debug("Loading dataset from file: {}", filePath);

        // Check if it's dataset.yaml
        if (fileName.equals("dataset.yaml")) {
            logger.info("Detected dataset.yaml file, using parent directory");
            return new FilesystemTestDataGroup(filePath);
        }

        // Check if it's an HDF5 file by extension
        String lowerName = fileName.toLowerCase();
        if (lowerName.endsWith(".hdf5") || lowerName.endsWith(".h5")) {
            logger.info("Detected HDF5 file by extension");
            return new TestDataGroup(filePath);
        }

        // Try to open as HDF5 as a fallback
        try {
            logger.debug("Attempting to open as HDF5 file");
            HdfFile hdfFile = new HdfFile(filePath);
            hdfFile.close();
            logger.info("Successfully opened as HDF5 file");
            return new TestDataGroup(filePath);
        } catch (Exception e) {
            logger.debug("Failed to open as HDF5: {}", e.getMessage());
            throw new IOException("Could not determine dataset format for file: " + filePath +
                ". Expected HDF5 file or dataset.yaml", e);
        }
    }

    /// Attempts to detect if a path points to an HDF5 file.
    ///
    /// @param path The path to check
    /// @return true if the path appears to be an HDF5 file
    private static boolean isHdf5File(Path path) {
        if (!Files.isRegularFile(path)) {
            return false;
        }

        String fileName = path.getFileName().toString().toLowerCase();
        if (fileName.endsWith(".hdf5") || fileName.endsWith(".h5")) {
            return true;
        }

        // Try to open as HDF5
        try {
            HdfFile hdfFile = new HdfFile(path);
            hdfFile.close();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /// Attempts to detect if a path points to a filesystem-based dataset.
    ///
    /// @param path The path to check
    /// @return true if the path appears to be a filesystem-based dataset
    private static boolean isFilesystemDataset(Path path) {
        if (Files.isDirectory(path)) {
            return Files.exists(path.resolve("dataset.yaml"));
        } else if (Files.isRegularFile(path)) {
            return path.getFileName().toString().equals("dataset.yaml");
        }
        return false;
    }
}
