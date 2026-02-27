/*
 * Copyright (c) nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.nosqlbench.command.common;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

/// Unified specification for dataset sources.
///
/// This class provides a centralized way to specify datasets from multiple source types:
///
/// | Format | Description | Example |
/// |--------|-------------|---------|
/// | `<datasetname>` | Catalog lookup by name | `sift-128` |
/// | `<datasetdir>` | Local directory with dataset.yaml | `./mydata` |
/// | `<datasetpath>` | Local path to dataset.yaml | `./mydata/dataset.yaml` |
/// | `http://...` | Remote dataset base directory (must contain dataset.yaml) | `https://example.com/datasets/sift` |
/// | `http://.../dataset.yaml` | Remote dataset.yaml URL | `https://example.com/datasets/sift/dataset.yaml` |
///
/// ## Resolution Priority
///
/// When an unqualified name is provided (no URL scheme, no path separators):
/// 1. Check if it's a local directory containing dataset.yaml
/// 2. Check if it's a local file path to dataset.yaml
/// 3. Treat it as a catalog dataset name
///
/// ## Usage Examples
///
/// ```java
/// // Parse various spec formats
/// DatasetSpec catalogDataset = DatasetSpec.parse("sift-128");
/// DatasetSpec localDir = DatasetSpec.parse("./mydata");
/// DatasetSpec localYaml = DatasetSpec.parse("./mydata/dataset.yaml");
/// DatasetSpec remoteBase = DatasetSpec.parse("https://example.com/datasets/sift/");
/// DatasetSpec remoteYaml = DatasetSpec.parse("https://example.com/datasets/sift/dataset.yaml");
/// ```
public final class DatasetSpec {

    /// The type of source this specification represents
    public enum SourceType {
        /// Dataset resolved through the catalog by name
        CATALOG,
        /// Local directory containing dataset.yaml
        LOCAL_DIRECTORY,
        /// Local path to a dataset.yaml file
        LOCAL_FILE,
        /// Remote dataset base URL (must contain a dataset.yaml file)
        REMOTE_BASE,
        /// Remote dataset.yaml URL
        REMOTE_YAML
    }

    private final SourceType sourceType;
    private final String rawSpec;

    // For CATALOG
    private final String datasetName;

    // For LOCAL_DIRECTORY and LOCAL_FILE
    private final Path localPath;

    // For REMOTE_BASE and REMOTE_YAML
    private final URI remoteUri;

    private DatasetSpec(SourceType sourceType, String rawSpec, String datasetName,
                       Path localPath, URI remoteUri) {
        this.sourceType = sourceType;
        this.rawSpec = rawSpec;
        this.datasetName = datasetName;
        this.localPath = localPath;
        this.remoteUri = remoteUri;
    }

    /// Parse a dataset specification string into a DatasetSpec.
    ///
    /// @param spec The specification string to parse
    /// @return A DatasetSpec representing the parsed specification
    /// @throws IllegalArgumentException if the specification is invalid
    public static DatasetSpec parse(String spec) {
        if (spec == null || spec.trim().isEmpty()) {
            throw new IllegalArgumentException("Dataset spec must not be null or empty");
        }

        String trimmed = spec.trim();

        // Check for HTTP/HTTPS URLs first
        if (trimmed.startsWith("http://") || trimmed.startsWith("https://")) {
            return parseRemoteSpec(trimmed);
        }

        // Check if it looks like a path (contains path separators or starts with . or /)
        if (looksLikePath(trimmed)) {
            return parseLocalSpec(trimmed);
        }

        // Check if it's a local directory or file that exists
        Path possiblePath = expandPath(trimmed);
        if (Files.isDirectory(possiblePath)) {
            // Check if it contains dataset.yaml
            Path datasetYaml = possiblePath.resolve("dataset.yaml");
            Path datasetYml = possiblePath.resolve("dataset.yml");
            if (Files.exists(datasetYaml) || Files.exists(datasetYml)) {
                return new DatasetSpec(SourceType.LOCAL_DIRECTORY, trimmed, null, possiblePath, null);
            }
            // Directory exists but no dataset.yaml - might be an error, but let's treat as catalog
            // and let the resolution phase handle the error
        }

        if (Files.exists(possiblePath) && !Files.isDirectory(possiblePath)) {
            // It's a file - check if it's a dataset.yaml
            String fileName = possiblePath.getFileName().toString().toLowerCase();
            if (fileName.equals("dataset.yaml") || fileName.equals("dataset.yml")) {
                return new DatasetSpec(SourceType.LOCAL_FILE, trimmed, null, possiblePath, null);
            }
        }

        // Default: treat as catalog dataset name
        validateDatasetName(trimmed);
        return new DatasetSpec(SourceType.CATALOG, trimmed, trimmed, null, null);
    }

    /// Check if the spec looks like a filesystem path
    private static boolean looksLikePath(String spec) {
        return spec.startsWith("./") ||
               spec.startsWith("../") ||
               spec.startsWith("/") ||
               spec.startsWith("~") ||
               spec.startsWith("${HOME}") ||
               spec.contains("/") ||
               spec.contains("\\") ||
               spec.endsWith("dataset.yaml") ||
               spec.endsWith("dataset.yml");
    }

    /// Parse a local path specification
    private static DatasetSpec parseLocalSpec(String spec) {
        Path path = expandPath(spec);

        // If it ends with dataset.yaml or dataset.yml, it's a file reference
        String fileName = path.getFileName().toString().toLowerCase();
        if (fileName.equals("dataset.yaml") || fileName.equals("dataset.yml")) {
            if (!Files.exists(path)) {
                throw new IllegalArgumentException(
                    "Dataset file not found: " + path + "\n" +
                    "Please verify the path exists and is readable.");
            }
            return new DatasetSpec(SourceType.LOCAL_FILE, spec, null, path, null);
        }

        // Otherwise it should be a directory
        if (Files.isDirectory(path)) {
            // Verify dataset.yaml exists
            Path datasetYaml = path.resolve("dataset.yaml");
            Path datasetYml = path.resolve("dataset.yml");
            if (!Files.exists(datasetYaml) && !Files.exists(datasetYml)) {
                throw new IllegalArgumentException(
                    "Directory '" + path + "' does not contain a dataset.yaml file.\n" +
                    "Expected to find: " + datasetYaml);
            }
            return new DatasetSpec(SourceType.LOCAL_DIRECTORY, spec, null, path, null);
        }

        if (!Files.exists(path)) {
            throw new IllegalArgumentException(
                "Path not found: " + path + "\n" +
                "Please verify the path exists.");
        }

        throw new IllegalArgumentException(
            "Path '" + path + "' is not a directory or dataset.yaml file.");
    }

    /// Parse a remote URL specification.
    /// URLs ending with `/dataset.yaml` or `/dataset.yml` are treated as REMOTE_YAML.
    /// All other URLs (with or without trailing slash) are treated as REMOTE_BASE,
    /// which assumes a `dataset.yaml` file exists at that location.
    private static DatasetSpec parseRemoteSpec(String spec) {
        URI uri;
        try {
            uri = URI.create(spec);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid URL: " + spec, e);
        }

        // Check for explicit dataset.yaml reference
        if (spec.endsWith("/dataset.yaml") || spec.endsWith("/dataset.yml")) {
            return new DatasetSpec(SourceType.REMOTE_YAML, spec, null, null, uri);
        }

        // All other URLs are treated as base directory references.
        // Normalize by ensuring trailing slash for consistent base URL handling.
        String normalizedSpec = spec.endsWith("/") ? spec : spec + "/";
        URI normalizedUri = URI.create(normalizedSpec);
        return new DatasetSpec(SourceType.REMOTE_BASE, spec, null, null, normalizedUri);
    }

    /// Validate a catalog dataset name
    private static void validateDatasetName(String name) {
        if (name.isEmpty()) {
            throw new IllegalArgumentException("Dataset name must not be empty");
        }
        // Dataset names should be alphanumeric with dashes, underscores, and dots
        if (!name.matches("^[a-zA-Z0-9][a-zA-Z0-9._-]*$")) {
            throw new IllegalArgumentException(
                "Invalid dataset name '" + name + "'.\n" +
                "Dataset names must start with alphanumeric and contain only " +
                "alphanumeric characters, dashes, underscores, and dots.");
        }
    }

    /// Expand path variables like ~ and ${HOME}
    private static Path expandPath(String pathStr) {
        String expanded = pathStr
            .replace("~", System.getProperty("user.home"))
            .replace("${HOME}", System.getProperty("user.home"));
        return Path.of(expanded);
    }

    /// Returns the source type of this specification.
    /// @return The source type of this specification
    public SourceType getSourceType() {
        return sourceType;
    }

    /// Returns the raw specification string as provided.
    /// @return The raw specification string as provided
    public String getRawSpec() {
        return rawSpec;
    }

    /// Returns the catalog dataset name, if this is a CATALOG spec.
    /// @return The catalog dataset name, if this is a CATALOG spec
    public Optional<String> getDatasetName() {
        return Optional.ofNullable(datasetName);
    }

    /// Returns the local path, if this is a LOCAL_* spec.
    /// @return The local path, if this is a LOCAL_* spec
    public Optional<Path> getLocalPath() {
        return Optional.ofNullable(localPath);
    }

    /// Returns the remote URI, if this is a REMOTE_* spec.
    /// @return The remote URI, if this is a REMOTE_* spec
    public Optional<URI> getRemoteUri() {
        return Optional.ofNullable(remoteUri);
    }

    /// Checks whether this spec represents a catalog dataset lookup.
    /// @return true if this spec represents a catalog dataset lookup
    public boolean isCatalog() {
        return sourceType == SourceType.CATALOG;
    }

    /// Checks whether this spec represents a local dataset (directory or file).
    /// @return true if this spec represents a local dataset (directory or file)
    public boolean isLocal() {
        return sourceType == SourceType.LOCAL_DIRECTORY || sourceType == SourceType.LOCAL_FILE;
    }

    /// Checks whether this spec represents a local directory.
    /// @return true if this spec represents a local directory
    public boolean isLocalDirectory() {
        return sourceType == SourceType.LOCAL_DIRECTORY;
    }

    /// Checks whether this spec represents a local dataset.yaml file.
    /// @return true if this spec represents a local dataset.yaml file
    public boolean isLocalFile() {
        return sourceType == SourceType.LOCAL_FILE;
    }

    /// Checks whether this spec represents a remote dataset.
    /// @return true if this spec represents a remote dataset
    public boolean isRemote() {
        return sourceType == SourceType.REMOTE_BASE || sourceType == SourceType.REMOTE_YAML;
    }

    /// Checks whether this spec represents a remote base URL.
    /// @return true if this spec represents a remote base URL
    public boolean isRemoteBase() {
        return sourceType == SourceType.REMOTE_BASE;
    }

    /// Checks whether this spec represents a remote dataset.yaml URL.
    /// @return true if this spec represents a remote dataset.yaml URL
    public boolean isRemoteYaml() {
        return sourceType == SourceType.REMOTE_YAML;
    }

    /// Get the dataset.yaml URL for remote specs.
    /// For REMOTE_BASE, appends "dataset.yaml" to the base URL.
    /// For REMOTE_YAML, returns the URL as-is.
    ///
    /// @return The dataset.yaml URL, or empty if not a remote spec
    public Optional<String> getDatasetYamlUrl() {
        if (sourceType == SourceType.REMOTE_BASE) {
            return Optional.of(remoteUri.toString() + "dataset.yaml");
        } else if (sourceType == SourceType.REMOTE_YAML) {
            return Optional.of(remoteUri.toString());
        }
        return Optional.empty();
    }

    /// Get the base URL for remote specs (without dataset.yaml).
    /// For REMOTE_BASE, returns the URL as-is.
    /// For REMOTE_YAML, removes "dataset.yaml" from the end.
    ///
    /// @return The base URL, or empty if not a remote spec
    public Optional<String> getDatasetBaseUrl() {
        if (sourceType == SourceType.REMOTE_BASE) {
            return Optional.of(remoteUri.toString());
        } else if (sourceType == SourceType.REMOTE_YAML) {
            String url = remoteUri.toString();
            if (url.endsWith("/dataset.yaml")) {
                return Optional.of(url.substring(0, url.length() - "dataset.yaml".length()));
            } else if (url.endsWith("/dataset.yml")) {
                return Optional.of(url.substring(0, url.length() - "dataset.yml".length()));
            }
        }
        return Optional.empty();
    }

    /// Get the local dataset.yaml path.
    /// For LOCAL_DIRECTORY, appends "dataset.yaml" to the directory path.
    /// For LOCAL_FILE, returns the path as-is.
    ///
    /// @return The dataset.yaml path, or empty if not a local spec
    public Optional<Path> getDatasetYamlPath() {
        if (sourceType == SourceType.LOCAL_DIRECTORY) {
            Path yamlPath = localPath.resolve("dataset.yaml");
            if (Files.exists(yamlPath)) {
                return Optional.of(yamlPath);
            }
            Path ymlPath = localPath.resolve("dataset.yml");
            if (Files.exists(ymlPath)) {
                return Optional.of(ymlPath);
            }
            return Optional.of(yamlPath); // Return expected path even if not found
        } else if (sourceType == SourceType.LOCAL_FILE) {
            return Optional.of(localPath);
        }
        return Optional.empty();
    }

    /// Get the local dataset directory.
    /// For LOCAL_DIRECTORY, returns the directory path.
    /// For LOCAL_FILE, returns the parent directory.
    ///
    /// @return The dataset directory path, or empty if not a local spec
    public Optional<Path> getDatasetDirectory() {
        if (sourceType == SourceType.LOCAL_DIRECTORY) {
            return Optional.of(localPath);
        } else if (sourceType == SourceType.LOCAL_FILE) {
            return Optional.of(localPath.getParent());
        }
        return Optional.empty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DatasetSpec that)) return false;
        return sourceType == that.sourceType &&
               Objects.equals(rawSpec, that.rawSpec) &&
               Objects.equals(datasetName, that.datasetName) &&
               Objects.equals(localPath, that.localPath) &&
               Objects.equals(remoteUri, that.remoteUri);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceType, rawSpec, datasetName, localPath, remoteUri);
    }

    @Override
    public String toString() {
        return rawSpec;
    }

    /// Create a descriptive string for display purposes.
    /// @return a human-readable description of this spec
    public String toDescription() {
        return switch (sourceType) {
            case CATALOG -> "Catalog dataset: " + datasetName;
            case LOCAL_DIRECTORY -> "Local directory: " + localPath;
            case LOCAL_FILE -> "Local file: " + localPath;
            case REMOTE_BASE -> "Remote dataset: " + remoteUri;
            case REMOTE_YAML -> "Remote dataset.yaml: " + remoteUri;
        };
    }
}
