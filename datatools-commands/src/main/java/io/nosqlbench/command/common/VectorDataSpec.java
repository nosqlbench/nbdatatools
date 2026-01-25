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

import io.nosqlbench.common.types.VectorFileExtension;
import io.nosqlbench.vectordata.spec.datasets.types.TestDataKind;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/// Unified specification for vector data sources.
///
/// This class provides a centralized way to specify vector data from multiple source types:
///
/// | Format | Description | Example |
/// |--------|-------------|---------|
/// | `file:<filepath>` | Local file | `file:./data/vectors.fvec` |
/// | `<filepath>` (unqualified) | Defaults to `file:` | `./data/vectors.fvec` |
/// | `<name>.<profile>.<facet>` | Catalog facet (shorthand, preferred) | `sift-128.default.base` |
/// | `<name>:<profile>:<facet>` | Catalog facet (shorthand, compat) | `sift-128:default:base` |
/// | `facet.<name>.<profile>.<facet>` | Catalog-resolved facet | `facet.sift-128.default.base` |
/// | `facet:<name>:<profile>:<facet>` | Catalog-resolved facet (compat) | `facet:sift-128:default:base` |
/// | `facet:<dirpath>:<profile>:<facet>` | Local dataset.yaml facet | `facet:./mydata:default:base` |
/// | `http://...` or `https://...` | Remote file URL | `https://example.com/vectors.fvec` |
/// | `http://.../dataset.yaml` | Remote dataset.yaml | `https://example.com/datasets/sift/dataset.yaml` |
///
/// Note: URLs ending with `/` (dataset base) are ambiguous and will prompt for a specific facet.
///
/// ## Usage Examples
///
/// ```java
/// // Parse various spec formats
/// VectorDataSpec localFile = VectorDataSpec.parse("./vectors.fvec");
/// VectorDataSpec explicitFile = VectorDataSpec.parse("file:./vectors.fvec");
/// VectorDataSpec catalogFacet = VectorDataSpec.parse("sift-128.default.base"); // shorthand
/// VectorDataSpec catalogFacet2 = VectorDataSpec.parse("facet.sift-128.default.base");
/// VectorDataSpec remoteFile = VectorDataSpec.parse("https://example.com/vectors.fvec");
/// ```
public final class VectorDataSpec {

    /// The type of source this specification represents
    public enum SourceType {
        /// Local file specified with `file:` prefix or unqualified path
        LOCAL_FILE,
        /// Local facet from a directory containing dataset.yaml
        LOCAL_FACET,
        /// Facet resolved through the catalog by dataset name
        CATALOG_FACET,
        /// Remote file accessed via HTTP/HTTPS
        REMOTE_FILE,
        /// Remote dataset.yaml URL (ends with `/dataset.yaml`)
        REMOTE_DATASET_YAML
    }

    /// Exception thrown when a dataset base URL is provided without specifying a facet.
    /// This exception includes the base URL so commands can fetch the dataset.yaml
    /// and suggest valid facets to the user.
    public static class AmbiguousDatasetBaseException extends IllegalArgumentException {
        private final String baseUrl;

        public AmbiguousDatasetBaseException(String baseUrl) {
            super("Dataset base URL '" + baseUrl + "' is ambiguous. " +
                  "Please specify a facet (base, query, indices, or distances). " +
                  "The system will check for available facets and suggest alternatives.");
            this.baseUrl = baseUrl;
        }

        /// @return The ambiguous base URL that was provided
        public String getBaseUrl() {
            return baseUrl;
        }

        /// @return The URL to fetch dataset.yaml from
        public String getDatasetYamlUrl() {
            return baseUrl + "dataset.yaml";
        }
    }

    /// Exception thrown when a dataset/profile spec is provided without a facet.
    /// The format `<dataset>:<profile>` or `<dataset>.<profile>` is a profile selector, not a vector source.
    /// Vector sources require the facet component: `<dataset>.<profile>.<facet>` or `<dataset>:<profile>:<facet>`.
    public static class IncompleteDatasetSpecException extends IllegalArgumentException {
        private final String datasetName;
        private final String profileName;
        private final String rawSpec;

        public IncompleteDatasetSpecException(String datasetName, String profileName) {
            this(datasetName, profileName, datasetName + ":" + profileName);
        }

        public IncompleteDatasetSpecException(String datasetName, String profileName, String rawSpec) {
            super(buildMessage(datasetName, profileName, rawSpec));
            this.datasetName = datasetName;
            this.profileName = profileName;
            this.rawSpec = rawSpec;
        }

        private static String buildMessage(String datasetName, String profileName, String rawSpec) {
            return "Incomplete vector data spec: '" + rawSpec + "'\n\n" +
                   "The format '<dataset>:<profile>' (or '<dataset>.<profile>') is a profile selector, not a direct vector source.\n" +
                   "To specify vectors, you must include a facet (data type).\n\n" +
                   "Did you mean '" + datasetName + "." + profileName + ".<facet>'? For example:\n" +
                   "  " + datasetName + "." + profileName + ".base        (base vectors for indexing)\n" +
                   "  " + datasetName + "." + profileName + ".query       (query vectors for searching)\n" +
                   "  " + datasetName + "." + profileName + ".indices     (ground truth neighbor indices)\n" +
                   "  " + datasetName + "." + profileName + ".distances   (ground truth neighbor distances)\n\n" +
                   "Colon separators are also accepted for compatibility:\n" +
                   "  " + datasetName + ":" + profileName + ":base\n" +
                   "  " + datasetName + ":" + profileName + ":query\n" +
                   "  " + datasetName + ":" + profileName + ":indices\n" +
                   "  " + datasetName + ":" + profileName + ":distances\n\n" +
                   "Available facets:\n" +
                   "  base, base_vectors     - Base vectors for building the index\n" +
                   "  query, query_vectors   - Query vectors for searching\n" +
                   "  indices, neighbor_indices   - Ground truth neighbor indices\n" +
                   "  distances, neighbor_distances - Ground truth neighbor distances";
        }

        /// @return The dataset name from the spec
        public String getDatasetName() {
            return datasetName;
        }

        /// @return The profile name from the spec
        public String getProfileName() {
            return profileName;
        }

        /// @return The raw spec that was provided
        public String getRawSpec() {
            return rawSpec;
        }
    }

    private final SourceType sourceType;
    private final String rawSpec;

    // For LOCAL_FILE
    private final Path localPath;

    // For REMOTE_FILE, REMOTE_DATASET_BASE, REMOTE_DATASET_YAML
    private final URI remoteUri;

    // For LOCAL_FACET and CATALOG_FACET
    private final String datasetRef;
    private final String profileName;
    private final TestDataKind facetKind;

    private VectorDataSpec(SourceType sourceType, String rawSpec, Path localPath, URI remoteUri,
                          String datasetRef, String profileName, TestDataKind facetKind) {
        this.sourceType = sourceType;
        this.rawSpec = rawSpec;
        this.localPath = localPath;
        this.remoteUri = remoteUri;
        this.datasetRef = datasetRef;
        this.profileName = profileName;
        this.facetKind = facetKind;
    }

    /// Parse a vector data specification string into a VectorDataSpec.
    ///
    /// @param spec The specification string to parse
    /// @return A VectorDataSpec representing the parsed specification
    /// @throws IllegalArgumentException if the specification is invalid
    public static VectorDataSpec parse(String spec) {
        if (spec == null || spec.trim().isEmpty()) {
            throw new IllegalArgumentException("Vector data spec must not be null or empty");
        }

        String trimmed = spec.trim();

        // Check for explicit file: prefix
        if (trimmed.startsWith("file:")) {
            String pathStr = trimmed.substring(5);
            if (pathStr.isEmpty()) {
                throw new IllegalArgumentException("file: prefix requires a path");
            }
            Path path = expandPath(pathStr);
            return new VectorDataSpec(SourceType.LOCAL_FILE, trimmed, path, null, null, null, null);
        }

        // Check for facet: or facet. prefix
        if (trimmed.startsWith("facet:") || trimmed.startsWith("facet.")) {
            return parseFacetSpec(trimmed.substring(6), trimmed);
        }

        // Check for HTTP/HTTPS URLs
        if (trimmed.startsWith("http://") || trimmed.startsWith("https://")) {
            return parseRemoteSpec(trimmed);
        }

        // Check if this looks like a facet spec (name.profile.facet or name:profile:facet) without the prefix
        // First, check if it could be a relative path to a directory
        if (looksLikeFacetSpec(trimmed)) {
            return parseFacetSpec(trimmed, trimmed);
        }

        // Check if this looks like an incomplete dataset/profile spec (missing the facet)
        // This is a common user error - they provide a profile selector instead of a vector source
        Optional<IncompleteDatasetSpec> incomplete = parseIncompleteDatasetSpec(trimmed);
        if (incomplete.isPresent()) {
            IncompleteDatasetSpec parts = incomplete.get();
            throw new IncompleteDatasetSpecException(parts.datasetName(), parts.profileName(), parts.rawSpec());
        }

        // Unqualified - default to local file
        Path path = expandPath(trimmed);
        return new VectorDataSpec(SourceType.LOCAL_FILE, trimmed, path, null, null, null, null);
    }

    private record IncompleteDatasetSpec(String datasetName, String profileName, String rawSpec) {}

    /// Check if the spec looks like an incomplete dataset:profile spec (missing the facet).
    /// This catches the common error where users provide a profile selector instead of
    /// a full vector source specification.
    ///
    /// Returns a dataset/profile pair if:
    /// - Has exactly 1 colon, or 1 dot (without known file extensions)
    /// - Is not a file: prefix
    /// - Is not an existing local path
    /// - Both parts look like valid dataset/profile names (not paths)
    private static Optional<IncompleteDatasetSpec> parseIncompleteDatasetSpec(String spec) {
        long colonCount = spec.chars().filter(c -> c == ':').count();
        long dotCount = spec.chars().filter(c -> c == '.').count();
        String separator;
        if (colonCount == 1) {
            separator = ":";
        } else if (colonCount == 0 && dotCount == 1 && !hasKnownVectorExtension(spec) && !hasDatasetYamlExtension(spec)) {
            separator = ".";
        } else {
            return Optional.empty();
        }

        if (spec.contains("://")) {
            return Optional.empty();
        }

        String[] parts = spec.split(Pattern.quote(separator), 2);
        if (parts.length != 2) {
            return Optional.empty();
        }

        String firstPart = parts[0].trim();
        String secondPart = parts[1].trim();

        // Both parts must be non-empty
        if (firstPart.isEmpty() || secondPart.isEmpty()) {
            return Optional.empty();
        }

        // If the first part looks like a path (contains / or \, starts with . or ~), skip
        if (firstPart.contains("/") || firstPart.contains("\\") ||
            firstPart.startsWith(".") || firstPart.startsWith("~")) {
            return Optional.empty();
        }

        // If the first part is an existing file or directory, it's not a dataset spec
        Path possiblePath = expandPath(spec);
        if (Files.exists(possiblePath)) {
            return Optional.empty();
        }

        // Check if the first part might be an existing directory (in case it has the colon)
        Path firstPath = expandPath(firstPart);
        if (Files.exists(firstPath)) {
            return Optional.empty();
        }

        // If both parts look like identifiers (alphanumeric with dashes/underscores/dots),
        // this is likely a dataset:profile spec missing the facet
        String identifierPattern = "^[a-zA-Z0-9][a-zA-Z0-9._-]*$";
        if (firstPart.matches(identifierPattern) && secondPart.matches(identifierPattern)) {
            return Optional.of(new IncompleteDatasetSpec(firstPart, secondPart, spec));
        }
        return Optional.empty();
    }

    private static boolean hasKnownVectorExtension(String spec) {
        String trimmed = spec.trim().toLowerCase();
        for (String ext : VectorFileExtension.getAllExtensions()) {
            if (trimmed.endsWith(ext.toLowerCase())) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasDatasetYamlExtension(String spec) {
        String trimmed = spec.trim().toLowerCase();
        return trimmed.endsWith(".yaml") || trimmed.endsWith(".yml");
    }

    /// Check if the spec looks like a facet specification (name.profile.facet or name:profile:facet)
    /// without the explicit facet: prefix
    private static boolean looksLikeFacetSpec(String spec) {
        var parsed = DatasetProfileFacetSpec.tryParse(spec);
        if (parsed.isEmpty()) {
            return false;
        }
        String firstPart = parsed.get().datasetRef().trim();
        String thirdPart = parsed.get().facetName().trim();

        // First, check if the first part is a local directory that contains dataset.yaml
        // (prioritize local directories with dataset.yaml over catalog names)
        Path possibleDir = expandPath(firstPart);
        if (isDatasetDirectory(possibleDir)) {
            // It's a local dataset directory - treat as local facet
            return true;
        }

        // Check if the third part is a valid facet name
        return TestDataKind.fromOptionalString(thirdPart).isPresent();
    }

    /// Check if a directory is a valid dataset directory (contains dataset.yaml or dataset.yml)
    private static boolean isDatasetDirectory(Path dir) {
        if (!Files.isDirectory(dir)) {
            return false;
        }
        return Files.exists(dir.resolve("dataset.yaml")) || Files.exists(dir.resolve("dataset.yml"));
    }

    /// Parse a facet specification (ref.profile.facet or ref:profile:facet)
    ///
    /// @param facetPart The facet specification without any prefix (e.g., "sift-128.default.base")
    /// @param originalSpec The original spec string for error messages
    private static VectorDataSpec parseFacetSpec(String facetPart, String originalSpec) {
        DatasetProfileFacetSpec parsed = DatasetProfileFacetSpec.parseRequired(facetPart, originalSpec);
        String datasetRef = parsed.datasetRef().trim();
        String profileName = parsed.profileName().trim();
        String facetName = parsed.facetName().trim();

        if (datasetRef.isEmpty() || profileName.isEmpty() || facetName.isEmpty()) {
            throw new IllegalArgumentException(
                "Facet spec components must not be empty, got: " + originalSpec);
        }

        // Parse the facet name using TestDataKind
        Optional<TestDataKind> facetKind = TestDataKind.fromOptionalString(facetName);
        if (facetKind.isEmpty()) {
            throw new IllegalArgumentException(
                "Unknown facet name '" + facetName + "'. Valid facets: " +
                "base, query, indices, distances, base_vectors, query_vectors, " +
                "neighbor_indices, neighbor_distances");
        }

        // Determine if this is a local directory or catalog reference
        // Check local directory FIRST to prioritize local paths over catalog names
        // but ONLY if the directory contains dataset.yaml
        Path possibleDir = expandPath(datasetRef);
        if (isDatasetDirectory(possibleDir)) {
            return new VectorDataSpec(SourceType.LOCAL_FACET, originalSpec, possibleDir, null,
                datasetRef, profileName, facetKind.get());
        } else {
            return new VectorDataSpec(SourceType.CATALOG_FACET, originalSpec, null, null,
                datasetRef, profileName, facetKind.get());
        }
    }

    /// Parse a remote URL specification
    private static VectorDataSpec parseRemoteSpec(String spec) {
        URI uri;
        try {
            uri = URI.create(spec);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid URL: " + spec, e);
        }

        // URLs ending with "/" are ambiguous - they point to a dataset base
        // but don't specify which facet is wanted
        if (spec.endsWith("/")) {
            throw new AmbiguousDatasetBaseException(spec);
        }

        if (spec.endsWith("/dataset.yaml") || spec.endsWith("/dataset.yml")) {
            return new VectorDataSpec(SourceType.REMOTE_DATASET_YAML, spec, null, uri,
                null, null, null);
        }

        return new VectorDataSpec(SourceType.REMOTE_FILE, spec, null, uri, null, null, null);
    }

    /// Expand path variables like ~ and ${HOME}
    private static Path expandPath(String pathStr) {
        String expanded = pathStr
            .replace("~", System.getProperty("user.home"))
            .replace("${HOME}", System.getProperty("user.home"));
        return Path.of(expanded);
    }

    /// @return The source type of this specification
    public SourceType getSourceType() {
        return sourceType;
    }

    /// @return The raw specification string as provided
    public String getRawSpec() {
        return rawSpec;
    }

    /// @return The local file path, if this is a LOCAL_FILE or LOCAL_FACET spec
    public Optional<Path> getLocalPath() {
        return Optional.ofNullable(localPath);
    }

    /// @return The remote URI, if this is a REMOTE_* spec
    public Optional<URI> getRemoteUri() {
        return Optional.ofNullable(remoteUri);
    }

    /// @return The dataset reference (directory path or catalog name), if this is a facet spec
    public Optional<String> getDatasetRef() {
        return Optional.ofNullable(datasetRef);
    }

    /// @return The profile name, if this is a facet spec
    public Optional<String> getProfileName() {
        return Optional.ofNullable(profileName);
    }

    /// @return The facet kind, if this is a facet spec
    public Optional<TestDataKind> getFacetKind() {
        return Optional.ofNullable(facetKind);
    }

    /// @return true if this spec represents a local file (not a facet or remote resource)
    public boolean isLocalFile() {
        return sourceType == SourceType.LOCAL_FILE;
    }

    /// @return true if this spec represents a local facet from a dataset.yaml directory
    public boolean isLocalFacet() {
        return sourceType == SourceType.LOCAL_FACET;
    }

    /// @return true if this spec represents a catalog-resolved facet
    public boolean isCatalogFacet() {
        return sourceType == SourceType.CATALOG_FACET;
    }

    /// @return true if this spec represents any kind of facet (local or catalog)
    public boolean isFacet() {
        return sourceType == SourceType.LOCAL_FACET || sourceType == SourceType.CATALOG_FACET;
    }

    /// @return true if this spec represents a remote resource
    public boolean isRemote() {
        return sourceType == SourceType.REMOTE_FILE ||
               sourceType == SourceType.REMOTE_DATASET_YAML;
    }

    /// @return true if this spec represents a remote file (not a dataset base or yaml)
    public boolean isRemoteFile() {
        return sourceType == SourceType.REMOTE_FILE;
    }

    /// @return true if this spec represents a remote dataset.yaml
    public boolean isRemoteDataset() {
        return sourceType == SourceType.REMOTE_DATASET_YAML;
    }

    /// Get the dataset.yaml URL for remote dataset specs.
    ///
    /// @return The dataset.yaml URL, or empty if not a remote dataset spec
    public Optional<String> getDatasetYamlUrl() {
        if (sourceType == SourceType.REMOTE_DATASET_YAML) {
            return Optional.of(remoteUri.toString());
        }
        return Optional.empty();
    }

    /// Get the base URL for remote dataset specs (without dataset.yaml).
    /// For REMOTE_DATASET_YAML, removes "dataset.yaml" from the end.
    ///
    /// @return The base URL, or empty if not a remote dataset spec
    public Optional<String> getDatasetBaseUrl() {
        if (sourceType == SourceType.REMOTE_DATASET_YAML) {
            String url = remoteUri.toString();
            if (url.endsWith("/dataset.yaml")) {
                return Optional.of(url.substring(0, url.length() - "dataset.yaml".length()));
            } else if (url.endsWith("/dataset.yml")) {
                return Optional.of(url.substring(0, url.length() - "dataset.yml".length()));
            }
        }
        return Optional.empty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof VectorDataSpec that)) return false;
        return sourceType == that.sourceType &&
               Objects.equals(rawSpec, that.rawSpec) &&
               Objects.equals(localPath, that.localPath) &&
               Objects.equals(remoteUri, that.remoteUri) &&
               Objects.equals(datasetRef, that.datasetRef) &&
               Objects.equals(profileName, that.profileName) &&
               facetKind == that.facetKind;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceType, rawSpec, localPath, remoteUri, datasetRef, profileName, facetKind);
    }

    @Override
    public String toString() {
        return rawSpec;
    }

    /// Create a descriptive string for display purposes
    public String toDescription() {
        return switch (sourceType) {
            case LOCAL_FILE -> "Local file: " + localPath;
            case LOCAL_FACET -> "Local facet: " + datasetRef + ":" + profileName + ":" + facetKind.name();
            case CATALOG_FACET -> "Catalog facet: " + datasetRef + ":" + profileName + ":" + facetKind.name();
            case REMOTE_FILE -> "Remote file: " + remoteUri;
            case REMOTE_DATASET_YAML -> "Remote dataset.yaml: " + remoteUri;
        };
    }
}
