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


import io.nosqlbench.vectordata.layout.FGroup;
import io.nosqlbench.vectordata.layout.FProfiles;
import io.nosqlbench.vectordata.spec.datasets.types.DistanceFunction;
import io.nosqlbench.vectordata.spec.datasets.types.TestDataKind;
import io.nosqlbench.vectordata.spec.tokens.SpecToken;
import io.nosqlbench.vectordata.spec.tokens.Templatizer;
import io.nosqlbench.vectordata.utils.SHARED;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/// TestDataGroup implementation for filesystem-based datasets using dataset.yaml configuration.
///
/// This class allows loading datasets directly from the local filesystem using a dataset.yaml
/// configuration file. It wires up vector files (fvec, ivec, etc.) through AsyncFileChannel
/// without requiring HDF5 format.
///
/// Example dataset.yaml:
/// <pre>
/// attributes:
///   distance_function: COSINE
///   license: APL
///   url: https://github.com/nosqlbench/nbdatatools
///
/// profiles:
///   default:
///     base_vectors: base.fvec
///     query_vectors: query.fvec
///     neighbor_distances: distances.fvec
///     neighbor_indices: indices.ivec
///   small:
///     base_vectors:
///       source: base.fvec
///       window: 0..1000
///     query_vectors: query.fvec
///     neighbor_indices: indices.ivec
/// </pre>
public class FilesystemTestDataGroup implements AutoCloseable, ProfileSelector {
    private static final Logger logger = LogManager.getLogger(FilesystemTestDataGroup.class);

    /// The default profile name
    public static final String DEFAULT_PROFILE = "default";

    /// The directory containing the dataset files
    private final Path datasetDirectory;

    /// The parsed profile configurations
    private final FGroup groupProfiles;

    /// The attributes from the dataset.yaml
    private final Map<String, Object> attributes;

    /// Cache of profile views
    private final Map<String, TestDataView> profileCache = new LinkedHashMap<>();

    /// The name of the dataset (derived from directory name)
    private final String datasetName;

    /// Creates a FilesystemTestDataGroup from a directory path containing a dataset.yaml file.
    ///
    /// @param path The path to either a directory containing dataset.yaml or the dataset.yaml file itself
    /// @throws IOException If the dataset.yaml cannot be read or parsed
    public FilesystemTestDataGroup(Path path) throws IOException {
        // Handle both directory and direct file paths
        if (Files.isDirectory(path)) {
            this.datasetDirectory = path;
            Path yamlPath = path.resolve("dataset.yaml");
            if (!Files.exists(yamlPath)) {
                throw new IOException("dataset.yaml not found in directory: " + path);
            }
        } else if (path.getFileName().toString().equals("dataset.yaml")) {
            this.datasetDirectory = path.getParent();
        } else {
            throw new IOException("Path must be either a directory containing dataset.yaml or the dataset.yaml file itself: " + path);
        }

        this.datasetName = datasetDirectory.getFileName().toString();

        // Load and parse dataset.yaml
        Path yamlPath = datasetDirectory.resolve("dataset.yaml");
        logger.info("Loading dataset configuration from: {}", yamlPath);

        String yamlContent = Files.readString(yamlPath);
        Map<String, Object> config = (Map<String, Object>) SHARED.yamlLoader.loadFromString(yamlContent);

        // Extract attributes
        this.attributes = (Map<String, Object>) config.getOrDefault("attributes", new LinkedHashMap<>());

        // Parse profiles - FGroup.fromObject expects the profiles map directly, not wrapped
        Map<String, Object> profilesData = (Map<String, Object>) config.get("profiles");
        if (profilesData == null || profilesData.isEmpty()) {
            throw new IOException("No profiles defined in dataset.yaml");
        }

        // FGroup.fromObject already handles normalization of shorthand names via TestDataKind.fromOptionalString
        this.groupProfiles = FGroup.fromObject(profilesData, null);

        logger.info("Loaded {} profile(s) from {}", groupProfiles.profiles().size(), yamlPath);
    }

    public String getName() {
        return datasetName;
    }

    /// Gets the directory containing the dataset files.
    ///
    /// @return The dataset directory path
    public Path getDatasetDirectory() {
        return datasetDirectory;
    }

    /// Gets an attribute value by name.
    ///
    /// @param name The attribute name
    /// @return The attribute value, or null if not found
    public Object getAttribute(String name) {
        return attributes.get(name);
    }

    /// Gets the set of all profile names available in this dataset.
    ///
    /// @return A set of profile names
    public Set<String> getProfileNames() {
        return groupProfiles.profiles().keySet();
    }

    /// Gets a profile by name.
    ///
    /// @param profileName The name of the profile to get
    /// @return The TestDataView for the profile
    /// @throws IllegalArgumentException If the profile is not found
    @Override
    public TestDataView profile(String profileName) {
        // Extract effective profile name based on the documented rules
        String effectiveProfileName;

        if (profileName.contains(":")) {
            // If it has colons, take the last word after the last colon
            int lastColonIndex = profileName.lastIndexOf(':');
            effectiveProfileName = profileName.substring(lastColonIndex + 1);
        } else if (profileName.equals(getName())) {
            // If it's a single word matching the dataset name, use "default"
            effectiveProfileName = DEFAULT_PROFILE;
        } else {
            effectiveProfileName = profileName;
        }

        return getProfileOptionally(effectiveProfileName)
            .orElseThrow(() -> new IllegalArgumentException(
                "profile '" + effectiveProfileName + "' not found in " + this));
    }

    /// Gets a profile by name, returning an empty Optional if not found.
    ///
    /// @param profileName The name of the profile to get
    /// @return An Optional containing the profile, or empty if not found
    public Optional<TestDataView> getProfileOptionally(String profileName) {
        FProfiles fprofile = this.groupProfiles.profiles().get(profileName);
        if (fprofile == null) {
            return Optional.empty();
        }

        TestDataView profile = profileCache.computeIfAbsent(
            profileName,
            p -> new FilesystemTestDataView(this, fprofile, profileName)
        );
        return Optional.of(profile);
    }

    /// Gets the default profile.
    ///
    /// @return The default profile
    public TestDataView getDefaultProfile() {
        return this.profile(DEFAULT_PROFILE);
    }

    @Override
    public ProfileSelector setCacheDir(String cacheDir) {
        // No-op for filesystem-based datasets
        return this;
    }

    /// Gets the distance function for this dataset.
    ///
    /// @return The distance function
    public DistanceFunction getDistanceFunction() {
        Object distanceFunc = attributes.get(SpecToken.distance_function.name());
        if (distanceFunc == null) {
            return DistanceFunction.COSINE; // default
        }
        return DistanceFunction.valueOf(distanceFunc.toString().toUpperCase());
    }

    /// Looks up a token value, first in the root attributes, then across all profiles.
    ///
    /// @param tokenName The name of the token to look up
    /// @return An Optional containing the token value, or empty if not found
    public Optional<String> lookupToken(String tokenName) {
        Object attrValue = attributes.get(tokenName);
        if (attrValue != null) {
            return Optional.of(attrValue.toString());
        }

        // Check across profiles
        Set<String> values = getProfileCache().values().stream()
            .map(p -> p.lookupToken(tokenName))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toSet());

        if (values.isEmpty()) {
            return Optional.empty();
        }
        if (values.size() == 1) {
            return Optional.of(values.iterator().next());
        }

        // Multiple different values
        return Optional.of("_");
    }

    /// Gets the cache of all profiles, initializing it if empty.
    ///
    /// @return A map of profile names to profile views
    public synchronized Map<String, TestDataView> getProfileCache() {
        if (profileCache.isEmpty()) {
            profileCache.putAll(getProfileNames().stream()
                .collect(Collectors.toMap(p -> p, p -> profile(p))));
        }
        return profileCache;
    }

    /// Gets all available tokens for this dataset across all profiles.
    ///
    /// @return A map of token names to token values
    public Map<String, String> getTokens() {
        Map<String, String> tokens = new LinkedHashMap<>();
        Set<String> tokenNames = new LinkedHashSet<>();
        getProfileCache().values().forEach(p -> tokenNames.addAll(p.getTokens().keySet()));
        tokenNames.forEach(t -> tokens.put(t, lookupToken(t).orElse(null)));
        return tokens;
    }

    /// Tokenizes a template string using the tokens available in this dataset.
    ///
    /// @param template The template string to tokenize
    /// @return An Optional containing the tokenized string, or empty if tokenization failed
    public Optional<String> tokenize(String template) {
        return new Templatizer(t -> this.lookupToken(t).orElse(null)).templatize(template);
    }

    @Override
    public void close() throws Exception {
        // Close all cached profile views if they need cleanup
        for (TestDataView view : profileCache.values()) {
            if (view instanceof AutoCloseable) {
                ((AutoCloseable) view).close();
            }
        }
        profileCache.clear();
    }

    @Override
    public String toString() {
        return "FilesystemTestDataGroup{" +
            "name='" + datasetName + '\'' +
            ", directory=" + datasetDirectory +
            ", profiles=" + getProfileNames() +
            '}';
    }
}
