package io.nosqlbench.vectordata.discovery.vector;

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


import io.nosqlbench.datatools.virtdata.VectorGenerator;
import io.nosqlbench.datatools.virtdata.VectorGeneratorIO;
import io.nosqlbench.vectordata.discovery.TestDataGroup;
import io.nosqlbench.vectordata.layoutv2.DSProfile;
import io.nosqlbench.vectordata.layoutv2.DSView;
import io.nosqlbench.vectordata.layoutv2.DSWindow;
import io.nosqlbench.vectordata.spec.datasets.impl.xvec.BaseVectorsXvecImpl;
import io.nosqlbench.vectordata.spec.datasets.impl.xvec.QueryVectorsXvecImpl;
import io.nosqlbench.vectordata.spec.datasets.impl.xvec.NeighborIndicesXvecImpl;
import io.nosqlbench.vectordata.spec.datasets.impl.xvec.NeighborDistancesXvecImpl;
import io.nosqlbench.vectordata.spec.datasets.types.BaseVectors;
import io.nosqlbench.vectordata.spec.datasets.types.DistanceFunction;
import io.nosqlbench.vectordata.spec.datasets.types.NeighborDistances;
import io.nosqlbench.vectordata.spec.datasets.types.NeighborIndices;
import io.nosqlbench.vectordata.spec.datasets.types.QueryVectors;
import io.nosqlbench.vectordata.spec.datasets.types.TestDataKind;
import io.nosqlbench.vectordata.spec.tokens.SpecToken;
import io.nosqlbench.vectordata.spec.tokens.Templatizer;
import io.nosqlbench.vectordata.views.VirtdataFloatVectorsViewVector;
import io.nosqlbench.vshapes.model.VectorSpaceModel;
import io.nosqlbench.vshapes.model.VectorSpaceModelConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.nosqlbench.nbdatatools.api.concurrent.ProgressIndicatingFuture;
import io.nosqlbench.nbdatatools.api.concurrent.ProgressIndicator;

/// TestDataView implementation for filesystem-based datasets.
///
/// This class provides access to vector datasets stored in local files (fvec, ivec, etc.)
/// using AsyncFileChannel for efficient I/O. It wires up the xvec implementations based
/// on the profile configuration from dataset.yaml.
public class FilesystemVectorTestDataView implements VectorTestDataView, AutoCloseable {
    private static final Logger logger = LogManager.getLogger(FilesystemVectorTestDataView.class);

    private final TestDataGroup dataGroup;
    private final DSProfile profile;
    private final String profileName;

    // Lazily loaded vector datasets
    private BaseVectors baseVectors;
    private QueryVectors queryVectors;
    private NeighborIndices neighborIndices;
    private NeighborDistances neighborDistances;
    private NeighborIndices filteredNeighborIndices;
    private NeighborDistances filteredNeighborDistances;

    // Track opened channels for cleanup
    private final Map<String, AsynchronousFileChannel> openChannels = new LinkedHashMap<>();

    /// Creates a new FilesystemTestDataView.
    ///
    /// @param dataGroup The data group containing dataset metadata
    /// @param profile The profile configuration
    /// @param profileName The name of this profile
    public FilesystemVectorTestDataView(TestDataGroup dataGroup, DSProfile profile, String profileName) {
        this.dataGroup = dataGroup;
        this.profile = profile;
        this.profileName = profileName;
    }

    @Override
    public Optional<BaseVectors> getBaseVectors() {
        if (baseVectors != null) {
            return Optional.of(baseVectors);
        }

        DSView view = profile.get(TestDataKind.base_vectors.name());
        if (view == null) {
            return Optional.empty();
        }

        try {
            String filename = view.getSource().getPath();
            Path filePath = dataGroup.getDatasetDirectory().resolve(filename);

            // Check for virtdata source
            if (view.getSource().isVirtdata()) {
                baseVectors = loadVirtdataVectors(filePath, view.getWindow());
                logger.debug("Base vectors (virtdata) for profile '{}': model={}, count={}",
                    profileName, filename, baseVectors.getCount());
                return Optional.of(baseVectors);
            }

            // Standard xvec file source
            if (!Files.exists(filePath)) {
                logger.warn("Base vectors file not found: {}", filePath);
                return Optional.empty();
            }

            String extension = getFileExtension(filename);
            DSWindow window = normalizeWindow(view.getWindow());

            logger.debug("Base vectors for profile '{}': file={}, window={}",
                profileName, filename, view.getWindow());

            baseVectors = new BaseVectorsXvecImpl(filePath, window, extension);
            logger.debug("Base vectors count after windowing: {}", baseVectors.getCount());
            return Optional.of(baseVectors);

        } catch (IOException e) {
            throw new RuntimeException("Failed to load base vectors: " + e.getMessage(), e);
        }
    }

    @Override
    public Optional<QueryVectors> getQueryVectors() {
        if (queryVectors != null) {
            return Optional.of(queryVectors);
        }

        DSView view = profile.get(TestDataKind.query_vectors.name());
        if (view == null) {
            return Optional.empty();
        }

        try {
            String filename = view.getSource().getPath();
            Path filePath = dataGroup.getDatasetDirectory().resolve(filename);

            // Check for virtdata source
            if (view.getSource().isVirtdata()) {
                queryVectors = loadVirtdataVectors(filePath, view.getWindow());
                logger.debug("Query vectors (virtdata) for profile '{}': model={}, count={}",
                    profileName, filename, queryVectors.getCount());
                return Optional.of(queryVectors);
            }

            // Standard xvec file source
            if (!Files.exists(filePath)) {
                logger.warn("Query vectors file not found: {}", filePath);
                return Optional.empty();
            }

            String extension = getFileExtension(filename);
            DSWindow window = normalizeWindow(view.getWindow());

            // Use QueryVectorsXvecImpl for proper typing
            queryVectors = new QueryVectorsXvecImpl(filePath, window, extension);
            return Optional.of(queryVectors);

        } catch (IOException e) {
            throw new RuntimeException("Failed to load query vectors: " + e.getMessage(), e);
        }
    }

    @Override
    public Optional<NeighborIndices> getNeighborIndices() {
        if (neighborIndices != null) {
            return Optional.of(neighborIndices);
        }

        DSView view = profile.get(TestDataKind.neighbor_indices.name());
        if (view == null) {
            return Optional.empty();
        }

        String filename = view.getSource().getPath();
        Path filePath = dataGroup.getDatasetDirectory().resolve(filename);

        if (!Files.exists(filePath)) {
            logger.warn("Neighbor indices file not found: {}", filePath);
            return Optional.empty();
        }

        String extension = getFileExtension(filename);
        DSWindow window = normalizeWindow(view.getWindow());

        logger.debug("Neighbor indices for profile '{}': file={}, window={}",
            profileName, filename, view.getWindow());

        neighborIndices = new NeighborIndicesXvecImpl(filePath, window, extension);
        logger.debug("Neighbor indices count: {}, maxK: {}",
            neighborIndices.getCount(), neighborIndices.getMaxK());
        return Optional.of(neighborIndices);
    }

    @Override
    public Optional<NeighborDistances> getNeighborDistances() {
        if (neighborDistances != null) {
            return Optional.of(neighborDistances);
        }

        DSView view = profile.get(TestDataKind.neighbor_distances.name());
        if (view == null) {
            return Optional.empty();
        }

        String filename = view.getSource().getPath();
        Path filePath = dataGroup.getDatasetDirectory().resolve(filename);

        if (!Files.exists(filePath)) {
            logger.warn("Neighbor distances file not found: {}", filePath);
            return Optional.empty();
        }

        String extension = getFileExtension(filename);
        DSWindow window = normalizeWindow(view.getWindow());

        neighborDistances = new NeighborDistancesXvecImpl(filePath, window, extension);
        return Optional.of(neighborDistances);
    }

    @Override
    public Optional<NeighborIndices> getFilteredNeighborIndices() {
        if (filteredNeighborIndices != null) {
            return Optional.of(filteredNeighborIndices);
        }

        DSView view = profile.get(TestDataKind.filtered_neighbor_indices.name());
        if (view == null) {
            return Optional.empty();
        }

        String filename = view.getSource().getPath();
        Path filePath = dataGroup.getDatasetDirectory().resolve(filename);

        if (!Files.exists(filePath)) {
            logger.warn("Filtered neighbor indices file not found: {}", filePath);
            return Optional.empty();
        }

        String extension = getFileExtension(filename);
        DSWindow window = normalizeWindow(view.getWindow());

        logger.debug("Filtered neighbor indices for profile '{}': file={}, window={}",
            profileName, filename, view.getWindow());

        filteredNeighborIndices = new NeighborIndicesXvecImpl(filePath, window, extension);
        logger.debug("Filtered neighbor indices count: {}, maxK: {}",
            filteredNeighborIndices.getCount(), filteredNeighborIndices.getMaxK());
        return Optional.of(filteredNeighborIndices);
    }

    @Override
    public Optional<NeighborDistances> getFilteredNeighborDistances() {
        if (filteredNeighborDistances != null) {
            return Optional.of(filteredNeighborDistances);
        }

        DSView view = profile.get(TestDataKind.filtered_neighbor_distances.name());
        if (view == null) {
            return Optional.empty();
        }

        String filename = view.getSource().getPath();
        Path filePath = dataGroup.getDatasetDirectory().resolve(filename);

        if (!Files.exists(filePath)) {
            logger.warn("Filtered neighbor distances file not found: {}", filePath);
            return Optional.empty();
        }

        String extension = getFileExtension(filename);
        DSWindow window = normalizeWindow(view.getWindow());

        filteredNeighborDistances = new NeighborDistancesXvecImpl(filePath, window, extension);
        return Optional.of(filteredNeighborDistances);
    }

    @Override
    public DistanceFunction getDistanceFunction() {
        return dataGroup.getDistanceFunction();
    }

    @Override
    public String getLicense() {
        Object license = dataGroup.getAttribute("license");
        return license != null ? license.toString() : "UNKNOWN";
    }

    @Override
    public URL getUrl() {
        Object url = dataGroup.getAttribute("url");
        if (url == null) {
            return null;
        }
        try {
            return new URL(url.toString());
        } catch (MalformedURLException e) {
            logger.warn("Invalid URL in dataset.yaml: {}", url);
            return null;
        }
    }

    @Override
    public String getModel() {
        Object model = dataGroup.getAttribute("model");
        return model != null ? model.toString() : "UNKNOWN";
    }

    @Override
    public String getVendor() {
        Object vendor = dataGroup.getAttribute("vendor");
        return vendor != null ? vendor.toString() : "UNKNOWN";
    }

    @Override
    public Optional<String> lookupToken(String tokenName) {
        try {
            return SpecToken.lookup(tokenName).flatMap(t -> t.apply(this));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    @Override
    public Optional<String> tokenize(String template) {
        return new Templatizer(t -> this.lookupToken(t).orElse(null)).templatize(template);
    }

    @Override
    public String getName() {
        return dataGroup.getName() + ":" + profileName;
    }

    @Override
    public Map<String, String> getTokens() {
        Map<String, String> tokens = new LinkedHashMap<>();
        for (SpecToken specToken : SpecToken.values()) {
            specToken.apply(this).ifPresent(t -> tokens.put(specToken.name(), t));
        }
        return tokens;
    }

    @Override
    public CompletableFuture<Void> prebuffer() {
        CompletableFuture<Void> baseVectorsFuture = getBaseVectors()
            .map(bv -> bv.prebuffer())
            .orElse(CompletableFuture.completedFuture(null));

        CompletableFuture<Void> queryVectorsFuture = getQueryVectors()
            .map(qv -> qv.prebuffer())
            .orElse(CompletableFuture.completedFuture(null));

        CompletableFuture<Void> neighborIndicesFuture = getNeighborIndices()
            .map(ni -> ni.prebuffer())
            .orElse(CompletableFuture.completedFuture(null));

        CompletableFuture<Void> neighborDistancesFuture = getNeighborDistances()
            .map(nd -> nd.prebuffer())
            .orElse(CompletableFuture.completedFuture(null));

        CompletableFuture<Void> filteredIndicesFuture = getFilteredNeighborIndices()
            .map(fi -> fi.prebuffer())
            .orElse(CompletableFuture.completedFuture(null));

        CompletableFuture<Void> filteredDistancesFuture = getFilteredNeighborDistances()
            .map(fd -> fd.prebuffer())
            .orElse(CompletableFuture.completedFuture(null));

        List<CompletableFuture<Void>> futures = List.of(
            baseVectorsFuture,
            queryVectorsFuture,
            neighborIndicesFuture,
            neighborDistancesFuture,
            filteredIndicesFuture,
            filteredDistancesFuture
        );

        CompletableFuture<Void> allOf = CompletableFuture.allOf(
            futures.toArray(new CompletableFuture[0])
        );

        // Aggregate progress tracking from individual futures
        List<ProgressIndicator<?>> progressSources = new ArrayList<>();
        for (CompletableFuture<Void> f : futures) {
            if (f instanceof ProgressIndicator) {
                progressSources.add((ProgressIndicator<?>) f);
            }
        }
        if (progressSources.isEmpty()) {
            return allOf;
        }

        double bytesPerUnit = progressSources.get(0).getBytesPerUnit();
        return new ProgressIndicatingFuture<>(
            allOf,
            () -> progressSources.stream().mapToDouble(ProgressIndicator::getTotalWork).sum(),
            () -> progressSources.stream().mapToDouble(ProgressIndicator::getCurrentWork).sum(),
            bytesPerUnit
        );
    }

    @Override
    public void close() throws Exception {
        // Close all open file channels
        for (Map.Entry<String, AsynchronousFileChannel> entry : openChannels.entrySet()) {
            try {
                entry.getValue().close();
                logger.debug("Closed channel for: {}", entry.getKey());
            } catch (IOException e) {
                logger.warn("Failed to close channel for {}: {}", entry.getKey(), e.getMessage());
            }
        }
        openChannels.clear();
    }

    /// Normalizes a DSWindow for use with xvec implementations.
    ///
    /// @param window The DSWindow to normalize
    /// @return The window, or null for ALL windows (null means ALL for xvec implementations)
    private DSWindow normalizeWindow(DSWindow window) {
        if (window == null || window == DSWindow.ALL || window.isEmpty()) {
            return null;
        }
        // Check for the sentinel ALL value (single interval with -1,-1)
        if (window.size() == 1 && window.get(0).getMinIncl() == -1 && window.get(0).getMaxExcl() == -1) {
            return null;
        }
        return window;
    }

    /// Loads a virtdata (generator-backed) vectors view from a model JSON file.
    ///
    /// The model file is loaded as a VectorSpaceModel, then an appropriate
    /// VectorGenerator is created and wrapped in a VirtdataFloatVectorsView.
    ///
    /// @param modelPath The path to the model JSON file
    /// @param window The window defining cardinality (required for bounded generation)
    /// @return A VirtdataFloatVectorsView implementing both BaseVectors and QueryVectors
    /// @throws IOException if the model file cannot be read
    /// @throws IllegalArgumentException if no generator supports the model type
    private VirtdataFloatVectorsViewVector loadVirtdataVectors(Path modelPath, DSWindow window) throws IOException {
        if (!Files.exists(modelPath)) {
            throw new IOException("Virtdata model file not found: " + modelPath);
        }

        // Load the VectorSpaceModel from JSON
        VectorSpaceModel model = VectorSpaceModelConfig.loadFromFile(modelPath);

        // Create and initialize a generator for the model
        VectorGenerator<VectorSpaceModel> generator = VectorGeneratorIO.createForModel(model);

        // Determine count from window
        int count;
        DSWindow normalized = normalizeWindow(window);
        if (normalized == null) {
            // Use model's unique vectors as count, capped at Integer.MAX_VALUE
            long unique = model.uniqueVectors();
            count = unique > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) unique;
        } else {
            // Use window size as count
            long windowSize = 0;
            for (var interval : normalized) {
                windowSize += interval.getMaxExcl() - interval.getMinIncl();
            }
            count = windowSize > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) windowSize;
        }

        return new VirtdataFloatVectorsViewVector(generator, count);
    }

    /// Extracts the file extension from a filename.
    ///
    /// @param filename The filename
    /// @return The extension (without the dot)
    private String getFileExtension(String filename) {
        int lastDot = filename.lastIndexOf('.');
        if (lastDot > 0 && lastDot < filename.length() - 1) {
            return filename.substring(lastDot + 1);
        }
        throw new IllegalArgumentException("Cannot determine file extension for: " + filename);
    }

    @Override
    public String toString() {
        return "FilesystemTestDataView{" +
            "profile='" + profileName + '\'' +
            ", dataset=" + dataGroup.getName() +
            '}';
    }
}
