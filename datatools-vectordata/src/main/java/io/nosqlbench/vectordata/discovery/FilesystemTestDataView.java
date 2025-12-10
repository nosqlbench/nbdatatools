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


import io.nosqlbench.vectordata.layout.FProfiles;
import io.nosqlbench.vectordata.layout.FView;
import io.nosqlbench.vectordata.layout.FWindow;
import io.nosqlbench.vectordata.layoutv2.DSInterval;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/// TestDataView implementation for filesystem-based datasets.
///
/// This class provides access to vector datasets stored in local files (fvec, ivec, etc.)
/// using AsyncFileChannel for efficient I/O. It wires up the xvec implementations based
/// on the profile configuration from dataset.yaml.
public class FilesystemTestDataView implements TestDataView, AutoCloseable {
    private static final Logger logger = LogManager.getLogger(FilesystemTestDataView.class);

    private final TestDataGroup dataGroup;
    private final FProfiles profile;
    private final String profileName;

    // Lazily loaded vector datasets
    private BaseVectors baseVectors;
    private QueryVectors queryVectors;
    private NeighborIndices neighborIndices;
    private NeighborDistances neighborDistances;

    // Track opened channels for cleanup
    private final Map<String, AsynchronousFileChannel> openChannels = new LinkedHashMap<>();

    /// Creates a new FilesystemTestDataView.
    ///
    /// @param dataGroup The data group containing dataset metadata
    /// @param profile The profile configuration
    /// @param profileName The name of this profile
    public FilesystemTestDataView(TestDataGroup dataGroup, FProfiles profile, String profileName) {
        this.dataGroup = dataGroup;
        this.profile = profile;
        this.profileName = profileName;
    }

    @Override
    public Optional<BaseVectors> getBaseVectors() {
        if (baseVectors != null) {
            return Optional.of(baseVectors);
        }

        FView view = profile.views().get(TestDataKind.base_vectors.name());
        if (view == null) {
            return Optional.empty();
        }

        try {
            String filename = view.source().inpath();
            Path filePath = dataGroup.getDatasetDirectory().resolve(filename);

            if (!Files.exists(filePath)) {
                logger.warn("Base vectors file not found: {}", filePath);
                return Optional.empty();
            }

            long fileSize = Files.size(filePath);
            AsynchronousFileChannel channel = AsynchronousFileChannel.open(
                filePath,
                StandardOpenOption.READ
            );
            openChannels.put("base_vectors", channel);

            String extension = getFileExtension(filename);
            DSWindow window = convertWindow(view.window());

            logger.debug("Base vectors for profile '{}': file={}, window={}",
                profileName, filename, view.window());

            baseVectors = new BaseVectorsXvecImpl(channel, fileSize, window, extension);
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

        FView view = profile.views().get(TestDataKind.query_vectors.name());
        if (view == null) {
            return Optional.empty();
        }

        try {
            String filename = view.source().inpath();
            Path filePath = dataGroup.getDatasetDirectory().resolve(filename);

            if (!Files.exists(filePath)) {
                logger.warn("Query vectors file not found: {}", filePath);
                return Optional.empty();
            }

            long fileSize = Files.size(filePath);
            AsynchronousFileChannel channel = AsynchronousFileChannel.open(
                filePath,
                StandardOpenOption.READ
            );
            openChannels.put("query_vectors", channel);

            String extension = getFileExtension(filename);
            DSWindow window = convertWindow(view.window());

            // Use QueryVectorsXvecImpl for proper typing
            queryVectors = new QueryVectorsXvecImpl(channel, fileSize, window, extension);
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

        FView view = profile.views().get(TestDataKind.neighbor_indices.name());
        if (view == null) {
            return Optional.empty();
        }

        try {
            String filename = view.source().inpath();
            Path filePath = dataGroup.getDatasetDirectory().resolve(filename);

            if (!Files.exists(filePath)) {
                logger.warn("Neighbor indices file not found: {}", filePath);
                return Optional.empty();
            }

            long fileSize = Files.size(filePath);
            AsynchronousFileChannel channel = AsynchronousFileChannel.open(
                filePath,
                StandardOpenOption.READ
            );
            openChannels.put("neighbor_indices", channel);

            String extension = getFileExtension(filename);
            DSWindow window = convertWindow(view.window());

            logger.debug("Neighbor indices for profile '{}': file={}, window={}",
                profileName, filename, view.window());

            // Use NeighborIndicesXvecImpl for proper typing
            neighborIndices = new NeighborIndicesXvecImpl(channel, fileSize, window, extension);
            logger.debug("Neighbor indices count: {}, maxK: {}",
                neighborIndices.getCount(), neighborIndices.getMaxK());
            return Optional.of(neighborIndices);

        } catch (IOException e) {
            throw new RuntimeException("Failed to load neighbor indices: " + e.getMessage(), e);
        }
    }

    @Override
    public Optional<NeighborDistances> getNeighborDistances() {
        if (neighborDistances != null) {
            return Optional.of(neighborDistances);
        }

        FView view = profile.views().get(TestDataKind.neighbor_distances.name());
        if (view == null) {
            return Optional.empty();
        }

        try {
            String filename = view.source().inpath();
            Path filePath = dataGroup.getDatasetDirectory().resolve(filename);

            if (!Files.exists(filePath)) {
                logger.warn("Neighbor distances file not found: {}", filePath);
                return Optional.empty();
            }

            long fileSize = Files.size(filePath);
            AsynchronousFileChannel channel = AsynchronousFileChannel.open(
                filePath,
                StandardOpenOption.READ
            );
            openChannels.put("neighbor_distances", channel);

            String extension = getFileExtension(filename);
            DSWindow window = convertWindow(view.window());

            // Use NeighborDistancesXvecImpl for proper typing
            neighborDistances = new NeighborDistancesXvecImpl(channel, fileSize, window, extension);
            return Optional.of(neighborDistances);

        } catch (IOException e) {
            throw new RuntimeException("Failed to load neighbor distances: " + e.getMessage(), e);
        }
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

        return CompletableFuture.allOf(
            baseVectorsFuture,
            queryVectorsFuture,
            neighborIndicesFuture,
            neighborDistancesFuture
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

    /// Converts an FWindow to a DSWindow.
    ///
    /// @param fwindow The FWindow to convert
    /// @return The converted DSWindow, or null for ALL windows
    private DSWindow convertWindow(FWindow fwindow) {
        if (fwindow == FWindow.ALL || fwindow.intervals().isEmpty()) {
            return null; // null means ALL for xvec implementations
        }

        // For now, only support single interval windows
        if (fwindow.intervals().size() > 1) {
            throw new UnsupportedOperationException(
                "Multiple interval windows not yet supported for filesystem datasets");
        }

        var interval = fwindow.intervals().get(0);
        DSWindow window = new DSWindow();
        window.add(new DSInterval(interval.minIncl(), interval.maxExcl()));
        return window;
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
