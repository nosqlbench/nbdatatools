package io.nosqlbench.vectordata.downloader;

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

import io.nosqlbench.vectordata.discovery.metadata.MetadataContent;
import io.nosqlbench.vectordata.discovery.metadata.MetadataLayout;
import io.nosqlbench.vectordata.discovery.metadata.MetadataLayoutImpl;
import io.nosqlbench.vectordata.discovery.metadata.PredicateStoreBackend;
import io.nosqlbench.vectordata.discovery.metadata.PredicateTestDataView;
import io.nosqlbench.vectordata.discovery.metadata.Predicates;
import io.nosqlbench.vectordata.discovery.metadata.ResultIndices;
import io.nosqlbench.vectordata.discovery.metadata.slab.SlabtasticPredicateBackend;
import io.nosqlbench.vectordata.discovery.metadata.sqlite.SQLitePredicateBackend;
import io.nosqlbench.vectordata.discovery.metadata.views.MetadataContentDatasetView;
import io.nosqlbench.vectordata.discovery.metadata.views.PredicatesDatasetView;
import io.nosqlbench.vectordata.discovery.metadata.views.ResultIndicesDatasetView;
import io.nosqlbench.vectordata.layout.SourceType;
import io.nosqlbench.vectordata.merklev2.CacheFileAccessor;
import io.nosqlbench.vectordata.merklev2.MAFileChannel;
import io.nosqlbench.vectordata.spec.predicates.PNode;
import io.nosqlbench.vectordata.spec.predicates.PredicateContext;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/// Virtual (remote/VFS) implementation of {@link PredicateTestDataView}.
///
/// Mirrors {@link VirtualVectorTestDataView} for predicate data. For
/// each predicate facet, an {@link MAFileChannel} is created for
/// remote access with incremental caching.
///
/// For {@code .slab} files, the MAFileChannel is passed directly to
/// {@link SlabtasticPredicateBackend}. For {@code .db} files, the
/// entire file is prebuffered first, then SQLite opens on the local
/// cache path.
public class VirtualPredicateTestDataView implements PredicateTestDataView<PNode<?>>, AutoCloseable {

    private final DatasetEntry datasetEntry;
    private final Path cachedir;
    private final Map<String, FacetSpec> facetSpecs;
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private final Map<String, PredicateStoreBackend> backendsByPath = new LinkedHashMap<>();

    private Predicates<PNode<?>> predicatesView;
    private ResultIndices resultIndicesView;
    private MetadataLayout metadataLayout;
    private MetadataContent metadataContentView;
    private PredicateContext predicateContext;

    /// Specification for a single predicate facet.
    public static final class FacetSpec {
        private final URL sourceUrl;
        private final Path cachePath;
        private final String extension;

        /// Creates a new facet spec.
        ///
        /// @param sourceUrl the remote URL
        /// @param cachePath the local cache path
        /// @param extension the file extension
        public FacetSpec(URL sourceUrl, Path cachePath, String extension) {
            this.sourceUrl = sourceUrl;
            this.cachePath = cachePath;
            this.extension = extension;
        }

        /// Returns the remote URL.
        ///
        /// @return the remote URL
        public URL sourceUrl() { return sourceUrl; }

        /// Returns the local cache path.
        ///
        /// @return the local cache path
        public Path cachePath() { return cachePath; }

        /// Returns the file extension.
        ///
        /// @return the file extension
        public String extension() { return extension; }
    }

    /// Creates a new virtual predicate test data view.
    ///
    /// @param cachedir     the local cache directory
    /// @param datasetEntry the dataset entry with metadata
    /// @param facetSpecs   map of facet kind names to their source specs
    public VirtualPredicateTestDataView(Path cachedir, DatasetEntry datasetEntry,
                                        Map<String, FacetSpec> facetSpecs) {
        this.cachedir = cachedir;
        this.datasetEntry = datasetEntry;
        this.facetSpecs = facetSpecs;
    }

    @Override
    public Optional<PredicateContext> getPredicateContext() {
        resolvePredicateContext();
        return Optional.ofNullable(predicateContext);
    }

    @Override
    public Optional<Predicates<PNode<?>>> getPredicatesView() {
        if (predicatesView != null) return Optional.of(predicatesView);
        PredicateStoreBackend backend = resolveBackend("metadata_predicates");
        if (backend == null) return Optional.empty();
        resolvePredicateContext();
        predicatesView = new PredicatesDatasetView(backend, predicateContext);
        return Optional.of(predicatesView);
    }

    @Override
    public Optional<ResultIndices> getResultIndices() {
        if (resultIndicesView != null) return Optional.of(resultIndicesView);
        PredicateStoreBackend backend = resolveBackend("predicate_results");
        if (backend == null) return Optional.empty();
        resultIndicesView = new ResultIndicesDatasetView(backend);
        return Optional.of(resultIndicesView);
    }

    @Override
    public Optional<MetadataLayout> getMetadataLayout() {
        if (metadataLayout != null) return Optional.of(metadataLayout);
        PredicateStoreBackend backend = resolveBackend("metadata_layout");
        if (backend == null) return Optional.empty();
        Optional<ByteBuffer> layoutBuf = backend.getMetadataLayout();
        if (layoutBuf.isEmpty()) return Optional.empty();
        metadataLayout = MetadataLayoutImpl.fromBuffer(layoutBuf.get());
        return Optional.of(metadataLayout);
    }

    @Override
    public Optional<MetadataContent> getMetadataContentView() {
        if (metadataContentView != null) return Optional.of(metadataContentView);
        PredicateStoreBackend backend = resolveBackend("metadata_content");
        if (backend == null) return Optional.empty();
        Optional<MetadataLayout> layout = getMetadataLayout();
        if (layout.isEmpty()) return Optional.empty();
        metadataContentView = new MetadataContentDatasetView(backend, layout.get());
        return Optional.of(metadataContentView);
    }

    @Override
    public CompletableFuture<Void> prebuffer() {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (PredicateStoreBackend backend : backendsByPath.values()) {
            futures.add(backend.prebuffer());
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    @Override
    public void close() throws Exception {
        for (AutoCloseable c : closeables) {
            try {
                c.close();
            } catch (Exception ignored) {}
        }
        closeables.clear();
        backendsByPath.clear();
    }

    /// Resolves the predicate context by loading the metadata layout and probing the
    /// first predicate to determine whether indexed or named field mode is in use.
    private void resolvePredicateContext() {
        if (predicateContext != null) return;
        Optional<MetadataLayout> layout = getMetadataLayout();
        if (layout.isEmpty()) return;

        PredicateStoreBackend backend = resolveBackend("metadata_predicates");
        if (backend == null || backend.getPredicateCount() == 0) {
            predicateContext = PredicateContext.indexed(layout.get());
            return;
        }
        Optional<java.nio.ByteBuffer> first = backend.getPredicate(0);
        if (first.isEmpty()) {
            predicateContext = PredicateContext.indexed(layout.get());
            return;
        }

        java.nio.ByteBuffer buf = first.get();
        int pos = buf.position();
        buf.get(); // skip PRED tag
        int fieldCount = layout.get().getFieldCount();

        if (buf.remaining() >= 2) {
            int b1 = Byte.toUnsignedInt(buf.get());
            int b2 = Byte.toUnsignedInt(buf.get());
            buf.position(pos);

            boolean couldBeIndexed = b1 < fieldCount && b2 < io.nosqlbench.vectordata.spec.predicates.OpType.values().length;
            if (couldBeIndexed) {
                predicateContext = PredicateContext.indexed(layout.get());
            } else {
                predicateContext = PredicateContext.named(layout.get());
            }
        } else {
            buf.position(pos);
            predicateContext = PredicateContext.indexed(layout.get());
        }
    }

    private PredicateStoreBackend resolveBackend(String kindName) {
        FacetSpec spec = facetSpecs.get(kindName);
        if (spec == null) return null;

        String pathKey = spec.cachePath().toAbsolutePath().toString();
        if (backendsByPath.containsKey(pathKey)) {
            return backendsByPath.get(pathKey);
        }

        SourceType sourceType = SourceType.inferFromPath(spec.extension());
        try {
            PredicateStoreBackend backend;
            switch (sourceType) {
                case SLAB: {
                    MAFileChannel channel = new MAFileChannel(
                        spec.cachePath(),
                        spec.cachePath().resolveSibling(spec.cachePath().getFileName() + ".mrkl"),
                        spec.sourceUrl().toString());
                    closeables.add(channel);
                    backend = new SlabtasticPredicateBackend(channel, channel.size());
                    break;
                }
                case SQLITE: {
                    // SQLite needs a real file — prebuffer the entire thing first
                    MAFileChannel channel = new MAFileChannel(
                        spec.cachePath(),
                        spec.cachePath().resolveSibling(spec.cachePath().getFileName() + ".mrkl"),
                        spec.sourceUrl().toString());
                    closeables.add(channel);
                    channel.prebuffer(0, channel.size()).get();
                    Path localPath = ((CacheFileAccessor) channel).getCacheFilePath();
                    backend = new SQLitePredicateBackend(localPath);
                    break;
                }
                default:
                    return null;
            }
            closeables.add(backend);
            backendsByPath.put(pathKey, backend);
            return backend;
        } catch (IOException | SQLException | java.util.concurrent.ExecutionException
                 | InterruptedException e) {
            throw new RuntimeException("Failed to open virtual predicate backend for " + spec.cachePath(), e);
        }
    }
}
