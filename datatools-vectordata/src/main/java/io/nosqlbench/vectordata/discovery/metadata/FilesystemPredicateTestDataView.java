package io.nosqlbench.vectordata.discovery.metadata;

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

import io.nosqlbench.vectordata.discovery.TestDataGroup;
import io.nosqlbench.vectordata.discovery.metadata.slab.SlabtasticPredicateBackend;
import io.nosqlbench.vectordata.discovery.metadata.sqlite.SQLitePredicateBackend;
import io.nosqlbench.vectordata.discovery.metadata.views.MetadataContentDatasetView;
import io.nosqlbench.vectordata.discovery.metadata.views.PredicatesDatasetView;
import io.nosqlbench.vectordata.discovery.metadata.views.ResultIndicesDatasetView;
import io.nosqlbench.vectordata.layout.SourceType;
import io.nosqlbench.vectordata.layoutv2.DSProfile;
import io.nosqlbench.vectordata.layoutv2.DSSource;
import io.nosqlbench.vectordata.layoutv2.DSView;
import io.nosqlbench.vectordata.spec.datasets.types.TestDataKind;
import io.nosqlbench.vectordata.spec.predicates.PNode;
import io.nosqlbench.vectordata.spec.predicates.PredicateContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/// Filesystem-backed implementation of {@link PredicateTestDataView}.
///
/// Constructed from a {@link TestDataGroup} and {@link DSProfile}, this
/// class resolves predicate facet files from the profile, determines
/// the backend type from the file extension ({@code .slab} or
/// {@code .db}/{@code .sqlite}), and wires up typed dataset view
/// adapters.
///
/// When all four predicate facets point to the same {@code .slab} file,
/// a single shared backend instance is used.
///
/// Namespace resolution for slab sources follows this precedence:
/// 1. Explicit namespace in the source spec (e.g. {@code file.slab:my_ns})
/// 2. Facet name as fallback when the slab has non-default namespaces
/// 3. Default namespace constants in {@link SlabtasticPredicateBackend}
public class FilesystemPredicateTestDataView implements PredicateTestDataView<PNode<?>>, AutoCloseable {

    private static final Logger logger = LogManager.getLogger(FilesystemPredicateTestDataView.class);

    /// Maps facet kind names to the default slab namespace they control.
    private static final Map<String, String> FACET_TO_DEFAULT_NS = Map.of(
        TestDataKind.metadata_predicates.name(), SlabtasticPredicateBackend.NS_PREDICATES,
        TestDataKind.predicate_results.name(), SlabtasticPredicateBackend.NS_RESULT_INDICES,
        TestDataKind.metadata_layout.name(), SlabtasticPredicateBackend.NS_METADATA_LAYOUT,
        TestDataKind.metadata_content.name(), SlabtasticPredicateBackend.NS_METADATA_CONTENT
    );

    private final TestDataGroup dataGroup;
    private final DSProfile profile;
    private final String profileName;
    private final List<PredicateStoreBackend> backends = new ArrayList<>();
    private final Map<String, PredicateStoreBackend> backendsByPath = new LinkedHashMap<>();

    private Predicates<PNode<?>> predicatesView;
    private ResultIndices resultIndicesView;
    private MetadataLayout metadataLayout;
    private MetadataContent metadataContentView;
    private PredicateContext predicateContext;

    /// Creates a new filesystem predicate test data view.
    ///
    /// @param dataGroup   the data group containing dataset metadata
    /// @param profile     the profile configuration
    /// @param profileName the name of this profile
    public FilesystemPredicateTestDataView(TestDataGroup dataGroup, DSProfile profile, String profileName) {
        this.dataGroup = dataGroup;
        this.profile = profile;
        this.profileName = profileName;
    }

    @Override
    public Optional<PredicateContext> getPredicateContext() {
        resolvePredicateContext();
        return Optional.ofNullable(predicateContext);
    }

    @Override
    public Optional<Predicates<PNode<?>>> getPredicatesView() {
        if (predicatesView != null) {
            return Optional.of(predicatesView);
        }
        PredicateStoreBackend backend = resolveBackend(TestDataKind.metadata_predicates.name());
        if (backend == null) return Optional.empty();
        resolvePredicateContext();
        predicatesView = new PredicatesDatasetView(backend, predicateContext);
        return Optional.of(predicatesView);
    }

    @Override
    public Optional<ResultIndices> getResultIndices() {
        if (resultIndicesView != null) {
            return Optional.of(resultIndicesView);
        }
        PredicateStoreBackend backend = resolveBackend(TestDataKind.predicate_results.name());
        if (backend == null) return Optional.empty();
        resultIndicesView = new ResultIndicesDatasetView(backend);
        return Optional.of(resultIndicesView);
    }

    @Override
    public Optional<MetadataLayout> getMetadataLayout() {
        if (metadataLayout != null) {
            return Optional.of(metadataLayout);
        }
        PredicateStoreBackend backend = resolveBackend(TestDataKind.metadata_layout.name());
        if (backend == null) return Optional.empty();
        Optional<ByteBuffer> layoutBuf = backend.getMetadataLayout();
        if (layoutBuf.isEmpty()) return Optional.empty();
        metadataLayout = MetadataLayoutImpl.fromBuffer(layoutBuf.get());
        return Optional.of(metadataLayout);
    }

    @Override
    public Optional<MetadataContent> getMetadataContentView() {
        if (metadataContentView != null) {
            return Optional.of(metadataContentView);
        }
        PredicateStoreBackend backend = resolveBackend(TestDataKind.metadata_content.name());
        if (backend == null) return Optional.empty();
        Optional<MetadataLayout> layout = getMetadataLayout();
        if (layout.isEmpty()) return Optional.empty();
        metadataContentView = new MetadataContentDatasetView(backend, layout.get());
        return Optional.of(metadataContentView);
    }

    @Override
    public CompletableFuture<Void> prebuffer() {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (PredicateStoreBackend backend : backends) {
            futures.add(backend.prebuffer());
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    @Override
    public void close() throws Exception {
        for (PredicateStoreBackend backend : backends) {
            try {
                backend.close();
            } catch (Exception e) {
                logger.warn("Failed to close predicate backend: {}", e.getMessage());
            }
        }
        backends.clear();
        backendsByPath.clear();
    }

    /// Resolves the predicate context by loading the metadata layout and probing the
    /// first predicate to determine whether indexed or named field mode is in use.
    private void resolvePredicateContext() {
        if (predicateContext != null) return;
        Optional<MetadataLayout> layout = getMetadataLayout();
        if (layout.isEmpty()) return;

        // Probe the first predicate to detect the field mode
        PredicateStoreBackend backend = resolveBackend(TestDataKind.metadata_predicates.name());
        if (backend == null || backend.getPredicateCount() == 0) {
            predicateContext = PredicateContext.indexed(layout.get());
            return;
        }
        Optional<java.nio.ByteBuffer> first = backend.getPredicate(0);
        if (first.isEmpty()) {
            predicateContext = PredicateContext.indexed(layout.get());
            return;
        }

        // Peek at the wire format after the PRED tag byte to distinguish modes.
        // Indexed format: [PRED:1][field:1][op:1]... — field byte is 0..fieldCount
        // Named format:   [PRED:1][nameLen:2][nameBytes:N][op:1]... — nameLen is usually > fieldCount
        java.nio.ByteBuffer buf = first.get();
        int pos = buf.position();
        buf.get(); // skip PRED tag
        int fieldCount = layout.get().getFieldCount();

        // In indexed mode the next byte is a field index (0..fieldCount-1).
        // In named mode the next two bytes are a UTF-8 length which will be >= 1.
        // We read the first byte: if it's in [0, fieldCount) AND the byte after it
        // is a valid OpType ordinal, it's indexed. Otherwise it's named.
        if (buf.remaining() >= 2) {
            int b1 = Byte.toUnsignedInt(buf.get());
            int b2 = Byte.toUnsignedInt(buf.get());
            buf.position(pos); // reset

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
        DSView view = profile.get(kindName);
        if (view == null) return null;

        DSSource source = view.getSource();
        String filename = source.getPath();
        Path filePath = dataGroup.getDatasetDirectory().resolve(filename);
        String pathKey = filePath.toAbsolutePath().toString();

        // Share backends for the same file
        boolean isNew = !backendsByPath.containsKey(pathKey);
        PredicateStoreBackend backend;
        if (!isNew) {
            backend = backendsByPath.get(pathKey);
        } else {
            if (!Files.exists(filePath)) {
                logger.warn("Predicate data file not found: {}", filePath);
                return null;
            }

            SourceType sourceType = SourceType.inferFromPath(filename);
            try {
                switch (sourceType) {
                    case SLAB:
                        backend = new SlabtasticPredicateBackend(filePath);
                        break;
                    case SQLITE:
                        backend = new SQLitePredicateBackend(filePath);
                        break;
                    default:
                        logger.warn("Unsupported source type {} for predicate data: {}", sourceType, filename);
                        return null;
                }
            } catch (IOException | SQLException e) {
                throw new RuntimeException("Failed to open predicate backend for " + filePath, e);
            }

            backends.add(backend);
            backendsByPath.put(pathKey, backend);
            logger.debug("Opened {} predicate backend for profile '{}': {}",
                sourceType, profileName, filename);
        }

        // Apply namespace resolution for slab backends
        if (backend instanceof SlabtasticPredicateBackend) {
            SlabtasticPredicateBackend slabBackend = (SlabtasticPredicateBackend) backend;
            String defaultNs = FACET_TO_DEFAULT_NS.get(kindName);
            if (defaultNs != null) {
                String effectiveNs = resolveNamespace(source, kindName, slabBackend);
                if (effectiveNs != null && !effectiveNs.equals(defaultNs)) {
                    slabBackend.overrideNamespace(defaultNs, effectiveNs);
                    logger.debug("Namespace override for facet '{}': {} -> {}",
                        kindName, defaultNs, effectiveNs);
                }
            }
        }

        return backend;
    }

    /// Resolves the effective namespace for a slab source.
    ///
    /// Precedence:
    /// 1. Explicit namespace in the source spec
    /// 2. Facet name as fallback when the slab has non-default namespaces
    /// 3. null (use backend defaults)
    private String resolveNamespace(DSSource source, String kindName,
                                     SlabtasticPredicateBackend slabBackend) {
        // Explicit namespace from source spec takes priority
        if (source.getNamespace() != null) {
            return source.getNamespace();
        }

        // Fallback: try the facet name if the slab has non-default namespaces
        Set<String> slabNamespaces = slabBackend.getReader().namespaces();
        boolean hasNonDefault = slabNamespaces.size() > 1
            || (slabNamespaces.size() == 1 && !slabNamespaces.contains(""));
        if (hasNonDefault && slabNamespaces.contains(kindName)) {
            return kindName;
        }

        return null;
    }

    @Override
    public String toString() {
        return String.format("FilesystemPredicateTestDataView{profile='%s', dataset=%s}",
            profileName, dataGroup.getName());
    }
}
