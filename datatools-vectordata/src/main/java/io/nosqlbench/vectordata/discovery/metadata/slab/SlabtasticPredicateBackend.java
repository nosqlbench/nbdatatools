package io.nosqlbench.vectordata.discovery.metadata.slab;

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

import io.nosqlbench.slabtastic.SlabReader;
import io.nosqlbench.vectordata.discovery.metadata.PredicateStoreBackend;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/// Slabtastic-backed implementation of {@link PredicateStoreBackend}.
///
/// A single {@code .slab} file stores four namespaces:
/// - {@code predicates} — serialized predicate trees
/// - {@code result_indices} — length-prefixed int arrays
/// - {@code metadata_layout} — schema descriptor (ordinal 0 only)
/// - {@code metadata_content} — serialized metadata records
public class SlabtasticPredicateBackend implements PredicateStoreBackend {

    /// Namespace name for predicate records
    public static final String NS_PREDICATES = "predicates";
    /// Namespace name for result index records
    public static final String NS_RESULT_INDICES = "result_indices";
    /// Namespace name for the metadata layout record
    public static final String NS_METADATA_LAYOUT = "metadata_layout";
    /// Namespace name for metadata content records
    public static final String NS_METADATA_CONTENT = "metadata_content";

    private final SlabReader reader;
    private final Map<String, String> namespaceOverrides = new HashMap<>();

    /// Creates a backend from an externally provided channel.
    ///
    /// @param channel  the async file channel (e.g. MAFileChannel)
    /// @param fileSize the total file size in bytes
    /// @throws IOException if the channel data is not a valid slabtastic file
    public SlabtasticPredicateBackend(AsynchronousFileChannel channel, long fileSize) throws IOException {
        this.reader = new SlabReader(channel, fileSize);
    }

    /// Creates a backend from a local file path.
    ///
    /// @param path the slab file to read
    /// @throws IOException if the file cannot be opened or is invalid
    public SlabtasticPredicateBackend(Path path) throws IOException {
        this.reader = new SlabReader(path);
    }

    /// Overrides the namespace used for a given default namespace constant.
    ///
    /// For example, calling {@code overrideNamespace(NS_PREDICATES, "my_preds")}
    /// causes {@link #getPredicate(long)} to read from the {@code "my_preds"}
    /// namespace instead of {@code "predicates"}.
    ///
    /// @param defaultNamespace one of the {@code NS_*} constants
    /// @param actualNamespace  the namespace to read from instead
    public void overrideNamespace(String defaultNamespace, String actualNamespace) {
        namespaceOverrides.put(defaultNamespace, actualNamespace);
    }

    /// Resolves the effective namespace for a given default, checking
    /// overrides first.
    private String ns(String defaultNamespace) {
        return namespaceOverrides.getOrDefault(defaultNamespace, defaultNamespace);
    }

    @Override
    public Optional<ByteBuffer> getPredicate(long ordinal) {
        return reader.get(ns(NS_PREDICATES), ordinal);
    }

    @Override
    public Optional<ByteBuffer> getResultIndices(long ordinal) {
        return reader.get(ns(NS_RESULT_INDICES), ordinal);
    }

    @Override
    public Optional<ByteBuffer> getMetadataLayout() {
        return reader.get(ns(NS_METADATA_LAYOUT), 0);
    }

    @Override
    public Optional<ByteBuffer> getMetadataContent(long ordinal) {
        return reader.get(ns(NS_METADATA_CONTENT), ordinal);
    }

    @Override
    public long getPredicateCount() {
        return safeRecordCount(ns(NS_PREDICATES));
    }

    @Override
    public long getResultIndicesCount() {
        return safeRecordCount(ns(NS_RESULT_INDICES));
    }

    @Override
    public long getMetadataContentCount() {
        return safeRecordCount(ns(NS_METADATA_CONTENT));
    }

    @Override
    public CompletableFuture<Void> prebuffer() {
        // For local files, no prebuffering needed.
        // For MAFileChannel-backed channels, callers should prebuffer
        // via pageRanges() before constructing the reader.
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void close() throws Exception {
        reader.close();
    }

    /// Returns the underlying reader for advanced operations.
    ///
    /// @return the slab reader
    public SlabReader getReader() {
        return reader;
    }

    private long safeRecordCount(String namespace) {
        if (!reader.namespaces().contains(namespace)) {
            return 0;
        }
        return reader.recordCount(namespace);
    }
}
