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

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/// Backend-agnostic interface for reading predicate test data from a file.
///
/// Implementations exist for slabtastic ({@code .slab}) and SQLite
/// ({@code .db}) formats. Each backend provides ordinal-based access
/// to four content namespaces: predicates, result indices, metadata
/// layout, and metadata content.
public interface PredicateStoreBackend extends AutoCloseable {

    /// Retrieves the predicate at the given ordinal.
    ///
    /// @param ordinal the predicate ordinal
    /// @return the raw predicate bytes, or empty if not present
    Optional<ByteBuffer> getPredicate(long ordinal);

    /// Retrieves the result indices at the given ordinal.
    ///
    /// @param ordinal the result set ordinal
    /// @return the raw index data bytes, or empty if not present
    Optional<ByteBuffer> getResultIndices(long ordinal);

    /// Retrieves the metadata layout (schema) record.
    ///
    /// Only ordinal 0 is meaningful for layout data.
    ///
    /// @return the raw layout bytes, or empty if not present
    Optional<ByteBuffer> getMetadataLayout();

    /// Retrieves a metadata content record at the given ordinal.
    ///
    /// @param ordinal the record ordinal
    /// @return the raw content bytes, or empty if not present
    Optional<ByteBuffer> getMetadataContent(long ordinal);

    /// Returns the number of predicate records.
    ///
    /// @return the predicate count
    long getPredicateCount();

    /// Returns the number of result index records.
    ///
    /// @return the result indices count
    long getResultIndicesCount();

    /// Returns the number of metadata content records.
    ///
    /// @return the metadata content count
    long getMetadataContentCount();

    /// Asynchronously prebuffers data for efficient subsequent reads.
    ///
    /// @return a future that completes when prebuffering is done
    CompletableFuture<Void> prebuffer();
}
