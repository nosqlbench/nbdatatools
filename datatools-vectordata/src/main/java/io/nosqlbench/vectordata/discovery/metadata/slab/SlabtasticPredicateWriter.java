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

import io.nosqlbench.slabtastic.SlabWriter;
import io.nosqlbench.vectordata.discovery.metadata.MetadataLayoutImpl;
import io.nosqlbench.vectordata.discovery.metadata.MetadataRecordCodec;
import io.nosqlbench.vectordata.spec.predicates.PNode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Map;

/// Writes predicate test data to a single slabtastic ({@code .slab})
/// file with four namespaces.
///
/// Usage:
/// ```java
/// try (var writer = new SlabtasticPredicateWriter(path, layout)) {
///     writer.writePredicate(0, predicate);
///     writer.writeResultIndices(0, new int[]{1, 2, 3});
///     writer.writeMetadataRecord(0, Map.of("color", "red"));
/// }
/// ```
public class SlabtasticPredicateWriter implements AutoCloseable {

    private final SlabWriter writer;
    private final MetadataLayoutImpl layout;

    /// Creates a new writer targeting the given file path.
    ///
    /// The metadata layout is written as ordinal 0 in the
    /// {@code metadata_layout} namespace during construction.
    ///
    /// @param path   the output slab file
    /// @param layout the metadata layout (schema)
    /// @throws IOException if the file cannot be opened
    public SlabtasticPredicateWriter(Path path, MetadataLayoutImpl layout) throws IOException {
        this(path, layout, 65536);
    }

    /// Creates a new writer with a custom preferred page size.
    ///
    /// @param path              the output slab file
    /// @param layout            the metadata layout (schema)
    /// @param preferredPageSize the preferred page size in bytes
    /// @throws IOException if the file cannot be opened
    public SlabtasticPredicateWriter(Path path, MetadataLayoutImpl layout, int preferredPageSize)
        throws IOException
    {
        this.writer = new SlabWriter(path, preferredPageSize);
        this.layout = layout;
        // Write layout as ordinal 0
        writer.write(SlabtasticPredicateBackend.NS_METADATA_LAYOUT, 0, layout.encode());
    }

    /// Writes a predicate at the given ordinal.
    ///
    /// @param ordinal   the predicate ordinal
    /// @param predicate the predicate tree node
    /// @throws IOException if writing fails
    public void writePredicate(long ordinal, PNode<?> predicate) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(4096).order(ByteOrder.LITTLE_ENDIAN);
        predicate.encode(buf);
        buf.flip();
        byte[] data = new byte[buf.remaining()];
        buf.get(data);
        writer.write(SlabtasticPredicateBackend.NS_PREDICATES, ordinal, data);
    }

    /// Writes result indices at the given ordinal.
    ///
    /// The indices are encoded as: `[count:4][index0:4][index1:4]...`
    /// using little-endian byte order.
    ///
    /// @param ordinal the result set ordinal
    /// @param indices the matching metadata record ordinals
    /// @throws IOException if writing fails
    public void writeResultIndices(long ordinal, int[] indices) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(4 + indices.length * 4).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(indices.length);
        for (int idx : indices) {
            buf.putInt(idx);
        }
        writer.write(SlabtasticPredicateBackend.NS_RESULT_INDICES, ordinal, buf.array());
    }

    /// Writes a metadata content record at the given ordinal.
    ///
    /// @param ordinal the record ordinal
    /// @param record  the field values keyed by name
    /// @throws IOException if writing fails
    public void writeMetadataRecord(long ordinal, Map<String, Object> record) throws IOException {
        byte[] encoded = MetadataRecordCodec.encode(layout, record);
        writer.write(SlabtasticPredicateBackend.NS_METADATA_CONTENT, ordinal, encoded);
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
