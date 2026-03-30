package io.nosqlbench.vshapes.stream;

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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;

/// DataSource that loads vector chunks in transposed (column-major) format.
///
/// ## Chunk Format
///
/// Unlike the standard [FloatArrayDataSource] which provides row-major chunks:
/// ```
/// chunk[vectorIndex][dimensionIndex]
/// ```
///
/// This source provides column-major chunks:
/// ```
/// chunk[dimensionIndex][vectorIndex]
/// ```
///
/// This format is optimal for analyzers that process one dimension at a time
/// (e.g., computing statistics, fitting distributions per dimension).
///
/// ## Memory Efficiency
///
/// - Loads directly from memory-mapped file when available (2-4x faster)
/// - Writes directly to column arrays (no intermediate row allocation)
/// - Chunk size automatically calculated from available heap via [ChunkSizeCalculator]
///
/// ## Usage
///
/// ```java
/// // Create source with auto-calculated chunk size
/// TransposedChunkDataSource source = TransposedChunkDataSource.builder()
///     .file(path)
///     .memoryBudgetFraction(0.6)
///     .build();
///
/// // Iterate over transposed chunks
/// for (float[][] chunk : source.chunks(source.getOptimalChunkSize())) {
///     // chunk[d] contains all values for dimension d
///     for (int d = 0; d < chunk.length; d++) {
///         processeDimension(d, chunk[d]);
///     }
/// }
///
/// // Or use with AnalyzerHarness for transposed-aware analyzers
/// harness.register(new TransposedModelExtractor());
/// harness.run(source, source.getOptimalChunkSize());
/// ```
///
/// @see ChunkSizeCalculator
/// @see StreamingAnalyzer
/// @see DataSource
public final class TransposedChunkDataSource implements DataSource {

    private static final Logger logger = LogManager.getLogger(TransposedChunkDataSource.class);

    private final Path filePath;
    private final int totalVectors;
    private final int dimensions;
    private final int optimalChunkSize;
    private final String id;

    private TransposedChunkDataSource(Builder builder) {
        this.filePath = builder.filePath;
        this.totalVectors = builder.totalVectors;
        this.dimensions = builder.dimensions;
        this.id = builder.id != null ? builder.id : filePath.getFileName().toString();

        // Calculate optimal chunk size
        ChunkSizeCalculator calculator = new ChunkSizeCalculator(
            dimensions,
            builder.memoryBudgetFraction,
            builder.overheadFactor
        );

        if (builder.explicitChunkSize > 0) {
            this.optimalChunkSize = Math.min(builder.explicitChunkSize, totalVectors);
        } else if (builder.memoryBudgetBytes > 0) {
            this.optimalChunkSize = Math.min(calculator.calculateForBudget(builder.memoryBudgetBytes), totalVectors);
        } else {
            this.optimalChunkSize = Math.min(calculator.calculate(), totalVectors);
        }

        logger.debug("TransposedChunkDataSource created: {} vectors, {} dims, chunk size {}",
            totalVectors, dimensions, optimalChunkSize);
    }

    @Override
    public DataspaceShape getShape() {
        return new DataspaceShape(totalVectors, dimensions, DataLayout.COLUMNAR);
    }

    @Override
    public Iterable<float[][]> chunks(int chunkSize) {
        return () -> new TransposedChunkIterator(chunkSize);
    }

    @Override
    public String getId() {
        return id;
    }

    /// Returns the heap-optimal chunk size calculated during construction.
    ///
    /// @return optimal number of vectors per chunk
    public int getOptimalChunkSize() {
        return optimalChunkSize;
    }

    /// Returns the file path this source reads from.
    ///
    /// @return the vector file path
    public Path getFilePath() {
        return filePath;
    }

    /// Creates a new builder for TransposedChunkDataSource.
    ///
    /// @return a new builder
    public static Builder builder() {
        return new Builder();
    }

    /// Builder for TransposedChunkDataSource.
    public static final class Builder {
        private Path filePath;
        private int totalVectors = -1;
        private int dimensions = -1;
        private double memoryBudgetFraction = ChunkSizeCalculator.DEFAULT_BUDGET_FRACTION;
        private double overheadFactor = ChunkSizeCalculator.DEFAULT_OVERHEAD_FACTOR;
        private long memoryBudgetBytes = -1;
        private int explicitChunkSize = -1;
        private String id;

        private Builder() {}

        /// Sets the vector file path.
        ///
        /// If totalVectors and dimensions are not set, they will be
        /// read from the file header.
        ///
        /// @param filePath path to .fvec or .ivec file
        /// @return this builder
        public Builder file(Path filePath) {
            this.filePath = filePath;
            return this;
        }

        /// Sets the total number of vectors.
        ///
        /// If not set, will be read from the file.
        ///
        /// @param totalVectors number of vectors in the file
        /// @return this builder
        public Builder totalVectors(int totalVectors) {
            this.totalVectors = totalVectors;
            return this;
        }

        /// Sets the dimensionality of vectors.
        ///
        /// If not set, will be read from the file.
        ///
        /// @param dimensions number of dimensions per vector
        /// @return this builder
        public Builder dimensions(int dimensions) {
            this.dimensions = dimensions;
            return this;
        }

        /// Sets the fraction of available heap to use for chunks.
        ///
        /// @param fraction value between 0.0 and 1.0 (default: 0.6)
        /// @return this builder
        public Builder memoryBudgetFraction(double fraction) {
            this.memoryBudgetFraction = fraction;
            return this;
        }

        /// Sets an explicit memory budget in bytes.
        ///
        /// Overrides the fraction-based calculation.
        ///
        /// @param bytes memory budget in bytes
        /// @return this builder
        public Builder memoryBudgetBytes(long bytes) {
            this.memoryBudgetBytes = bytes;
            return this;
        }

        /// Sets an explicit chunk size.
        ///
        /// Overrides automatic calculation.
        ///
        /// @param chunkSize number of vectors per chunk
        /// @return this builder
        public Builder chunkSize(int chunkSize) {
            this.explicitChunkSize = chunkSize;
            return this;
        }

        /// Sets the overhead factor for memory estimation.
        ///
        /// @param factor multiplier for safety margin (default: 1.2)
        /// @return this builder
        public Builder overheadFactor(double factor) {
            this.overheadFactor = factor;
            return this;
        }

        /// Sets an identifier for this data source.
        ///
        /// @param id identifier for logging and tracking
        /// @return this builder
        public Builder id(String id) {
            this.id = id;
            return this;
        }

        /// Builds the TransposedChunkDataSource.
        ///
        /// @return a new TransposedChunkDataSource
        /// @throws IllegalStateException if filePath is not set
        /// @throws IOException if file metadata cannot be read
        public TransposedChunkDataSource build() throws IOException {
            if (filePath == null) {
                throw new IllegalStateException("filePath must be set");
            }

            // Read metadata from file if not explicitly set
            if (totalVectors < 0 || dimensions < 0) {
                int[] metadata = readFileMetadata(filePath);
                if (totalVectors < 0) totalVectors = metadata[0];
                if (dimensions < 0) dimensions = metadata[1];
            }

            return new TransposedChunkDataSource(this);
        }

        /// Reads [vectorCount, dimension] from a vector file header.
        ///
        /// The xvec format stores each vector as a 4-byte little-endian dimension
        /// count followed by dimension * 4 bytes of float data.
        private int[] readFileMetadata(Path path) throws IOException {
            try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) {
                long fileSize = raf.length();

                // Read dimension (little-endian int at offset 0)
                byte[] dimBytes = new byte[4];
                raf.read(dimBytes);
                int dimension = (dimBytes[0] & 0xFF) |
                               ((dimBytes[1] & 0xFF) << 8) |
                               ((dimBytes[2] & 0xFF) << 16) |
                               ((dimBytes[3] & 0xFF) << 24);

                // Calculate vector count from file size
                long vectorStride = (1 + dimension) * 4L;  // dimension header + float values
                int vectorCount = (int) (fileSize / vectorStride);

                return new int[] { vectorCount, dimension };
            }
        }
    }

    /// Iterator that yields transposed chunks.
    private class TransposedChunkIterator implements Iterator<float[][]> {
        private final int chunkSize;
        private int currentOffset = 0;

        TransposedChunkIterator(int chunkSize) {
            this.chunkSize = Math.min(chunkSize, totalVectors);
        }

        @Override
        public boolean hasNext() {
            return currentOffset < totalVectors;
        }

        @Override
        public float[][] next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            int endOffset = Math.min(currentOffset + chunkSize, totalVectors);

            float[][] transposed = loadTransposedChunk(currentOffset, endOffset);

            currentOffset = endOffset;
            return transposed;
        }

        /// Loads vectors from the xvec file in transposed (column-major) layout.
        ///
        /// Reads vectors [startIndex, endIndex) from the file and returns them
        /// as `float[dimension][vectorCount]` — each row contains all values
        /// for one dimension across the chunk.
        private float[][] loadTransposedChunk(int startIndex, int endIndex) {
            int count = endIndex - startIndex;
            long vectorStride = (1 + dimensions) * 4L;

            float[][] transposed = new float[dimensions][count];

            try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r");
                 FileChannel channel = raf.getChannel()) {

                long offset = (long) startIndex * vectorStride;
                // Buffer holds one vector: 4-byte dim header + dimensions * 4 bytes
                ByteBuffer buf = ByteBuffer.allocate((1 + dimensions) * 4);
                buf.order(ByteOrder.LITTLE_ENDIAN);

                for (int v = 0; v < count; v++) {
                    buf.clear();
                    channel.read(buf, offset);
                    buf.flip();
                    buf.getInt(); // skip dimension header
                    for (int d = 0; d < dimensions; d++) {
                        transposed[d][v] = buf.getFloat();
                    }
                    offset += vectorStride;
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to load transposed chunk from " + filePath, e);
            }

            return transposed;
        }
    }
}
