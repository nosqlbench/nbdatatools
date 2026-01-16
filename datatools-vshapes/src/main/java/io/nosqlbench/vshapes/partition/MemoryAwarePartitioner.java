package io.nosqlbench.vshapes.partition;

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

import java.util.Iterator;
import java.util.NoSuchElementException;

/// Memory-aware partitioner for processing large vector datasets.
///
/// ## Purpose
///
/// When processing vector files that exceed available heap memory, this class
/// computes optimal partition sizes to avoid OutOfMemoryError while maximizing
/// throughput.
///
/// ## Memory Model
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────┐
/// │                    MEMORY ALLOCATION                            │
/// └─────────────────────────────────────────────────────────────────┘
///
///   Available Heap (Xmx)
///   ┌────────────────────────────────────────────────────────────┐
///   │  70% for vector data   │  30% for JVM overhead + analysis  │
///   └────────────────────────────────────────────────────────────┘
///                ▲
///                │
///   Partition Size = (availableHeap × 0.7) ÷ (dimensions × 4 bytes)
/// ```
///
/// ## Usage
///
/// ```java
/// // Auto-detect available memory
/// MemoryAwarePartitioner partitioner = MemoryAwarePartitioner.forDataset(
///     1_000_000,  // totalVectors
///     384         // dimensions
/// );
///
/// System.out.println("Partition size: " + partitioner.partitionSize());
/// System.out.println("Number of partitions: " + partitioner.partitionCount());
///
/// // Process partitions
/// for (Partition partition : partitioner.partitions()) {
///     float[][] chunk = loadVectors(partition.start(), partition.end());
///     process(chunk);
/// }
/// ```
///
/// ## Manual Override
///
/// ```java
/// // Force specific memory limit (e.g., from --max-memory CLI option)
/// MemoryAwarePartitioner partitioner = MemoryAwarePartitioner.builder()
///     .totalVectors(1_000_000)
///     .dimensions(384)
///     .maxMemoryBytes(4L * 1024 * 1024 * 1024)  // 4GB
///     .build();
/// ```
///
/// @see io.nosqlbench.vshapes.checkpoint.CheckpointManager
public final class MemoryAwarePartitioner {

    /// Default fraction of heap to use for vector data (70%).
    public static final double DEFAULT_HEAP_FRACTION = 0.70;

    /// Minimum partition size to avoid excessive overhead.
    public static final int MIN_PARTITION_SIZE = 1000;

    /// Maximum partition size to enable progress reporting.
    public static final int MAX_PARTITION_SIZE = 10_000_000;

    private final int totalVectors;
    private final int dimensions;
    private final long maxMemoryBytes;
    private final double heapFraction;
    private final int partitionSize;
    private final int partitionCount;

    private MemoryAwarePartitioner(int totalVectors, int dimensions,
                                    long maxMemoryBytes, double heapFraction) {
        if (totalVectors <= 0) {
            throw new IllegalArgumentException("totalVectors must be positive, got: " + totalVectors);
        }
        if (dimensions <= 0) {
            throw new IllegalArgumentException("dimensions must be positive, got: " + dimensions);
        }
        if (maxMemoryBytes <= 0) {
            throw new IllegalArgumentException("maxMemoryBytes must be positive, got: " + maxMemoryBytes);
        }
        if (heapFraction <= 0 || heapFraction > 1.0) {
            throw new IllegalArgumentException("heapFraction must be in (0, 1], got: " + heapFraction);
        }

        this.totalVectors = totalVectors;
        this.dimensions = dimensions;
        this.maxMemoryBytes = maxMemoryBytes;
        this.heapFraction = heapFraction;

        // Calculate partition size
        long bytesPerVector = (long) dimensions * Float.BYTES;
        long availableForData = (long) (maxMemoryBytes * heapFraction);
        int computedSize = (int) Math.min(availableForData / bytesPerVector, Integer.MAX_VALUE);

        // Apply bounds
        this.partitionSize = Math.max(MIN_PARTITION_SIZE,
                                       Math.min(MAX_PARTITION_SIZE,
                                                Math.min(computedSize, totalVectors)));

        this.partitionCount = (int) Math.ceil((double) totalVectors / partitionSize);
    }

    /// Creates a partitioner for the given dataset using auto-detected memory.
    ///
    /// @param totalVectors the total number of vectors in the dataset
    /// @param dimensions the number of dimensions per vector
    /// @return a new MemoryAwarePartitioner
    public static MemoryAwarePartitioner forDataset(int totalVectors, int dimensions) {
        return builder()
            .totalVectors(totalVectors)
            .dimensions(dimensions)
            .build();
    }

    /// Creates a builder for constructing a MemoryAwarePartitioner.
    ///
    /// @return a new builder
    public static Builder builder() {
        return new Builder();
    }

    /// Returns the total number of vectors in the dataset.
    public int totalVectors() {
        return totalVectors;
    }

    /// Returns the number of dimensions per vector.
    public int dimensions() {
        return dimensions;
    }

    /// Returns the maximum memory available (in bytes).
    public long maxMemoryBytes() {
        return maxMemoryBytes;
    }

    /// Returns the heap fraction used for vector data.
    public double heapFraction() {
        return heapFraction;
    }

    /// Returns the computed partition size (number of vectors per partition).
    public int partitionSize() {
        return partitionSize;
    }

    /// Returns the total number of partitions.
    public int partitionCount() {
        return partitionCount;
    }

    /// Returns the estimated memory usage per partition in bytes.
    public long bytesPerPartition() {
        return (long) partitionSize * dimensions * Float.BYTES;
    }

    /// Returns whether the dataset fits in a single partition.
    public boolean isSinglePartition() {
        return partitionCount == 1;
    }

    /// Returns an iterable over all partitions.
    ///
    /// @return an iterable of Partition objects
    public Iterable<Partition> partitions() {
        return () -> new PartitionIterator();
    }

    /// Returns a summary string for logging/display.
    @Override
    public String toString() {
        return String.format(
            "MemoryAwarePartitioner[vectors=%,d, dims=%d, partitionSize=%,d, partitions=%d, " +
            "memPerPartition=%,dMB, maxMem=%,dMB]",
            totalVectors, dimensions, partitionSize, partitionCount,
            bytesPerPartition() / (1024 * 1024),
            maxMemoryBytes / (1024 * 1024));
    }

    /// Represents a single partition of the dataset.
    ///
    /// @param index the zero-based partition index
    /// @param start the starting vector index (inclusive)
    /// @param end the ending vector index (exclusive)
    public record Partition(int index, int start, int end) {
        /// Returns the number of vectors in this partition.
        public int size() {
            return end - start;
        }

        /// Returns the progress of this partition as a fraction of total.
        public double progress(int totalPartitions) {
            return (double) (index + 1) / totalPartitions;
        }
    }

    /// Builder for constructing MemoryAwarePartitioner instances.
    public static final class Builder {
        private int totalVectors;
        private int dimensions;
        private Long maxMemoryBytes;
        private double heapFraction = DEFAULT_HEAP_FRACTION;

        private Builder() {}

        /// Sets the total number of vectors.
        ///
        /// @param totalVectors the total vector count
        /// @return this builder
        public Builder totalVectors(int totalVectors) {
            this.totalVectors = totalVectors;
            return this;
        }

        /// Sets the number of dimensions per vector.
        ///
        /// @param dimensions the dimension count
        /// @return this builder
        public Builder dimensions(int dimensions) {
            this.dimensions = dimensions;
            return this;
        }

        /// Sets the maximum memory to use in bytes.
        ///
        /// If not specified, defaults to auto-detection using
        /// {@code Runtime.getRuntime().maxMemory()}.
        ///
        /// @param maxMemoryBytes the maximum memory in bytes
        /// @return this builder
        public Builder maxMemoryBytes(long maxMemoryBytes) {
            this.maxMemoryBytes = maxMemoryBytes;
            return this;
        }

        /// Sets the fraction of heap to use for vector data.
        ///
        /// @param heapFraction the heap fraction (0 < x <= 1)
        /// @return this builder
        public Builder heapFraction(double heapFraction) {
            this.heapFraction = heapFraction;
            return this;
        }

        /// Builds the MemoryAwarePartitioner.
        ///
        /// @return a new MemoryAwarePartitioner
        /// @throws IllegalStateException if required parameters are not set
        public MemoryAwarePartitioner build() {
            if (totalVectors <= 0) {
                throw new IllegalStateException("totalVectors must be set");
            }
            if (dimensions <= 0) {
                throw new IllegalStateException("dimensions must be set");
            }

            long effectiveMaxMemory = (maxMemoryBytes != null)
                ? maxMemoryBytes
                : Runtime.getRuntime().maxMemory();

            return new MemoryAwarePartitioner(totalVectors, dimensions,
                                               effectiveMaxMemory, heapFraction);
        }
    }

    /// Iterator over partitions.
    private class PartitionIterator implements Iterator<Partition> {
        private int currentIndex = 0;

        @Override
        public boolean hasNext() {
            return currentIndex < partitionCount;
        }

        @Override
        public Partition next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            int start = currentIndex * partitionSize;
            int end = Math.min(start + partitionSize, totalVectors);

            Partition partition = new Partition(currentIndex, start, end);
            currentIndex++;
            return partition;
        }
    }
}
