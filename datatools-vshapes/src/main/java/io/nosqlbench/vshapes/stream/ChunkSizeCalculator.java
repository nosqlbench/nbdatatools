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

/// Calculates chunk sizes for memory-bounded vector loading.
///
/// ## Purpose
///
/// When processing large vector datasets that exceed available heap memory,
/// data must be loaded in chunks. This calculator determines the optimal
/// chunk size based on available heap memory, vector dimensionality, and
/// a configurable memory budget fraction.
///
/// ## Memory Layout for Transposed Data
///
/// Transposed chunk memory footprint:
/// ```
/// chunk_bytes = dimensions × vectors_per_chunk × sizeof(float)
///             = dimensions × vectors_per_chunk × 4 bytes
/// ```
///
/// ## Budget Calculation
///
/// Given:
/// - `available_heap`: Runtime.maxMemory() - Runtime.totalMemory() + Runtime.freeMemory()
/// - `budget_fraction`: Configurable fraction of available heap (default: 0.6)
/// - `dimensions`: Number of dimensions per vector
/// - `overhead_factor`: Safety margin for GC, object headers, etc. (default: 1.2)
///
/// The maximum vectors per chunk:
/// ```
/// max_vectors = (available_heap × budget_fraction) / (dimensions × 4 × overhead_factor)
/// ```
///
/// ## Usage
///
/// ```java
/// // Auto-calculate based on current heap
/// ChunkSizeCalculator calc = new ChunkSizeCalculator(768);
/// int chunkSize = calc.calculate();
///
/// // With explicit memory budget
/// long budget = 4L * 1024 * 1024 * 1024; // 4GB
/// int chunkSize = calc.calculateForBudget(budget);
///
/// // Estimate memory for a given chunk size
/// long memoryNeeded = calc.estimateMemoryBytes(100_000);
/// ```
///
/// @see TransposedChunkDataSource
public final class ChunkSizeCalculator {

    /// Default fraction of available heap to use for chunk data (60%).
    public static final double DEFAULT_BUDGET_FRACTION = 0.6;

    /// Default overhead factor for GC pressure, object headers, etc.
    public static final double DEFAULT_OVERHEAD_FACTOR = 1.2;

    /// Minimum chunk size to ensure progress even under memory pressure.
    public static final int MIN_CHUNK_SIZE = 1000;

    /// Maximum chunk size to prevent excessive single allocations
    /// and ensure reasonable progress granularity.
    /// 500K vectors provides ~20 progress updates for 10M vector files.
    public static final int MAX_CHUNK_SIZE = 500_000;

    private final int dimensions;
    private final double budgetFraction;
    private final double overheadFactor;

    /// Creates a calculator with default settings.
    ///
    /// Uses 60% of available heap with 1.2x overhead factor.
    ///
    /// @param dimensions number of dimensions per vector
    /// @throws IllegalArgumentException if dimensions is not positive
    public ChunkSizeCalculator(int dimensions) {
        this(dimensions, DEFAULT_BUDGET_FRACTION, DEFAULT_OVERHEAD_FACTOR);
    }

    /// Creates a calculator with custom settings.
    ///
    /// @param dimensions number of dimensions per vector
    /// @param budgetFraction fraction of available heap to use (0.0 to 1.0)
    /// @param overheadFactor multiplier for safety margin (typically 1.1 to 1.5)
    /// @throws IllegalArgumentException if any parameter is invalid
    public ChunkSizeCalculator(int dimensions, double budgetFraction, double overheadFactor) {
        if (dimensions <= 0) {
            throw new IllegalArgumentException("dimensions must be positive: " + dimensions);
        }
        if (budgetFraction <= 0 || budgetFraction > 1.0) {
            throw new IllegalArgumentException("budgetFraction must be in (0, 1]: " + budgetFraction);
        }
        if (overheadFactor < 1.0) {
            throw new IllegalArgumentException("overheadFactor must be >= 1.0: " + overheadFactor);
        }
        this.dimensions = dimensions;
        this.budgetFraction = budgetFraction;
        this.overheadFactor = overheadFactor;
    }

    /// Calculates optimal chunk size based on current heap state.
    ///
    /// Queries the JVM runtime for available memory and calculates
    /// the maximum number of vectors that can fit within the budget.
    ///
    /// @return optimal number of vectors per chunk, clamped to [MIN_CHUNK_SIZE, MAX_CHUNK_SIZE]
    public int calculate() {
        Runtime rt = Runtime.getRuntime();
        long maxMemory = rt.maxMemory();
        long totalMemory = rt.totalMemory();
        long freeMemory = rt.freeMemory();
        long availableHeap = maxMemory - totalMemory + freeMemory;

        return calculateForBudget((long) (availableHeap * budgetFraction));
    }

    /// Calculates chunk size for a specific memory budget.
    ///
    /// @param memoryBudgetBytes explicit memory budget in bytes
    /// @return optimal number of vectors per chunk, clamped to [MIN_CHUNK_SIZE, MAX_CHUNK_SIZE]
    public int calculateForBudget(long memoryBudgetBytes) {
        if (memoryBudgetBytes <= 0) {
            return MIN_CHUNK_SIZE;
        }

        long bytesPerVector = (long) (dimensions * Float.BYTES * overheadFactor);
        if (bytesPerVector <= 0) {
            return MAX_CHUNK_SIZE;
        }

        long optimalChunkSize = memoryBudgetBytes / bytesPerVector;

        // Clamp to valid range
        if (optimalChunkSize > MAX_CHUNK_SIZE) {
            return MAX_CHUNK_SIZE;
        }
        if (optimalChunkSize < MIN_CHUNK_SIZE) {
            return MIN_CHUNK_SIZE;
        }
        return (int) optimalChunkSize;
    }

    /// Estimates memory required for a given chunk size.
    ///
    /// @param vectorCount number of vectors in chunk
    /// @return estimated bytes including overhead
    public long estimateMemoryBytes(int vectorCount) {
        return (long) (vectorCount * dimensions * Float.BYTES * overheadFactor);
    }

    /// Returns the raw bytes per vector without overhead.
    ///
    /// @return bytes per vector (dimensions × 4)
    public long bytesPerVector() {
        return (long) dimensions * Float.BYTES;
    }

    /// Returns the dimensions this calculator was configured for.
    ///
    /// @return number of dimensions per vector
    public int getDimensions() {
        return dimensions;
    }

    /// Returns the budget fraction.
    ///
    /// @return fraction of available heap to use
    public double getBudgetFraction() {
        return budgetFraction;
    }

    /// Returns the overhead factor.
    ///
    /// @return safety margin multiplier
    public double getOverheadFactor() {
        return overheadFactor;
    }

    /// Parses a memory specification string (e.g., "4g", "512m", "0.6").
    ///
    /// Supports:
    /// - Absolute sizes with suffix: "4g", "4G", "512m", "512M", "1024k", "1024K"
    /// - Fractions of available heap: "0.6", "0.75"
    ///
    /// @param spec the memory specification string
    /// @return memory budget in bytes
    /// @throws IllegalArgumentException if the spec cannot be parsed
    public static long parseMemorySpec(String spec) {
        if (spec == null || spec.isBlank()) {
            throw new IllegalArgumentException("Memory spec cannot be null or blank");
        }

        String trimmed = spec.trim().toLowerCase();

        // Check for suffix-based sizes
        if (trimmed.endsWith("g")) {
            return parseNumber(trimmed.substring(0, trimmed.length() - 1)) * 1024L * 1024L * 1024L;
        }
        if (trimmed.endsWith("m")) {
            return parseNumber(trimmed.substring(0, trimmed.length() - 1)) * 1024L * 1024L;
        }
        if (trimmed.endsWith("k")) {
            return parseNumber(trimmed.substring(0, trimmed.length() - 1)) * 1024L;
        }

        // Try parsing as a fraction
        try {
            double fraction = Double.parseDouble(trimmed);
            if (fraction > 0 && fraction <= 1.0) {
                // Fraction of available heap
                Runtime rt = Runtime.getRuntime();
                long availableHeap = rt.maxMemory() - rt.totalMemory() + rt.freeMemory();
                return (long) (availableHeap * fraction);
            } else if (fraction > 1.0) {
                // Treat as raw bytes
                return (long) fraction;
            } else {
                throw new IllegalArgumentException("Fraction must be positive: " + spec);
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Cannot parse memory spec: " + spec, e);
        }
    }

    private static long parseNumber(String s) {
        try {
            return Long.parseLong(s.trim());
        } catch (NumberFormatException e) {
            // Try as double for fractional values like "1.5g"
            return (long) Double.parseDouble(s.trim());
        }
    }

    @Override
    public String toString() {
        return String.format("ChunkSizeCalculator[dims=%d, budget=%.0f%%, overhead=%.1fx]",
            dimensions, budgetFraction * 100, overheadFactor);
    }
}
