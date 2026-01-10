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

/// Interface for streaming analyzers that process vectors in columnar chunks.
///
/// ## Purpose
///
/// Implementations of this interface analyze vector data streamed in columnar chunks,
/// accumulating state across chunks, and finally producing a model of type `M`.
/// This enables efficient processing of large datasets without loading everything
/// into memory at once.
///
/// ## Columnar Data Format
///
/// All chunks are provided in **column-major (columnar) format**:
/// ```
/// chunk[dimensionIndex][vectorIndex]
/// ```
///
/// This layout is optimal for per-dimension analysis because:
/// - Each `chunk[d]` is a contiguous array of all values for dimension `d`
/// - Sequential iteration through a dimension is cache-friendly
/// - SIMD operations can be applied to contiguous dimension arrays
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────┐
/// │                    COLUMNAR CHUNK LAYOUT                    │
/// └─────────────────────────────────────────────────────────────┘
///
///   chunk[0] = [v0.dim0, v1.dim0, v2.dim0, ..., vN.dim0]  ← dimension 0
///   chunk[1] = [v0.dim1, v1.dim1, v2.dim1, ..., vN.dim1]  ← dimension 1
///   chunk[2] = [v0.dim2, v1.dim2, v2.dim2, ..., vN.dim2]  ← dimension 2
///   ...
///   chunk[D] = [v0.dimD, v1.dimD, v2.dimD, ..., vN.dimD]  ← dimension D
///
///   where:
///     chunk.length = number of dimensions
///     chunk[d].length = number of vectors in this chunk
/// ```
///
/// ## Lifecycle
///
/// ```
/// 1. initialize(shape)     - Called once with dataspace shape
/// 2. accept(chunk, index)  - Called multiple times with columnar chunks
/// 3. complete()            - Called once to finalize and return the model
/// ```
///
/// ## Thread Safety
///
/// Implementations **must be thread-safe**. The `accept()` method may be
/// called concurrently from multiple threads when processing chunks in parallel.
/// Use appropriate synchronization (locks, atomic variables, concurrent data structures)
/// to protect internal state.
///
/// ## Accumulator Pattern
///
/// A typical implementation maintains per-dimension accumulators:
///
/// ```java
/// public class MyAnalyzer implements StreamingAnalyzer<MyModel> {
///     private StreamingDimensionAccumulator[] accumulators;
///     private ReentrantLock[] locks;
///
///     @Override
///     public String getAnalyzerType() { return "my-analyzer"; }
///
///     @Override
///     public void initialize(DataspaceShape shape) {
///         int dims = shape.dimensionality();
///         accumulators = new StreamingDimensionAccumulator[dims];
///         locks = new ReentrantLock[dims];
///         for (int d = 0; d < dims; d++) {
///             accumulators[d] = new StreamingDimensionAccumulator(d);
///             locks[d] = new ReentrantLock();
///         }
///     }
///
///     @Override
///     public void accept(float[][] chunk, long startIndex) {
///         // chunk[d] contains all values for dimension d
///         for (int d = 0; d < chunk.length; d++) {
///             locks[d].lock();
///             try {
///                 accumulators[d].addAll(chunk[d]);
///             } finally {
///                 locks[d].unlock();
///             }
///         }
///     }
///
///     @Override
///     public MyModel complete() {
///         // Build model from accumulated statistics
///         return new MyModel(accumulators);
///     }
/// }
/// ```
///
/// @param <M> the type of model produced by this analyzer
/// @see AnalyzerHarness
/// @see DataspaceShape
/// @see StreamingDimensionAccumulator
public interface StreamingAnalyzer<M> {

    /// Returns the unique identifier for this analyzer type.
    ///
    /// This identifier is used to:
    /// - Retrieve results from [AnalysisResults]
    /// - Report errors for specific analyzers
    /// - Log and monitor analyzer execution
    ///
    /// @return a unique, human-readable identifier (e.g., "model-extractor", "stats")
    String getAnalyzerType();

    /// Initializes the analyzer with the dataspace shape.
    ///
    /// Called exactly once before any chunks are delivered. Use this to:
    /// - Allocate per-dimension data structures
    /// - Read configuration from parameters
    /// - Initialize accumulators
    ///
    /// @param shape the shape of the dataspace (cardinality, dimensionality, parameters)
    void initialize(DataspaceShape shape);

    /// Processes a columnar chunk of vectors.
    ///
    /// Called multiple times with successive chunks of the dataset.
    /// Each chunk is a 2D array in **column-major format**:
    /// - `chunk[d]` is a contiguous array of all values for dimension `d`
    /// - `chunk[d][i]` is dimension `d` of vector `i` in this chunk
    /// - `chunk.length` equals the dimensionality
    /// - `chunk[0].length` equals the number of vectors in this chunk (may vary)
    ///
    /// **Thread Safety:** This method may be called concurrently from multiple
    /// threads. Implementations must use appropriate synchronization.
    ///
    /// @param chunk columnar data, shape `[dimensionality][vectorsInChunk]`
    /// @param startIndex the ordinal of the first vector in this chunk within the full dataset
    void accept(float[][] chunk, long startIndex);

    /// Finalizes processing and produces the model.
    ///
    /// Called exactly once after all chunks have been processed. Use this to:
    /// - Compute final statistics from accumulated data
    /// - Fit models to collected samples
    /// - Build and return the final result
    ///
    /// @return the model produced by this analyzer
    M complete();

    /// Returns the estimated memory footprint of this analyzer in bytes.
    ///
    /// This is a hint for the harness to schedule analyzers efficiently.
    /// Return 0 if unknown.
    ///
    /// @return estimated memory usage in bytes, or 0 if unknown
    default long estimatedMemoryBytes() {
        return 0;
    }

    /// Returns a human-readable description of this analyzer.
    ///
    /// Used for logging and reporting.
    ///
    /// @return description of what this analyzer does
    default String getDescription() {
        return getAnalyzerType();
    }
}
