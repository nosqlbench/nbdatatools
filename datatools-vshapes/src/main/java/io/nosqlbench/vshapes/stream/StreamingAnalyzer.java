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

/// Interface for streaming analyzers that process vectors in chunks and produce a model.
///
/// ## Purpose
///
/// Implementations of this interface analyze vector data streamed in chunks,
/// accumulating state across chunks, and finally producing a model of type `M`.
/// This enables efficient processing of large datasets without loading everything
/// into memory at once.
///
/// ## Lifecycle
///
/// ```
/// 1. initialize(shape)     - Called once with dataspace shape
/// 2. accept(chunk, index)  - Called multiple times with chunks of vectors
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
/// ## Usage with AnalyzerHarness
///
/// ```java
/// public class MyAnalyzer implements StreamingAnalyzer<MyModel> {
///     private final AtomicLong count = new AtomicLong();
///     private DataspaceShape shape;
///
///     @Override
///     public String getAnalyzerType() { return "my-analyzer"; }
///
///     @Override
///     public void initialize(DataspaceShape shape) {
///         this.shape = shape;
///     }
///
///     @Override
///     public void accept(float[][] chunk, long startIndex) {
///         count.addAndGet(chunk.length);
///         // Process vectors...
///     }
///
///     @Override
///     public MyModel complete() {
///         return new MyModel(count.get());
///     }
/// }
/// ```
///
/// @param <M> the type of model produced by this analyzer
/// @see AnalyzerHarness
/// @see DataspaceShape
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

    /// Processes a chunk of vectors.
    ///
    /// Called multiple times with successive chunks of the dataset.
    /// Each chunk is a 2D array where:
    /// - `chunk[i]` is the i-th vector in this chunk
    /// - `chunk[i][d]` is dimension d of vector i
    /// - `chunk.length` may vary between calls (last chunk may be smaller)
    ///
    /// **Thread Safety:** This method may be called concurrently from multiple
    /// threads. Implementations must use appropriate synchronization.
    ///
    /// @param chunk the chunk of vectors, shape [numVectors][dimensionality]
    /// @param startIndex the index of the first vector in this chunk within the full dataset
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
