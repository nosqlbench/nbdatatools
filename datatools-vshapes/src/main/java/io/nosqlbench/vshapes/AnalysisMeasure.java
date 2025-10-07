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

package io.nosqlbench.vshapes;

import java.nio.file.Path;
import java.util.Map;

/// # AnalysisMeasure Interface
///
/// Base interface for vector space analysis measures with caching support.
///
/// ## Purpose
/// Defines the contract for all vector space analysis measures including:
/// - **Computation**: Core analysis algorithm implementation
/// - **Dependencies**: Dependency management between measures  
/// - **Caching**: Persistent caching of expensive computations
/// - **Identification**: Unique mnemonics for measure registration
///
/// ## Lifecycle
/// 1. **Check Dependencies**: Ensure required measures are computed first
/// 2. **Check Cache**: Load existing results if available and valid
/// 3. **Compute**: Execute analysis algorithm if needed
/// 4. **Save Cache**: Persist results for future use
///
/// ## Implementation Guide
/// ```java
/// public class MyMeasure extends AbstractAnalysisMeasure<MyResult> {
///     @Override
///     public String getMnemonic() { return "MY"; }
///     
///     @Override
///     public String[] getDependencies() { return new String[0]; }
///     
///     @Override
///     protected MyResult computeImpl(VectorSpace vectorSpace, Path cacheDir, 
///                                  Map<String, Object> dependencies) {
///         // Your analysis logic here
///         return new MyResult(...);
///     }
/// }
/// ```
///
/// @param <T> the type of result produced by this measure
public interface AnalysisMeasure<T> {

    /// Gets the mnemonic identifier for this measure.
    /// Must be unique across all measures in the system.
    /// 
    /// @return a short, unique identifier (e.g., "LID", "Margin", "Hubness")
    String getMnemonic();

    /// Gets the dependencies required for this measure.
    /// Dependencies are computed before this measure and their results
    /// are passed to the compute method.
    /// 
    /// @return array of measure mnemonics this depends on (empty array if none)
    String[] getDependencies();

    /// Computes this measure for the given vector space.
    /// Implementations should check for cached results before computing.
    /// The framework handles dependency resolution automatically.
    ///
    /// @param vectorSpace the vector space to analyze
    /// @param cacheDir directory for storing computational artifacts
    /// @param dependencyResults results from dependency measures (keyed by mnemonic)
    /// @return the computed measure result
    T compute(VectorSpace vectorSpace, Path cacheDir, Map<String, Object> dependencyResults);

    /// Gets the cache filename for this measure.
    /// Default implementation uses: `mnemonic_vectorspaceId.json`
    /// 
    /// @param vectorSpace the vector space being analyzed
    /// @return the cache filename (including extension)
    default String getCacheFilename(VectorSpace vectorSpace) {
        return getMnemonic().toLowerCase() + "_" + vectorSpace.getId() + ".json";
    }

    /// Checks if cached results exist and are valid for the given vector space.
    /// 
    /// @param cacheDir the cache directory
    /// @param vectorSpace the vector space
    /// @return true if valid cache exists
    boolean hasCachedResult(Path cacheDir, VectorSpace vectorSpace);

    /// Loads cached results if available.
    /// 
    /// @param cacheDir the cache directory  
    /// @param vectorSpace the vector space
    /// @return the cached result, or null if not available
    T loadCachedResult(Path cacheDir, VectorSpace vectorSpace);

    /// Saves computed results to cache for future use.
    /// 
    /// @param result the result to cache
    /// @param cacheDir the cache directory
    /// @param vectorSpace the vector space
    void saveCachedResult(T result, Path cacheDir, VectorSpace vectorSpace);
}