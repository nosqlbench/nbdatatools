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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/// # AbstractAnalysisMeasure
///
/// Base implementation of AnalysisMeasure that provides JSON caching functionality.
/// 
/// ## Purpose
/// Provides default implementations of caching logic using JSON serialization.
/// Subclasses need only implement the core computation logic and result type handling.
///
/// ## Features
/// - **Automatic Caching**: JSON-based persistence of computation results
/// - **Cache Validation**: Checks for existing valid cache files
/// - **Template Method**: Separates caching concerns from computation logic
/// - **Type Safety**: Generic result type handling
///
/// ## Implementation Pattern
/// ```java
/// public class MyMeasure extends AbstractAnalysisMeasure<MyResult> {
///     @Override
///     protected MyResult computeImpl(VectorSpace space, Path cache, Map<String, Object> deps) {
///         // Your computation logic here
///         return new MyResult(...);
///     }
///     
///     @Override
///     protected Class<MyResult> getResultClass() {
///         return MyResult.class;
///     }
/// }
/// ```
///
/// @param <T> the result type for this measure
public abstract class AbstractAnalysisMeasure<T> implements AnalysisMeasure<T> {

    /// Creates a new AbstractAnalysisMeasure.
    /// Default constructor for subclasses.
    public AbstractAnalysisMeasure() {
        // Default constructor
    }

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public final T compute(VectorSpace vectorSpace, Path cacheDir, Map<String, Object> dependencyResults) {
        // Check for cached result first (only if cacheDir is provided)
        if (cacheDir != null && hasCachedResult(cacheDir, vectorSpace)) {
            T cachedResult = loadCachedResult(cacheDir, vectorSpace);
            if (cachedResult != null) {
                return cachedResult;
            }
        }

        // Compute fresh result
        T result = computeImpl(vectorSpace, cacheDir, dependencyResults);

        // Cache the result (only if cacheDir is provided)
        if (cacheDir != null) {
            saveCachedResult(result, cacheDir, vectorSpace);
        }

        return result;
    }

    /**
     * Performs the actual computation for this measure.
     * Subclasses implement this method with their specific logic.
     *
     * @param vectorSpace the vector space to analyze
     * @param cacheDir directory for storing computational artifacts
     * @param dependencyResults results from dependency measures
     * @return the computed result
     */
    protected abstract T computeImpl(VectorSpace vectorSpace, Path cacheDir, Map<String, Object> dependencyResults);

    /**
     * Gets the Java class representing the result type.
     * Used for JSON deserialization.
     * @return the result class
     */
    protected abstract Class<T> getResultClass();

    @Override
    public boolean hasCachedResult(Path cacheDir, VectorSpace vectorSpace) {
        Path cacheFile = cacheDir.resolve(getCacheFilename(vectorSpace));
        return Files.exists(cacheFile) && Files.isRegularFile(cacheFile);
    }

    @Override
    public T loadCachedResult(Path cacheDir, VectorSpace vectorSpace) {
        Path cacheFile = cacheDir.resolve(getCacheFilename(vectorSpace));
        try {
            return objectMapper.readValue(cacheFile.toFile(), getResultClass());
        } catch (IOException e) {
            // If we can't read the cache, treat it as non-existent
            return null;
        }
    }

    @Override
    public void saveCachedResult(T result, Path cacheDir, VectorSpace vectorSpace) {
        try {
            Files.createDirectories(cacheDir);
            Path cacheFile = cacheDir.resolve(getCacheFilename(vectorSpace));
            objectMapper.writeValue(cacheFile.toFile(), result);
        } catch (IOException e) {
            // Log error but don't fail - caching is optional
            System.err.println("Warning: Failed to cache result for " + getMnemonic() + ": " + e.getMessage());
        }
    }

    /// Gets the shared ObjectMapper instance for JSON serialization.
    /// 
    /// @return configured Jackson ObjectMapper for caching operations
    protected ObjectMapper getObjectMapper() {
        return objectMapper;
    }
}