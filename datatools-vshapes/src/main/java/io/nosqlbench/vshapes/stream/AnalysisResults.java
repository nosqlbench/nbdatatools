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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Container for results from multiple streaming analyzers.
 *
 * <h2>Purpose</h2>
 *
 * <p>Holds the models produced by all analyzers run through an {@link AnalyzerHarness},
 * along with any errors that occurred during processing.
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * AnalysisResults results = harness.run(source, 1000);
 *
 * // Get a specific result by type
 * VectorSpaceModel model = results.getResult("model-extractor", VectorSpaceModel.class);
 *
 * // Check for errors
 * if (results.hasErrors()) {
 *     results.getErrors().forEach((type, error) ->
 *         System.err.println(type + " failed: " + error.getMessage()));
 * }
 *
 * // Get all results
 * Map<String, Object> all = results.getAllResults();
 * }</pre>
 *
 * @see AnalyzerHarness
 */
public final class AnalysisResults {

    private final Map<String, Object> results;
    private final Map<String, Throwable> errors;
    private final long processingTimeMs;

    /**
     * Creates an AnalysisResults with results and errors.
     *
     * @param results map of analyzer type to result object
     * @param errors map of analyzer type to error (if any failed)
     * @param processingTimeMs total processing time in milliseconds
     */
    public AnalysisResults(Map<String, Object> results, Map<String, Throwable> errors, long processingTimeMs) {
        this.results = Collections.unmodifiableMap(new HashMap<>(results));
        this.errors = Collections.unmodifiableMap(new HashMap<>(errors));
        this.processingTimeMs = processingTimeMs;
    }

    /**
     * Creates an AnalysisResults with results and errors (no timing).
     */
    public AnalysisResults(Map<String, Object> results, Map<String, Throwable> errors) {
        this(results, errors, 0);
    }

    /**
     * Creates an AnalysisResults with only results (no errors).
     */
    public AnalysisResults(Map<String, Object> results) {
        this(results, Collections.emptyMap(), 0);
    }

    /**
     * Gets a result by analyzer type with type safety.
     *
     * @param analyzerType the analyzer type identifier
     * @param resultClass the expected result class
     * @param <M> the result type
     * @return the result, or null if not found or type mismatch
     */
    @SuppressWarnings("unchecked")
    public <M> M getResult(String analyzerType, Class<M> resultClass) {
        Object result = results.get(analyzerType);
        if (result != null && resultClass.isAssignableFrom(result.getClass())) {
            return (M) result;
        }
        return null;
    }

    /**
     * Gets a result by analyzer type without type checking.
     *
     * @param analyzerType the analyzer type identifier
     * @return the result, or null if not found
     */
    public Object getResult(String analyzerType) {
        return results.get(analyzerType);
    }

    /**
     * Checks if a result exists for the given analyzer type.
     *
     * @param analyzerType the analyzer type identifier
     * @return true if a result exists
     */
    public boolean hasResult(String analyzerType) {
        return results.containsKey(analyzerType);
    }

    /**
     * Returns all results as an unmodifiable map.
     *
     * @return map of analyzer type to result
     */
    public Map<String, Object> getAllResults() {
        return results;
    }

    /**
     * Returns the set of analyzer types that produced results.
     *
     * @return set of analyzer type identifiers
     */
    public Set<String> getSuccessfulAnalyzers() {
        return results.keySet();
    }

    /**
     * Checks if any analyzer failed with an error.
     *
     * @return true if there are errors
     */
    public boolean hasErrors() {
        return !errors.isEmpty();
    }

    /**
     * Gets the error for a specific analyzer.
     *
     * @param analyzerType the analyzer type identifier
     * @return the error, or null if no error occurred
     */
    public Throwable getError(String analyzerType) {
        return errors.get(analyzerType);
    }

    /**
     * Returns all errors as an unmodifiable map.
     *
     * @return map of analyzer type to error
     */
    public Map<String, Throwable> getErrors() {
        return errors;
    }

    /**
     * Returns the set of analyzer types that failed.
     *
     * @return set of failed analyzer type identifiers
     */
    public Set<String> getFailedAnalyzers() {
        return errors.keySet();
    }

    /**
     * Returns the total processing time.
     *
     * @return processing time in milliseconds
     */
    public long getProcessingTimeMs() {
        return processingTimeMs;
    }

    /**
     * Returns a summary of the results.
     *
     * @return human-readable summary
     */
    public String getSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append("AnalysisResults:\n");
        sb.append(String.format("  Processing time: %dms\n", processingTimeMs));
        sb.append(String.format("  Successful: %d, Failed: %d\n", results.size(), errors.size()));

        if (!results.isEmpty()) {
            sb.append("  Results:\n");
            for (Map.Entry<String, Object> entry : results.entrySet()) {
                sb.append(String.format("    - %s: %s\n",
                    entry.getKey(),
                    entry.getValue().getClass().getSimpleName()));
            }
        }

        if (!errors.isEmpty()) {
            sb.append("  Errors:\n");
            for (Map.Entry<String, Throwable> entry : errors.entrySet()) {
                sb.append(String.format("    - %s: %s\n",
                    entry.getKey(),
                    entry.getValue().getMessage()));
            }
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return String.format("AnalysisResults[results=%d, errors=%d, time=%dms]",
            results.size(), errors.size(), processingTimeMs);
    }
}
