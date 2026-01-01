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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Orchestrates multiple streaming analyzers over a dataset.
 *
 * <h2>Purpose</h2>
 *
 * <p>The harness coordinates multiple analyzers processing the same dataset:
 * <ul>
 *   <li>Reads data from a {@link DataSource} in chunks</li>
 *   <li>Distributes each chunk to all registered analyzers concurrently</li>
 *   <li>Collects results and errors from all analyzers</li>
 *   <li>Provides progress reporting during processing</li>
 * </ul>
 *
 * <h2>Concurrency Model</h2>
 *
 * <p>The harness uses a fork-join approach:
 * <ol>
 *   <li>Read one chunk from the data source (sequential)</li>
 *   <li>Submit the chunk to all analyzers in parallel</li>
 *   <li>Wait for all analyzers to complete the chunk</li>
 *   <li>Repeat until all chunks processed</li>
 * </ol>
 *
 * <p>Analyzers must be thread-safe as they receive concurrent calls to {@code accept()}.
 *
 * <h2>Error Handling</h2>
 *
 * <p>By default, the harness uses fail-fast behavior: if any analyzer throws an exception,
 * all processing stops immediately. Use {@link #failFast(boolean)} to allow other
 * analyzers to continue when one fails.
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * // Create harness
 * AnalyzerHarness harness = new AnalyzerHarness()
 *     .failFast(true);  // Default
 *
 * // Register analyzers
 * harness.register(new StreamingModelExtractor());
 * harness.register(new StreamingStatsAnalyzer());
 *
 * // Run analysis
 * DataSource source = new FloatArrayDataSource(data);
 * AnalysisResults results = harness.run(source, 1000);
 *
 * // Get results
 * VectorSpaceModel model = results.getResult("model-extractor", VectorSpaceModel.class);
 * }</pre>
 *
 * @see StreamingAnalyzer
 * @see DataSource
 * @see AnalysisResults
 */
public final class AnalyzerHarness {

    private final List<StreamingAnalyzer<?>> analyzers = new CopyOnWriteArrayList<>();
    private final ExecutorService executor;
    private boolean failFast = true;
    private boolean ownsExecutor = true;

    /**
     * Creates a harness with a default work-stealing thread pool.
     */
    public AnalyzerHarness() {
        this.executor = Executors.newWorkStealingPool();
        this.ownsExecutor = true;
    }

    /**
     * Creates a harness with a custom executor service.
     *
     * @param executor the executor to use for parallel processing
     */
    public AnalyzerHarness(ExecutorService executor) {
        this.executor = Objects.requireNonNull(executor, "executor cannot be null");
        this.ownsExecutor = false;
    }

    /**
     * Sets whether to abort all processing when any analyzer fails.
     *
     * @param failFast true to abort on first error (default), false to continue others
     * @return this harness for chaining
     */
    public AnalyzerHarness failFast(boolean failFast) {
        this.failFast = failFast;
        return this;
    }

    /**
     * Registers an analyzer to run during analysis.
     *
     * @param analyzer the analyzer to register
     * @param <M> the model type produced by the analyzer
     * @return this harness for chaining
     */
    public <M> AnalyzerHarness register(StreamingAnalyzer<M> analyzer) {
        Objects.requireNonNull(analyzer, "analyzer cannot be null");
        analyzers.add(analyzer);
        return this;
    }

    /**
     * Registers an analyzer by name using SPI discovery.
     *
     * <p>Looks up the analyzer using {@link StreamingAnalyzerIO} and registers it if found.
     *
     * @param analyzerName the name of the analyzer to register
     * @return this harness for chaining
     * @throws IllegalArgumentException if no analyzer with the given name is found
     */
    public AnalyzerHarness register(String analyzerName) {
        Optional<StreamingAnalyzer<?>> analyzer = StreamingAnalyzerIO.get(analyzerName);
        if (analyzer.isEmpty()) {
            throw new IllegalArgumentException(
                "No analyzer found with name: " + analyzerName +
                ". Available: " + StreamingAnalyzerIO.getAvailableNames());
        }
        analyzers.add(analyzer.get());
        return this;
    }

    /**
     * Registers an analyzer by name if available.
     *
     * <p>Looks up the analyzer using {@link StreamingAnalyzerIO} and registers it if found.
     * Does nothing if the analyzer is not available.
     *
     * @param analyzerName the name of the analyzer to register
     * @return this harness for chaining
     */
    public AnalyzerHarness registerIfAvailable(String analyzerName) {
        StreamingAnalyzerIO.get(analyzerName).ifPresent(analyzers::add);
        return this;
    }

    /**
     * Registers multiple analyzers by name using SPI discovery.
     *
     * @param analyzerNames the names of the analyzers to register
     * @return this harness for chaining
     * @throws IllegalArgumentException if any analyzer is not found
     */
    public AnalyzerHarness registerAll(String... analyzerNames) {
        for (String name : analyzerNames) {
            register(name);
        }
        return this;
    }

    /**
     * Registers all available analyzers from SPI.
     *
     * @return this harness for chaining
     */
    public AnalyzerHarness registerAllAvailable() {
        for (StreamingAnalyzer<?> analyzer : StreamingAnalyzerIO.getAll()) {
            analyzers.add(analyzer);
        }
        return this;
    }

    /**
     * Runs all registered analyzers over the data source.
     *
     * @param source the data source to process
     * @param chunkSize the number of vectors per chunk
     * @return the results from all analyzers
     */
    public AnalysisResults run(DataSource source, int chunkSize) {
        return run(source, chunkSize, null);
    }

    /**
     * Runs all registered analyzers with progress reporting.
     *
     * @param source the data source to process
     * @param chunkSize the number of vectors per chunk
     * @param progressCallback callback for progress updates (nullable)
     * @return the results from all analyzers
     */
    public AnalysisResults run(DataSource source, int chunkSize, ProgressCallback progressCallback) {
        Objects.requireNonNull(source, "source cannot be null");
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize must be positive, got: " + chunkSize);
        }
        if (analyzers.isEmpty()) {
            throw new IllegalStateException("No analyzers registered");
        }

        long startTime = System.currentTimeMillis();
        DataspaceShape shape = source.getShape();
        Map<String, Throwable> errors = new ConcurrentHashMap<>();
        AtomicBoolean aborted = new AtomicBoolean(false);

        // Initialize all analyzers
        for (StreamingAnalyzer<?> analyzer : analyzers) {
            try {
                analyzer.initialize(shape);
            } catch (Exception e) {
                errors.put(analyzer.getAnalyzerType(), e);
                if (failFast) {
                    return buildResults(errors, startTime);
                }
            }
        }

        // Stream chunks to all analyzers
        long totalVectors = shape.cardinality();
        long processedVectors = 0;

        for (float[][] chunk : source.chunks(chunkSize)) {
            if (aborted.get()) break;

            final long startIndex = processedVectors;
            List<Future<?>> futures = new ArrayList<>();

            // Submit chunk to all non-failed analyzers
            for (StreamingAnalyzer<?> analyzer : analyzers) {
                String type = analyzer.getAnalyzerType();
                if (errors.containsKey(type)) continue;

                futures.add(executor.submit(() -> {
                    try {
                        analyzer.accept(chunk, startIndex);
                    } catch (Exception e) {
                        errors.put(type, e);
                        if (failFast) {
                            aborted.set(true);
                        }
                    }
                }));
            }

            // Wait for all to complete this chunk
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    // Errors already captured in the Runnable
                }
            }

            processedVectors += chunk.length;

            // Report progress
            if (progressCallback != null) {
                double progress = (double) processedVectors / totalVectors;
                progressCallback.onProgress(progress, processedVectors, totalVectors);
            }
        }

        // Complete all non-errored analyzers and collect results
        Map<String, Object> results = new HashMap<>();
        for (StreamingAnalyzer<?> analyzer : analyzers) {
            String type = analyzer.getAnalyzerType();
            if (!errors.containsKey(type)) {
                try {
                    Object result = analyzer.complete();
                    results.put(type, result);
                } catch (Exception e) {
                    errors.put(type, e);
                }
            }
        }

        long processingTime = System.currentTimeMillis() - startTime;
        return new AnalysisResults(results, errors, processingTime);
    }

    private AnalysisResults buildResults(Map<String, Throwable> errors, long startTime) {
        long processingTime = System.currentTimeMillis() - startTime;
        return new AnalysisResults(Map.of(), errors, processingTime);
    }

    /**
     * Returns the number of registered analyzers.
     *
     * @return count of registered analyzers
     */
    public int getAnalyzerCount() {
        return analyzers.size();
    }

    /**
     * Returns the registered analyzer types.
     *
     * @return list of analyzer type identifiers
     */
    public List<String> getAnalyzerTypes() {
        List<String> types = new ArrayList<>();
        for (StreamingAnalyzer<?> analyzer : analyzers) {
            types.add(analyzer.getAnalyzerType());
        }
        return types;
    }

    /**
     * Clears all registered analyzers.
     *
     * @return this harness for chaining
     */
    public AnalyzerHarness clear() {
        analyzers.clear();
        return this;
    }

    /**
     * Shuts down the executor if owned by this harness.
     *
     * <p>Call this when done using the harness to release thread pool resources.
     */
    public void shutdown() {
        if (ownsExecutor) {
            executor.shutdown();
        }
    }

    /**
     * Callback for progress reporting during analysis.
     */
    @FunctionalInterface
    public interface ProgressCallback {
        /**
         * Called to report progress.
         *
         * @param progress fraction complete (0.0 to 1.0)
         * @param processedVectors number of vectors processed so far
         * @param totalVectors total number of vectors
         */
        void onProgress(double progress, long processedVectors, long totalVectors);
    }
}
