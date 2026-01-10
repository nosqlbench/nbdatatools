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
import java.util.Iterator;
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
 *   <li>Converts chunks to columnar format if needed</li>
 *   <li>Distributes each chunk to all registered analyzers concurrently</li>
 *   <li>Collects results and errors from all analyzers</li>
 *   <li>Provides progress reporting during processing</li>
 * </ul>
 *
 * <h2>Columnar Data Format</h2>
 *
 * <p>All analyzers receive data in <strong>columnar (column-major) format</strong>:
 * <pre>{@code
 * chunk[dimensionIndex][vectorIndex]
 * }</pre>
 *
 * <p>This layout is optimal for per-dimension analysis:
 * <ul>
 *   <li>Each {@code chunk[d]} is a contiguous array of all values for dimension {@code d}</li>
 *   <li>Sequential iteration is cache-friendly</li>
 *   <li>SIMD operations can be applied to contiguous arrays</li>
 * </ul>
 *
 * <p>The harness automatically converts row-major data sources to columnar format.
 * For best performance, use {@link TransposedChunkDataSource} which provides
 * pre-transposed columnar chunks.
 *
 * <h2>Concurrency Model</h2>
 *
 * <p>The harness uses a fork-join approach:
 * <ol>
 *   <li>Read one chunk from the data source (sequential)</li>
 *   <li>Convert to columnar format if needed</li>
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
 * harness.register(new DimensionStatisticsAnalyzer());
 *
 * // Run analysis with columnar data source for best performance
 * TransposedChunkDataSource source = TransposedChunkDataSource.builder()
 *     .file(path)
 *     .build();
 * AnalysisResults results = harness.run(source, source.getOptimalChunkSize());
 *
 * // Get results
 * VectorSpaceModel model = results.getResult("model-extractor", VectorSpaceModel.class);
 * }</pre>
 *
 * @see StreamingAnalyzer
 * @see TransposedChunkDataSource
 * @see DataSource
 * @see AnalysisResults
 */
public final class AnalyzerHarness {

    private final List<StreamingAnalyzer<?>> analyzers = new CopyOnWriteArrayList<>();
    private final ExecutorService executor;
    private boolean failFast = true;
    private boolean ownsExecutor = true;
    private final AtomicBoolean stopRequested = new AtomicBoolean(false);

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
     * <p>All chunks are delivered to analyzers in columnar format {@code [dims][vectors]}.
     * Row-major data sources are automatically transposed.
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

        // Detect if source provides columnar (transposed) chunks
        boolean sourceIsColumnar = isColumnarDataSource(source);

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
        int chunkNumber = 0;
        int estimatedTotalChunks = (int) Math.ceil((double) totalVectors / chunkSize);

        // Use explicit iterator to report progress BEFORE each chunk load
        Iterator<float[][]> chunkIterator = source.chunks(chunkSize).iterator();
        while (chunkIterator.hasNext()) {
            if (aborted.get() || stopRequested.get()) break;

            chunkNumber++;

            // Report loading phase BEFORE chunk is loaded (I/O happens in next())
            if (progressCallback != null) {
                double progress = (double) processedVectors / totalVectors;
                progressCallback.onProgress(Phase.LOADING, progress, processedVectors,
                    totalVectors, chunkNumber, estimatedTotalChunks);
            }

            // Load the chunk (this is where I/O happens)
            float[][] chunk = chunkIterator.next();

            // Ensure chunk is in columnar format [dims][vectors]
            final float[][] columnarChunk = sourceIsColumnar ? chunk : toColumnar(chunk);
            final long startIndex = processedVectors;
            List<Future<?>> futures = new ArrayList<>();

            // Report processing phase (chunk is loaded, about to process)
            if (progressCallback != null) {
                double progress = (double) processedVectors / totalVectors;
                progressCallback.onProgress(Phase.PROCESSING, progress, processedVectors,
                    totalVectors, chunkNumber, estimatedTotalChunks);
            }

            // Submit chunk to all non-failed analyzers
            for (StreamingAnalyzer<?> analyzer : analyzers) {
                String type = analyzer.getAnalyzerType();
                if (errors.containsKey(type)) continue;

                futures.add(executor.submit(() -> {
                    try {
                        analyzer.accept(columnarChunk, startIndex);
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

            // For columnar chunks, vector count is chunk[0].length
            // For row-major chunks (before transpose), vector count is chunk.length
            processedVectors += sourceIsColumnar ? chunk[0].length : chunk.length;

            // Check if any analyzer requests early stopping (e.g., convergence reached)
            if (checkEarlyStopRequested()) {
                break;
            }

            // Report progress after processing
            if (progressCallback != null) {
                double progress = (double) processedVectors / totalVectors;
                progressCallback.onProgress(progress, processedVectors, totalVectors);
            }

            // Check for early termination immediately after callback
            // (callback may have called requestStop())
            if (stopRequested.get()) {
                break;
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

    /**
     * Detects if a data source provides columnar (column-major) chunks.
     */
    private boolean isColumnarDataSource(DataSource source) {
        if (source instanceof TransposedChunkDataSource) {
            return true;
        }
        // Check shape for columnar flag
        return source.getShape().isColumnar();
    }

    /**
     * Converts a row-major chunk to columnar format.
     *
     * @param rowMajor data in format {@code [vectors][dims]}
     * @return data in format {@code [dims][vectors]}
     */
    private float[][] toColumnar(float[][] rowMajor) {
        if (rowMajor == null || rowMajor.length == 0) {
            return new float[0][0];
        }
        int vectors = rowMajor.length;
        int dims = rowMajor[0].length;
        float[][] columnar = new float[dims][vectors];
        for (int v = 0; v < vectors; v++) {
            for (int d = 0; d < dims; d++) {
                columnar[d][v] = rowMajor[v][d];
            }
        }
        return columnar;
    }

    private AnalysisResults buildResults(Map<String, Throwable> errors, long startTime) {
        long processingTime = System.currentTimeMillis() - startTime;
        return new AnalysisResults(Map.of(), errors, processingTime);
    }

    /**
     * Checks if any registered analyzer requests early stopping.
     *
     * <p>This is called after each chunk is processed to allow analyzers
     * to signal that they have gathered sufficient data (e.g., convergence reached).
     *
     * <p>Currently checks for {@link StreamingModelExtractor} instances
     * that implement the {@code shouldStopEarly()} method.
     *
     * @return true if any analyzer requests early stopping
     */
    private boolean checkEarlyStopRequested() {
        for (StreamingAnalyzer<?> analyzer : analyzers) {
            if (analyzer instanceof StreamingModelExtractor extractor) {
                // Call convergence check and then see if early stop is requested
                extractor.checkConvergence();
                if (extractor.shouldStopEarly()) {
                    return true;
                }
            }
        }
        return false;
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
     * Requests early termination of the current analysis.
     *
     * <p>This method is thread-safe and can be called from any thread, including
     * progress callbacks. The harness will complete the current chunk before stopping.
     *
     * @return true if stop was newly requested, false if already requested
     */
    public boolean requestStop() {
        return stopRequested.compareAndSet(false, true);
    }

    /**
     * Checks if early termination has been requested.
     *
     * @return true if stop has been requested
     */
    public boolean isStopRequested() {
        return stopRequested.get();
    }

    /**
     * Resets the stop request flag.
     *
     * <p>Call this before reusing the harness for another analysis run.
     */
    public void resetStopRequest() {
        stopRequested.set(false);
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

        /**
         * Called to report progress with phase information.
         *
         * <p>Default implementation delegates to the simpler
         * {@link #onProgress(double, long, long)} method.
         *
         * @param phase the current processing phase
         * @param progress fraction complete (0.0 to 1.0)
         * @param processedVectors number of vectors processed so far
         * @param totalVectors total number of vectors
         * @param chunkNumber current chunk number (1-based)
         * @param totalChunks estimated total number of chunks
         */
        default void onProgress(Phase phase, double progress, long processedVectors,
                                long totalVectors, int chunkNumber, int totalChunks) {
            onProgress(progress, processedVectors, totalVectors);
        }
    }

    /**
     * Processing phases reported by the progress callback.
     */
    public enum Phase {
        /**
         * Loading data from source (I/O bound).
         */
        LOADING,

        /**
         * Processing loaded data through analyzers (CPU bound).
         */
        PROCESSING,

        /**
         * Finalizing analysis and building results.
         */
        COMPLETING
    }
}
