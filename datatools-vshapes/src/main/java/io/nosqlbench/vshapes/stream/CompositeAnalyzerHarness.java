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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

/// Harness for running multiple analyzers in a single pass over data.
///
/// ## Purpose
///
/// When multiple analyses need to be performed on the same dataset, running
/// them sequentially requires reading the data multiple times. The CompositeAnalyzerHarness
/// allows multiple analyzers to process the same data stream concurrently, reading
/// the data only once.
///
/// ## Single-Pass Processing
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────┐
/// │                    SINGLE-PASS EXECUTION                        │
/// └─────────────────────────────────────────────────────────────────┘
///
///   DataSource
///       │
///       │  chunk[n]
///       ▼
///   ┌───────────────────────────────────────────────────────────┐
///   │                     CompositeHarness                       │
///   │                                                            │
///   │   ┌───────────────────────────────────────────────────┐   │
///   │   │              chunk[n] broadcast                    │   │
///   │   └───────────────────────────────────────────────────┘   │
///   │       │                   │                   │            │
///   │       ▼                   ▼                   ▼            │
///   │  ┌─────────┐        ┌─────────┐        ┌─────────┐        │
///   │  │Analyzer1│        │Analyzer2│        │Analyzer3│        │
///   │  │ Thread  │        │ Thread  │        │ Thread  │        │
///   │  └─────────┘        └─────────┘        └─────────┘        │
///   │       │                   │                   │            │
///   │       └───────────────────┼───────────────────┘            │
///   │                           ▼                                │
///   │                   wait for chunk                           │
///   │                                                            │
///   └───────────────────────────────────────────────────────────┘
///                           │
///                           ▼
///                   AnalysisResults
///                   (from all analyzers)
/// ```
///
/// ## Usage
///
/// ```java
/// AnalysisResults results = CompositeAnalyzerHarness.builder()
///     .add(new DimensionStatisticsAnalyzer())
///     .add(new DimensionDistributionAnalyzer())
///     .add(new StreamingModelExtractor())
///     .build()
///     .run(dataSource, 10000);
///
/// // Results contain outputs from all analyzers
/// DimensionStatistics stats = results.getResult("dimension-statistics", DimensionStatistics[].class);
/// VectorSpaceModel model = results.getResult("model-extractor", VectorSpaceModel.class);
/// ```
///
/// ## Equivalence Guarantee
///
/// Running analyzers through the composite harness produces the same results as
/// running each analyzer independently (modulo thread-timing differences in
/// stateful analyzers).
///
/// @see AnalyzerHarness
/// @see StreamingAnalyzer
/// @see AnalysisResults
public final class CompositeAnalyzerHarness {

    private final AnalyzerHarness harness;
    private final List<String> analyzerTypes;

    private CompositeAnalyzerHarness(AnalyzerHarness harness, List<String> analyzerTypes) {
        this.harness = harness;
        this.analyzerTypes = Collections.unmodifiableList(new ArrayList<>(analyzerTypes));
    }

    /// Creates a new builder for constructing a CompositeAnalyzerHarness.
    ///
    /// @return a new builder
    public static Builder builder() {
        return new Builder();
    }

    /// Creates a composite harness from the given analyzers.
    ///
    /// @param analyzers the analyzers to run together
    /// @return a new composite harness
    @SafeVarargs
    public static CompositeAnalyzerHarness of(StreamingAnalyzer<?>... analyzers) {
        Builder builder = builder();
        for (StreamingAnalyzer<?> analyzer : analyzers) {
            builder.add(analyzer);
        }
        return builder.build();
    }

    /// Runs all analyzers over the data source in a single pass.
    ///
    /// @param source the data source to process
    /// @param chunkSize the number of vectors per processing chunk
    /// @return aggregated results from all analyzers
    public AnalysisResults run(DataSource source, int chunkSize) {
        return harness.run(source, chunkSize);
    }

    /// Runs all analyzers with progress reporting.
    ///
    /// @param source the data source to process
    /// @param chunkSize the number of vectors per processing chunk
    /// @param progressCallback callback for progress updates
    /// @return aggregated results from all analyzers
    public AnalysisResults run(DataSource source, int chunkSize,
                                AnalyzerHarness.ProgressCallback progressCallback) {
        return harness.run(source, chunkSize, progressCallback);
    }

    /// Returns the list of analyzer types in this composite.
    ///
    /// @return unmodifiable list of analyzer type identifiers
    public List<String> getAnalyzerTypes() {
        return analyzerTypes;
    }

    /// Returns the number of analyzers in this composite.
    ///
    /// @return count of analyzers
    public int size() {
        return analyzerTypes.size();
    }

    /// Shuts down the underlying executor when done.
    ///
    /// Call this to release thread pool resources.
    public void shutdown() {
        harness.shutdown();
    }

    /// Builder for constructing CompositeAnalyzerHarness instances.
    public static final class Builder {
        private final List<StreamingAnalyzer<?>> analyzers = new ArrayList<>();
        private ExecutorService executor;
        private boolean failFast = true;

        private Builder() {}

        /// Adds an analyzer to the composite.
        ///
        /// @param analyzer the analyzer to add
        /// @param <M> the model type produced by the analyzer
        /// @return this builder
        public <M> Builder add(StreamingAnalyzer<M> analyzer) {
            analyzers.add(Objects.requireNonNull(analyzer, "analyzer cannot be null"));
            return this;
        }

        /// Adds multiple analyzers to the composite.
        ///
        /// @param analyzers the analyzers to add
        /// @return this builder
        public Builder addAll(Iterable<? extends StreamingAnalyzer<?>> analyzers) {
            for (StreamingAnalyzer<?> analyzer : analyzers) {
                add(analyzer);
            }
            return this;
        }

        /// Adds an analyzer by name using SPI discovery.
        ///
        /// @param analyzerName the name of the analyzer to add
        /// @return this builder
        /// @throws IllegalArgumentException if no analyzer with the given name is found
        public Builder add(String analyzerName) {
            StreamingAnalyzer<?> analyzer = StreamingAnalyzerIO.get(analyzerName)
                .orElseThrow(() -> new IllegalArgumentException(
                    "No analyzer found with name: " + analyzerName +
                    ". Available: " + StreamingAnalyzerIO.getAvailableNames()));
            return add(analyzer);
        }

        /// Adds an analyzer by name if available.
        ///
        /// @param analyzerName the name of the analyzer to add
        /// @return this builder
        public Builder addIfAvailable(String analyzerName) {
            StreamingAnalyzerIO.get(analyzerName).ifPresent(this::add);
            return this;
        }

        /// Sets a custom executor service for parallel processing.
        ///
        /// @param executor the executor to use
        /// @return this builder
        public Builder executor(ExecutorService executor) {
            this.executor = executor;
            return this;
        }

        /// Sets whether to abort all processing when any analyzer fails.
        ///
        /// @param failFast true to abort on first error (default), false to continue others
        /// @return this builder
        public Builder failFast(boolean failFast) {
            this.failFast = failFast;
            return this;
        }

        /// Builds the CompositeAnalyzerHarness.
        ///
        /// @return a new composite harness
        /// @throws IllegalStateException if no analyzers were added
        public CompositeAnalyzerHarness build() {
            if (analyzers.isEmpty()) {
                throw new IllegalStateException("At least one analyzer must be added");
            }

            AnalyzerHarness harness = (executor != null)
                ? new AnalyzerHarness(executor)
                : new AnalyzerHarness();

            harness.failFast(failFast);

            List<String> types = new ArrayList<>();
            for (StreamingAnalyzer<?> analyzer : analyzers) {
                harness.register(analyzer);
                types.add(analyzer.getAnalyzerType());
            }

            return new CompositeAnalyzerHarness(harness, types);
        }
    }
}
