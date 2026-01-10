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

package io.nosqlbench.vshapes.extract;

import io.nosqlbench.vshapes.model.ScalarModel;
import io.nosqlbench.vshapes.model.VectorSpaceModel;

import java.util.ArrayList;
import java.util.List;

/**
 * Model extractor with per-dimension convergence detection.
 *
 * <p>This extractor processes vector data incrementally and tracks convergence
 * for each dimension independently. Extraction can be stopped early when all
 * dimensions have converged, saving computation on large datasets.
 *
 * <h2>Convergence-Based Extraction</h2>
 *
 * <p>The extractor monitors statistical parameters (mean, variance, skewness, kurtosis)
 * for each dimension. When parameter changes fall below the convergence threshold
 * (relative to theoretical standard error), the dimension is considered converged.
 *
 * <h2>Early Stopping</h2>
 *
 * <p>When all dimensions have converged, the extractor can optionally stop early.
 * This is particularly useful for large datasets where convergence may be achieved
 * with only a fraction of the total data.
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * // Create extractor with high-precision convergence
 * ConvergentDatasetModelExtractor extractor = ConvergentDatasetModelExtractor.builder()
 *     .convergenceThreshold(0.05)  // 5% of standard error
 *     .checkpointInterval(1000)    // Check every 1000 samples
 *     .earlyStoppingEnabled(true)  // Stop when all dimensions converge
 *     .selector(BestFitSelector.boundedDataSelector())
 *     .build();
 *
 * // Extract model - may stop early if convergence is achieved
 * ExtractionResult result = extractor.extractWithStats(data);
 *
 * // Check convergence status
 * ConvergenceSummary summary = extractor.getConvergenceSummary();
 * System.out.println("Converged at: " + summary.averageConvergenceSamples());
 * }</pre>
 *
 * @see ConvergentDimensionEstimator
 */
public final class ConvergentDatasetModelExtractor implements ModelExtractor {

    private final double convergenceThreshold;
    private final int checkpointInterval;
    private final boolean earlyStoppingEnabled;
    private final BestFitSelector selector;
    private final long uniqueVectors;

    // Per-dimension estimators (created during extraction)
    private ConvergentDimensionEstimator[] estimators;
    private int dimensions;
    private long samplesProcessed;
    private boolean extractionComplete;
    private long extractionTimeMs;

    // Storage for per-dimension data (used for model fitting)
    private float[][] dimensionData;

    private ConvergentDatasetModelExtractor(Builder builder) {
        this.convergenceThreshold = builder.convergenceThreshold;
        this.checkpointInterval = builder.checkpointInterval;
        this.earlyStoppingEnabled = builder.earlyStoppingEnabled;
        this.selector = builder.selector;
        this.uniqueVectors = builder.uniqueVectors;
    }

    /**
     * Creates a builder for configuring the extractor.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a default convergent extractor with high-precision settings.
     *
     * @return a default convergent extractor
     */
    public static ConvergentDatasetModelExtractor withDefaults() {
        return builder().build();
    }

    @Override
    public VectorSpaceModel extractVectorModel(float[][] data) {
        return extractWithStats(data).model();
    }

    @Override
    public ExtractionResult extractFromTransposed(float[][] transposedData) {
        // Process transposed data directly without copying back to row-major
        if (transposedData == null || transposedData.length == 0) {
            throw new IllegalArgumentException("Data cannot be null or empty");
        }

        long startTime = System.currentTimeMillis();

        dimensions = transposedData.length;
        int vectorCount = transposedData[0].length;

        // Initialize per-dimension estimators
        estimators = new ConvergentDimensionEstimator[dimensions];
        for (int d = 0; d < dimensions; d++) {
            estimators[d] = new ConvergentDimensionEstimator(d, convergenceThreshold, checkpointInterval);
        }

        // Use transposed data directly as dimension storage (zero-copy)
        dimensionData = transposedData;

        // Process vectors by iterating through vector indices
        // Access pattern: for each vector v, process transposedData[d][v] for all d
        samplesProcessed = 0;
        extractionComplete = false;

        for (int v = 0; v < vectorCount; v++) {
            for (int d = 0; d < dimensions; d++) {
                estimators[d].accept(transposedData[d][v]);
            }
            samplesProcessed++;

            // Check for early stopping
            if (earlyStoppingEnabled && samplesProcessed % checkpointInterval == 0) {
                if (allDimensionsConverged()) {
                    break;
                }
            }
        }

        extractionComplete = true;

        // Build dimension statistics
        DimensionStatistics[] stats = new DimensionStatistics[dimensions];
        for (int d = 0; d < dimensions; d++) {
            stats[d] = estimators[d].toStatistics();
        }

        // Fit models to each dimension using the transposed data directly
        ScalarModel[] scalarModels = new ScalarModel[dimensions];
        ComponentModelFitter.FitResult[] fitResults = new ComponentModelFitter.FitResult[dimensions];

        for (int d = 0; d < dimensions; d++) {
            // Use only the processed samples for fitting
            float[] dimValues;
            if (samplesProcessed < vectorCount) {
                dimValues = new float[(int) samplesProcessed];
                System.arraycopy(transposedData[d], 0, dimValues, 0, (int) samplesProcessed);
            } else {
                dimValues = transposedData[d];
            }

            ComponentModelFitter.FitResult result = selector.selectBestResult(stats[d], dimValues);
            scalarModels[d] = result.model();
            fitResults[d] = result;
        }

        VectorSpaceModel model = new VectorSpaceModel(
            uniqueVectors > 0 ? uniqueVectors : samplesProcessed,
            scalarModels
        );

        extractionTimeMs = System.currentTimeMillis() - startTime;

        return new ExtractionResult(model, stats, fitResults, extractionTimeMs);
    }

    @Override
    public ExtractionResult extractWithStats(float[][] data) {
        long startTime = System.currentTimeMillis();

        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("Data cannot be null or empty");
        }

        int vectorCount = data.length;
        dimensions = data[0].length;

        // Initialize per-dimension estimators
        estimators = new ConvergentDimensionEstimator[dimensions];
        for (int d = 0; d < dimensions; d++) {
            estimators[d] = new ConvergentDimensionEstimator(d, convergenceThreshold, checkpointInterval);
        }

        // Initialize per-dimension data storage (for model fitting later)
        dimensionData = new float[dimensions][];
        for (int d = 0; d < dimensions; d++) {
            dimensionData[d] = new float[vectorCount];
        }

        // Process vectors
        samplesProcessed = 0;
        extractionComplete = false;

        for (int v = 0; v < vectorCount; v++) {
            float[] vector = data[v];
            for (int d = 0; d < dimensions; d++) {
                estimators[d].accept(vector[d]);
                dimensionData[d][v] = vector[d];
            }
            samplesProcessed++;

            // Check for early stopping
            if (earlyStoppingEnabled && samplesProcessed % checkpointInterval == 0) {
                if (allDimensionsConverged()) {
                    break;
                }
            }
        }

        extractionComplete = true;

        // Build dimension statistics
        DimensionStatistics[] stats = new DimensionStatistics[dimensions];
        for (int d = 0; d < dimensions; d++) {
            stats[d] = estimators[d].toStatistics();
        }

        // Fit models to each dimension using actual data for fitting
        ScalarModel[] scalarModels = new ScalarModel[dimensions];
        ComponentModelFitter.FitResult[] fitResults = new ComponentModelFitter.FitResult[dimensions];

        for (int d = 0; d < dimensions; d++) {
            // Use only the processed samples for fitting
            float[] dimValues;
            if (samplesProcessed < vectorCount) {
                dimValues = new float[(int) samplesProcessed];
                System.arraycopy(dimensionData[d], 0, dimValues, 0, (int) samplesProcessed);
            } else {
                dimValues = dimensionData[d];
            }

            ComponentModelFitter.FitResult result = selector.selectBestResult(stats[d], dimValues);
            scalarModels[d] = result.model();
            fitResults[d] = result;
        }

        VectorSpaceModel model = new VectorSpaceModel(
            uniqueVectors > 0 ? uniqueVectors : samplesProcessed,
            scalarModels
        );

        extractionTimeMs = System.currentTimeMillis() - startTime;

        return new ExtractionResult(model, stats, fitResults, extractionTimeMs);
    }

    /**
     * Checks if all dimensions have converged.
     *
     * @return true if all dimensions have converged
     */
    public boolean allDimensionsConverged() {
        if (estimators == null) return false;
        for (ConvergentDimensionEstimator estimator : estimators) {
            if (!estimator.hasConverged()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the number of dimensions that have converged.
     *
     * @return the count of converged dimensions
     */
    public int getConvergedDimensionCount() {
        if (estimators == null) return 0;
        int count = 0;
        for (ConvergentDimensionEstimator estimator : estimators) {
            if (estimator.hasConverged()) {
                count++;
            }
        }
        return count;
    }

    /**
     * Returns the total number of samples processed.
     *
     * @return the sample count
     */
    public long getSamplesProcessed() {
        return samplesProcessed;
    }

    /**
     * Returns the convergence threshold.
     *
     * @return the convergence threshold
     */
    public double getConvergenceThreshold() {
        return convergenceThreshold;
    }

    /**
     * Returns a summary of convergence across all dimensions.
     *
     * @return the convergence summary
     */
    public ConvergenceSummary getConvergenceSummary() {
        if (estimators == null || !extractionComplete) {
            return null;
        }

        int convergedCount = 0;
        long totalConvergenceSamples = 0;
        long minConvergenceSamples = Long.MAX_VALUE;
        long maxConvergenceSamples = 0;

        ConvergentDimensionEstimator.ConvergenceStatus[] statuses =
            new ConvergentDimensionEstimator.ConvergenceStatus[dimensions];

        for (int d = 0; d < dimensions; d++) {
            statuses[d] = estimators[d].getConvergenceStatus();
            if (estimators[d].hasConverged()) {
                convergedCount++;
                long convergeSamples = estimators[d].getConvergenceSampleCount();
                totalConvergenceSamples += convergeSamples;
                minConvergenceSamples = Math.min(minConvergenceSamples, convergeSamples);
                maxConvergenceSamples = Math.max(maxConvergenceSamples, convergeSamples);
            }
        }

        double avgConvergenceSamples = convergedCount > 0
            ? (double) totalConvergenceSamples / convergedCount
            : 0;

        return new ConvergenceSummary(
            dimensions,
            convergedCount,
            samplesProcessed,
            avgConvergenceSamples,
            minConvergenceSamples == Long.MAX_VALUE ? 0 : minConvergenceSamples,
            maxConvergenceSamples,
            statuses
        );
    }

    /**
     * Summary of convergence across all dimensions.
     *
     * @param totalDimensions total number of dimensions
     * @param convergedDimensions number of converged dimensions
     * @param totalSamplesProcessed total samples processed
     * @param averageConvergenceSamples average samples to convergence
     * @param minConvergenceSamples minimum samples to convergence
     * @param maxConvergenceSamples maximum samples to convergence
     * @param dimensionStatuses per-dimension convergence status
     */
    public record ConvergenceSummary(
        int totalDimensions,
        int convergedDimensions,
        long totalSamplesProcessed,
        double averageConvergenceSamples,
        long minConvergenceSamples,
        long maxConvergenceSamples,
        ConvergentDimensionEstimator.ConvergenceStatus[] dimensionStatuses
    ) {
        /**
         * Returns the convergence rate (fraction of dimensions converged).
         */
        public double convergenceRate() {
            return totalDimensions > 0 ? (double) convergedDimensions / totalDimensions : 0;
        }

        /**
         * Returns true if all dimensions converged.
         */
        public boolean allConverged() {
            return convergedDimensions == totalDimensions;
        }

        /**
         * Returns a human-readable summary.
         */
        @Override
        public String toString() {
            return String.format(
                "Convergence Summary: %d/%d dimensions (%.1f%%) converged after %d samples%n" +
                "  Average convergence: %.0f samples%n" +
                "  Range: %d - %d samples",
                convergedDimensions, totalDimensions, convergenceRate() * 100,
                totalSamplesProcessed,
                averageConvergenceSamples,
                minConvergenceSamples, maxConvergenceSamples
            );
        }
    }

    /**
     * Builder for ConvergentDatasetModelExtractor.
     */
    public static final class Builder {
        private double convergenceThreshold = ConvergentDimensionEstimator.DEFAULT_THRESHOLD;
        private int checkpointInterval = ConvergentDimensionEstimator.DEFAULT_CHECKPOINT_INTERVAL;
        private boolean earlyStoppingEnabled = true;
        private BestFitSelector selector = BestFitSelector.boundedDataSelector();
        private long uniqueVectors = 0;

        private Builder() {}

        /**
         * Sets the convergence threshold.
         *
         * <p>This is the tolerance factor relative to theoretical standard error.
         * Lower values require more precision before declaring convergence.
         *
         * @param threshold the convergence threshold (default: 0.05)
         * @return this builder
         */
        public Builder convergenceThreshold(double threshold) {
            this.convergenceThreshold = threshold;
            return this;
        }

        /**
         * Sets the checkpoint interval.
         *
         * <p>Convergence is checked every N samples.
         *
         * @param interval the checkpoint interval (default: 1000)
         * @return this builder
         */
        public Builder checkpointInterval(int interval) {
            this.checkpointInterval = interval;
            return this;
        }

        /**
         * Enables or disables early stopping.
         *
         * <p>When enabled, extraction stops when all dimensions have converged.
         *
         * @param enabled true to enable early stopping (default: true)
         * @return this builder
         */
        public Builder earlyStoppingEnabled(boolean enabled) {
            this.earlyStoppingEnabled = enabled;
            return this;
        }

        /**
         * Sets the best-fit selector for model fitting.
         *
         * @param selector the selector (default: boundedDataSelector)
         * @return this builder
         */
        public Builder selector(BestFitSelector selector) {
            this.selector = selector;
            return this;
        }

        /**
         * Sets the unique vectors count for the model.
         *
         * @param uniqueVectors the unique vectors count (default: samples processed)
         * @return this builder
         */
        public Builder uniqueVectors(long uniqueVectors) {
            this.uniqueVectors = uniqueVectors;
            return this;
        }

        /**
         * Builds the extractor.
         *
         * @return a new ConvergentDatasetModelExtractor
         */
        public ConvergentDatasetModelExtractor build() {
            return new ConvergentDatasetModelExtractor(this);
        }
    }
}
