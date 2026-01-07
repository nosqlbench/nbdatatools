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

/**
 * Online parameter estimator with convergence detection for a single dimension.
 *
 * <p>This class tracks statistical parameters incrementally using Welford's algorithm
 * and detects when parameters have converged to stable values. Convergence is determined
 * by comparing parameter changes between estimation windows against expected standard errors.
 *
 * <h2>Convergence Theory</h2>
 *
 * <p>For method-of-moments estimators on IID data, parameter estimates converge at rate O(1/√n):
 *
 * <table border="1">
 *   <caption>Standard Errors and Convergence Rates for Statistical Parameters</caption>
 *   <tr><th>Parameter</th><th>Standard Error</th><th>Convergence Rate</th></tr>
 *   <tr><td>Mean (μ)</td><td>σ/√n</td><td>O(1/√n)</td></tr>
 *   <tr><td>Variance (σ²)</td><td>σ²√(2/n)</td><td>O(1/√n)</td></tr>
 *   <tr><td>Skewness (γ₁)</td><td>√(6/n)</td><td>O(1/√n)</td></tr>
 *   <tr><td>Kurtosis (γ₂)</td><td>√(24/n)</td><td>O(1/√n)</td></tr>
 * </table>
 *
 * <h2>Convergence Detection</h2>
 *
 * <p>The estimator tracks parameter values at checkpoints and compares changes:
 * <pre>
 * Convergence criterion: |θ̂ₙ - θ̂ₙ₋ₖ| &lt; ε × SE(θ̂ₙ)
 * </pre>
 *
 * <p>Where:
 * <ul>
 *   <li>θ̂ₙ = parameter estimate at n samples</li>
 *   <li>k = checkpoint interval (samples between convergence checks)</li>
 *   <li>ε = tolerance factor (e.g., 0.1 = within 10% of standard error)</li>
 *   <li>SE(θ̂ₙ) = theoretical standard error at n samples</li>
 * </ul>
 *
 * <h2>Thread Safety</h2>
 *
 * <p>This class is NOT thread-safe. Each dimension should have its own estimator
 * instance, and external synchronization is required for concurrent access.
 *
 * @see ConvergentDatasetModelExtractor
 */
public final class ConvergentDimensionEstimator {

    /**
     * Default convergence threshold - high precision.
     * Parameter changes must be within 5% of theoretical standard error.
     */
    public static final double DEFAULT_THRESHOLD = 0.05;

    /**
     * Default checkpoint interval - check convergence every 1000 samples.
     */
    public static final int DEFAULT_CHECKPOINT_INTERVAL = 1000;

    /**
     * Minimum samples required before convergence can be declared.
     */
    public static final int MINIMUM_SAMPLES = 5000;

    private final int dimension;
    private final double convergenceThreshold;
    private final int checkpointInterval;

    // Welford's algorithm state
    private long count = 0;
    private double mean = 0;
    private double m2 = 0;   // Second central moment (for variance)
    private double m3 = 0;   // Third central moment (for skewness)
    private double m4 = 0;   // Fourth central moment (for kurtosis)
    private double min = Double.MAX_VALUE;
    private double max = -Double.MAX_VALUE;

    // Checkpoint state for convergence detection
    private long checkpointCount = 0;
    private double checkpointMean = 0;
    private double checkpointVariance = 0;
    private double checkpointSkewness = 0;
    private double checkpointKurtosis = 0;

    // Convergence state
    private boolean meanConverged = false;
    private boolean varianceConverged = false;
    private boolean skewnessConverged = false;
    private boolean kurtosisConverged = false;
    private long convergenceSampleCount = 0;

    /**
     * Creates a convergent estimator with default parameters.
     *
     * @param dimension the dimension index
     */
    public ConvergentDimensionEstimator(int dimension) {
        this(dimension, DEFAULT_THRESHOLD, DEFAULT_CHECKPOINT_INTERVAL);
    }

    /**
     * Creates a convergent estimator with custom parameters.
     *
     * @param dimension the dimension index
     * @param convergenceThreshold tolerance factor for convergence (e.g., 0.05 = 5% of SE)
     * @param checkpointInterval samples between convergence checks
     */
    public ConvergentDimensionEstimator(int dimension, double convergenceThreshold, int checkpointInterval) {
        this.dimension = dimension;
        this.convergenceThreshold = convergenceThreshold;
        this.checkpointInterval = checkpointInterval;
    }

    /**
     * Accepts a new value into the estimator.
     *
     * @param value the value to accumulate
     */
    public void accept(float value) {
        count++;
        double delta = value - mean;
        double deltaN = delta / count;
        double deltaN2 = deltaN * deltaN;
        double term1 = delta * deltaN * (count - 1);

        mean += deltaN;

        // Update higher moments (order matters - M4 before M3 before M2)
        m4 += term1 * deltaN2 * (count * count - 3 * count + 3) +
              6 * deltaN2 * m2 - 4 * deltaN * m3;
        m3 += term1 * deltaN * (count - 2) - 3 * deltaN * m2;
        m2 += term1;

        if (value < min) min = value;
        if (value > max) max = value;

        // Check convergence at checkpoint intervals
        if (count >= MINIMUM_SAMPLES && count % checkpointInterval == 0) {
            checkConvergence();
        }
    }

    /**
     * Accepts multiple values in a batch.
     *
     * @param values the values to accumulate
     */
    public void acceptAll(float[] values) {
        for (float value : values) {
            accept(value);
        }
    }

    /**
     * Accepts a batch of values from a specific offset.
     *
     * @param values the array containing values
     * @param offset the starting offset
     * @param length the number of values to accept
     */
    public void acceptBatch(float[] values, int offset, int length) {
        for (int i = 0; i < length; i++) {
            accept(values[offset + i]);
        }
    }

    /**
     * Checks if all parameters have converged.
     *
     * @return true if all parameters have converged
     */
    public boolean hasConverged() {
        return meanConverged && varianceConverged && skewnessConverged && kurtosisConverged;
    }

    /**
     * Checks if mean and variance have converged (sufficient for many applications).
     *
     * @return true if mean and variance have converged
     */
    public boolean hasPrimaryConverged() {
        return meanConverged && varianceConverged;
    }

    /**
     * Returns the sample count at which convergence was achieved.
     *
     * @return convergence sample count, or 0 if not yet converged
     */
    public long getConvergenceSampleCount() {
        return convergenceSampleCount;
    }

    /**
     * Returns the current sample count.
     *
     * @return the number of samples processed
     */
    public long getCount() {
        return count;
    }

    /**
     * Returns the dimension index.
     *
     * @return the dimension index
     */
    public int getDimension() {
        return dimension;
    }

    /**
     * Returns the current mean estimate.
     *
     * @return the mean
     */
    public double getMean() {
        return mean;
    }

    /**
     * Returns the current variance estimate.
     *
     * @return the variance
     */
    public double getVariance() {
        return count > 1 ? m2 / count : 0;
    }

    /**
     * Returns the current standard deviation estimate.
     *
     * @return the standard deviation
     */
    public double getStdDev() {
        return Math.sqrt(getVariance());
    }

    /**
     * Returns the current skewness estimate.
     *
     * @return the skewness
     */
    public double getSkewness() {
        if (count < 3) return 0;
        double variance = getVariance();
        if (variance <= 0) return 0;
        double stdDev = Math.sqrt(variance);
        return (m3 / count) / (stdDev * stdDev * stdDev);
    }

    /**
     * Returns the current kurtosis estimate.
     *
     * @return the kurtosis (excess kurtosis, Normal = 0)
     */
    public double getKurtosis() {
        if (count < 4) return 0;
        double variance = getVariance();
        if (variance <= 0) return 0;
        return (m4 / count) / (variance * variance) - 3.0;
    }

    /**
     * Returns the observed minimum value.
     *
     * @return the minimum
     */
    public double getMin() {
        return min;
    }

    /**
     * Returns the observed maximum value.
     *
     * @return the maximum
     */
    public double getMax() {
        return max;
    }

    /**
     * Converts to DimensionStatistics record.
     *
     * @return the dimension statistics
     */
    public DimensionStatistics toStatistics() {
        return new DimensionStatistics(
            dimension, count, min, max, mean, getVariance(), getSkewness(), getKurtosis() + 3.0
        );
    }

    /**
     * Returns convergence status for each parameter.
     *
     * @return convergence status
     */
    public ConvergenceStatus getConvergenceStatus() {
        return new ConvergenceStatus(
            dimension, count, convergenceSampleCount,
            meanConverged, varianceConverged, skewnessConverged, kurtosisConverged
        );
    }

    /**
     * Checks convergence by comparing current estimates to checkpoint estimates.
     */
    private void checkConvergence() {
        if (checkpointCount == 0) {
            // First checkpoint - just save state
            saveCheckpoint();
            return;
        }

        double currentVariance = getVariance();
        double currentSkewness = getSkewness();
        double currentKurtosis = getKurtosis();

        // Calculate standard errors
        double seMean = Math.sqrt(currentVariance / count);
        double seVariance = currentVariance * Math.sqrt(2.0 / count);
        double seSkewness = Math.sqrt(6.0 / count);
        double seKurtosis = Math.sqrt(24.0 / count);

        // Check convergence for each parameter
        if (!meanConverged && seMean > 0) {
            double meanChange = Math.abs(mean - checkpointMean);
            meanConverged = meanChange < convergenceThreshold * seMean;
        }

        if (!varianceConverged && seVariance > 0) {
            double varianceChange = Math.abs(currentVariance - checkpointVariance);
            varianceConverged = varianceChange < convergenceThreshold * seVariance;
        }

        if (!skewnessConverged) {
            double skewnessChange = Math.abs(currentSkewness - checkpointSkewness);
            skewnessConverged = skewnessChange < convergenceThreshold * seSkewness;
        }

        if (!kurtosisConverged) {
            double kurtosisChange = Math.abs(currentKurtosis - checkpointKurtosis);
            kurtosisConverged = kurtosisChange < convergenceThreshold * seKurtosis;
        }

        // Record convergence point
        if (hasConverged() && convergenceSampleCount == 0) {
            convergenceSampleCount = count;
        }

        // Save new checkpoint
        saveCheckpoint();
    }

    private void saveCheckpoint() {
        checkpointCount = count;
        checkpointMean = mean;
        checkpointVariance = getVariance();
        checkpointSkewness = getSkewness();
        checkpointKurtosis = getKurtosis();
    }

    /**
     * Convergence status record for a dimension.
     *
     * @param dimension the dimension index
     * @param sampleCount current sample count
     * @param convergenceSampleCount sample count when convergence was achieved (0 if not converged)
     * @param meanConverged whether mean has converged
     * @param varianceConverged whether variance has converged
     * @param skewnessConverged whether skewness has converged
     * @param kurtosisConverged whether kurtosis has converged
     */
    public record ConvergenceStatus(
        int dimension,
        long sampleCount,
        long convergenceSampleCount,
        boolean meanConverged,
        boolean varianceConverged,
        boolean skewnessConverged,
        boolean kurtosisConverged
    ) {
        /**
         * Returns true if all parameters have converged.
         */
        public boolean allConverged() {
            return meanConverged && varianceConverged && skewnessConverged && kurtosisConverged;
        }

        /**
         * Returns true if mean and variance have converged.
         */
        public boolean primaryConverged() {
            return meanConverged && varianceConverged;
        }

        /**
         * Returns a human-readable summary.
         */
        @Override
        public String toString() {
            return String.format(
                "Dim %d: n=%d, converged@%d [mean=%s, var=%s, skew=%s, kurt=%s]",
                dimension, sampleCount, convergenceSampleCount,
                meanConverged ? "✓" : "✗",
                varianceConverged ? "✓" : "✗",
                skewnessConverged ? "✓" : "✗",
                kurtosisConverged ? "✓" : "✗"
            );
        }
    }
}
