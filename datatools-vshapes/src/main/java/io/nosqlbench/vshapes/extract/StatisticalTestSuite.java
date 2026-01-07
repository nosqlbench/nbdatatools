package io.nosqlbench.vshapes.extract;

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

import java.util.Arrays;

/**
 * Statistical test suite for accuracy validation.
 *
 * <p>Provides implementations of statistical tests for comparing
 * distributions between original and synthetic data:
 * <ul>
 *   <li>Two-sample Kolmogorov-Smirnov test</li>
 *   <li>Moment comparison (mean, variance, skewness, kurtosis)</li>
 *   <li>Quantile comparison</li>
 * </ul>
 *
 * <p>All tests follow the K-S + Moments approach for efficient,
 * interpretable validation.
 */
public final class StatisticalTestSuite {

    private StatisticalTestSuite() {
        // Utility class
    }

    /**
     * Result of a statistical test.
     *
     * @param testName name of the test
     * @param statistic the test statistic value
     * @param criticalValue critical value at specified alpha
     * @param alpha significance level
     * @param passed whether the test passed (statistic &lt; critical value)
     */
    public record TestResult(
        String testName,
        double statistic,
        double criticalValue,
        double alpha,
        boolean passed
    ) {
        /**
         * Creates a test result with standard alpha=0.05.
         */
        public static TestResult of(String name, double statistic, double critical) {
            return new TestResult(name, statistic, critical, 0.05, statistic < critical);
        }
    }

    /**
     * Result of moment comparison between two samples.
     *
     * @param meanError absolute error in means
     * @param varianceRelError relative error in variance (as fraction)
     * @param skewnessError absolute error in skewness
     * @param kurtosisError absolute error in kurtosis
     * @param meanPassed mean error within tolerance
     * @param variancePassed variance error within tolerance
     * @param skewnessPassed skewness error within tolerance
     * @param kurtosisPassed kurtosis error within tolerance
     */
    public record MomentComparison(
        double meanError,
        double varianceRelError,
        double skewnessError,
        double kurtosisError,
        boolean meanPassed,
        boolean variancePassed,
        boolean skewnessPassed,
        boolean kurtosisPassed
    ) {
        /**
         * Returns true if all moment tests passed.
         */
        public boolean allPassed() {
            return meanPassed && variancePassed && skewnessPassed && kurtosisPassed;
        }
    }

    /**
     * Comprehensive accuracy result for a single dimension.
     */
    public record DimensionAccuracy(
        int dimension,
        String modelType,
        TestResult ksTest,
        MomentComparison moments,
        double qqCorrelation
    ) {
        /**
         * Returns true if all tests passed for this dimension.
         */
        public boolean passed() {
            return ksTest.passed() && moments.allPassed() && qqCorrelation > 0.995;
        }
    }

    // ========== Two-Sample Kolmogorov-Smirnov Test ==========

    /**
     * Performs a two-sample Kolmogorov-Smirnov test.
     *
     * <p>The K-S test measures the maximum absolute difference between
     * the empirical CDFs of two samples. The critical value is:
     * D_crit = c(α) × √((n1 + n2) / (n1 × n2))
     *
     * <p>where c(α) = 1.36 for α=0.05, 1.63 for α=0.01.
     *
     * @param sample1 first sample (will be sorted internally)
     * @param sample2 second sample (will be sorted internally)
     * @return test result with statistic, critical value, and pass/fail
     */
    public static TestResult kolmogorovSmirnovTest(float[] sample1, float[] sample2) {
        return kolmogorovSmirnovTest(sample1, sample2, 0.05);
    }

    /**
     * Performs a two-sample K-S test with specified significance level.
     *
     * @param sample1 first sample
     * @param sample2 second sample
     * @param alpha significance level (0.01, 0.05, or 0.10)
     * @return test result
     */
    public static TestResult kolmogorovSmirnovTest(float[] sample1, float[] sample2, double alpha) {
        if (sample1 == null || sample2 == null) {
            throw new IllegalArgumentException("Samples cannot be null");
        }
        if (sample1.length < 2 || sample2.length < 2) {
            throw new IllegalArgumentException("Samples must have at least 2 elements");
        }

        int n1 = sample1.length;
        int n2 = sample2.length;

        // Sort copies
        float[] sorted1 = sample1.clone();
        float[] sorted2 = sample2.clone();
        Arrays.sort(sorted1);
        Arrays.sort(sorted2);

        // Compute K-S statistic: max |F1(x) - F2(x)|
        double maxD = computeKSStatistic(sorted1, sorted2);

        // Compute critical value
        // Note: Must use (long) cast to avoid integer overflow for large samples (n1*n2 > Integer.MAX_VALUE)
        double c = getCriticalCoefficient(alpha);
        double criticalValue = c * Math.sqrt((double) (n1 + n2) / ((long) n1 * n2));

        return new TestResult("Two-Sample K-S", maxD, criticalValue, alpha, maxD < criticalValue);
    }

    /**
     * Computes the K-S statistic between two sorted samples.
     */
    private static double computeKSStatistic(float[] sorted1, float[] sorted2) {
        int n1 = sorted1.length;
        int n2 = sorted2.length;

        double maxD = 0;
        int i = 0, j = 0;

        while (i < n1 && j < n2) {
            double cdf1 = (double) (i + 1) / n1;
            double cdf2 = (double) (j + 1) / n2;

            if (sorted1[i] <= sorted2[j]) {
                double d = Math.abs(cdf1 - (double) j / n2);
                maxD = Math.max(maxD, d);
                i++;
            } else {
                double d = Math.abs((double) i / n1 - cdf2);
                maxD = Math.max(maxD, d);
                j++;
            }
        }

        // Handle remaining elements
        while (i < n1) {
            double d = Math.abs((double) (i + 1) / n1 - 1.0);
            maxD = Math.max(maxD, d);
            i++;
        }
        while (j < n2) {
            double d = Math.abs(1.0 - (double) (j + 1) / n2);
            maxD = Math.max(maxD, d);
            j++;
        }

        return maxD;
    }

    /**
     * Gets the critical coefficient c(α) for K-S test.
     */
    private static double getCriticalCoefficient(double alpha) {
        if (alpha <= 0.01) {
            return 1.63;  // α = 0.01
        } else if (alpha <= 0.05) {
            return 1.36;  // α = 0.05
        } else {
            return 1.22;  // α = 0.10
        }
    }

    // ========== Moment Comparison ==========

    /**
     * Compares moments between two samples with default tolerances.
     *
     * <p>Default tolerances:
     * <ul>
     *   <li>Mean: |error| &lt; 0.01 × σ_original</li>
     *   <li>Variance: relative error &lt; 5%</li>
     *   <li>Skewness: |error| &lt; 0.15</li>
     *   <li>Kurtosis: |error| &lt; 0.5</li>
     * </ul>
     *
     * @param original original sample
     * @param synthetic synthetic sample
     * @return moment comparison result
     */
    public static MomentComparison compareMoments(float[] original, float[] synthetic) {
        return compareMoments(original, synthetic, 0.01, 0.05, 0.15, 0.5);
    }

    /**
     * Compares moments between two samples with custom tolerances.
     *
     * @param original original sample
     * @param synthetic synthetic sample
     * @param meanTolFactor mean tolerance as factor of σ (0.01 = 1% of stddev)
     * @param varianceTol variance relative tolerance (0.05 = 5%)
     * @param skewnessTol skewness absolute tolerance
     * @param kurtosisTol kurtosis absolute tolerance
     * @return moment comparison result
     */
    public static MomentComparison compareMoments(
            float[] original, float[] synthetic,
            double meanTolFactor, double varianceTol,
            double skewnessTol, double kurtosisTol) {

        // Compute statistics for original
        DimensionStatistics origStats = DimensionStatistics.compute(0, original);
        DimensionStatistics synthStats = DimensionStatistics.compute(0, synthetic);

        // Compute errors
        double meanError = Math.abs(origStats.mean() - synthStats.mean());
        double meanTolerance = meanTolFactor * origStats.stdDev();

        double varianceRelError = origStats.variance() > 0
            ? Math.abs(origStats.variance() - synthStats.variance()) / origStats.variance()
            : 0;

        double skewnessError = Math.abs(origStats.skewness() - synthStats.skewness());
        double kurtosisError = Math.abs(origStats.kurtosis() - synthStats.kurtosis());

        return new MomentComparison(
            meanError,
            varianceRelError,
            skewnessError,
            kurtosisError,
            meanError < meanTolerance,
            varianceRelError < varianceTol,
            skewnessError < skewnessTol,
            kurtosisError < kurtosisTol
        );
    }

    // ========== Quantile Comparison ==========

    /**
     * Computes specific quantiles for a sorted sample.
     *
     * @param sorted sorted sample
     * @param quantiles quantile values (e.g., 0.25, 0.5, 0.75)
     * @return computed quantile values
     */
    public static double[] computeQuantiles(float[] sorted, double[] quantiles) {
        double[] result = new double[quantiles.length];
        int n = sorted.length;

        for (int i = 0; i < quantiles.length; i++) {
            double q = quantiles[i];
            double index = q * (n - 1);
            int lower = (int) Math.floor(index);
            int upper = (int) Math.ceil(index);

            if (lower == upper || upper >= n) {
                result[i] = sorted[Math.min(lower, n - 1)];
            } else {
                double frac = index - lower;
                result[i] = sorted[lower] * (1 - frac) + sorted[upper] * frac;
            }
        }

        return result;
    }

    /**
     * Compares quantiles between two samples.
     *
     * @param original original sample (will be sorted)
     * @param synthetic synthetic sample (will be sorted)
     * @param quantiles quantile values to compare
     * @return array of absolute errors at each quantile
     */
    public static double[] compareQuantiles(float[] original, float[] synthetic, double[] quantiles) {
        float[] sortedOrig = original.clone();
        float[] sortedSynth = synthetic.clone();
        Arrays.sort(sortedOrig);
        Arrays.sort(sortedSynth);

        double[] origQuantiles = computeQuantiles(sortedOrig, quantiles);
        double[] synthQuantiles = computeQuantiles(sortedSynth, quantiles);

        double[] errors = new double[quantiles.length];
        for (int i = 0; i < quantiles.length; i++) {
            errors[i] = Math.abs(origQuantiles[i] - synthQuantiles[i]);
        }

        return errors;
    }

    // ========== Q-Q Correlation ==========

    /**
     * Computes the Q-Q plot correlation between two samples.
     *
     * <p>A high correlation (close to 1.0) indicates the distributions match well.
     *
     * @param original original sample
     * @param synthetic synthetic sample
     * @param numQuantiles number of quantiles to compare
     * @return Pearson correlation coefficient
     */
    public static double qqCorrelation(float[] original, float[] synthetic, int numQuantiles) {
        float[] sortedOrig = original.clone();
        float[] sortedSynth = synthetic.clone();
        Arrays.sort(sortedOrig);
        Arrays.sort(sortedSynth);

        // Generate quantile points
        double[] quantiles = new double[numQuantiles];
        for (int i = 0; i < numQuantiles; i++) {
            quantiles[i] = (i + 0.5) / numQuantiles;
        }

        double[] origQ = computeQuantiles(sortedOrig, quantiles);
        double[] synthQ = computeQuantiles(sortedSynth, quantiles);

        // Compute Pearson correlation
        double meanOrig = 0, meanSynth = 0;
        for (int i = 0; i < numQuantiles; i++) {
            meanOrig += origQ[i];
            meanSynth += synthQ[i];
        }
        meanOrig /= numQuantiles;
        meanSynth /= numQuantiles;

        double cov = 0, varOrig = 0, varSynth = 0;
        for (int i = 0; i < numQuantiles; i++) {
            double dOrig = origQ[i] - meanOrig;
            double dSynth = synthQ[i] - meanSynth;
            cov += dOrig * dSynth;
            varOrig += dOrig * dOrig;
            varSynth += dSynth * dSynth;
        }

        if (varOrig <= 0 || varSynth <= 0) {
            return 1.0;  // Constant data
        }

        return cov / Math.sqrt(varOrig * varSynth);
    }

    /**
     * Computes Q-Q correlation with default 100 quantiles.
     */
    public static double qqCorrelation(float[] original, float[] synthetic) {
        return qqCorrelation(original, synthetic, 100);
    }

    // ========== Comprehensive Dimension Accuracy ==========

    /**
     * Performs comprehensive accuracy analysis for a single dimension.
     *
     * @param dimension dimension index
     * @param modelType model type string
     * @param original original sample
     * @param synthetic synthetic sample
     * @return comprehensive accuracy result
     */
    public static DimensionAccuracy analyzeDimension(
            int dimension, String modelType,
            float[] original, float[] synthetic) {

        TestResult ksTest = kolmogorovSmirnovTest(original, synthetic);
        MomentComparison moments = compareMoments(original, synthetic);
        double qqCorr = qqCorrelation(original, synthetic);

        return new DimensionAccuracy(dimension, modelType, ksTest, moments, qqCorr);
    }
}
