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

import java.util.ArrayList;
import java.util.List;

/**
 * Comprehensive accuracy report for closed-loop validation.
 *
 * <p>Aggregates per-dimension accuracy results and provides formatted
 * output suitable for analysis and documentation.
 */
public class AccuracyReport {

    private final String datasetName;
    private final int originalSamples;
    private final int syntheticSamples;
    private final List<StatisticalTestSuite.DimensionAccuracy> dimensionResults;
    private final CorrelationAnalysis.CorrelationComparison correlationComparison;
    private final GeometricMetrics geometricMetrics;

    /**
     * Geometric accuracy metrics for vector space properties.
     */
    public record GeometricMetrics(
        StatisticalTestSuite.TestResult distanceDistributionKS,
        StatisticalTestSuite.TestResult cosineDistributionKS,
        StatisticalTestSuite.TestResult normDistributionKS
    ) {
        public boolean allPassed() {
            return distanceDistributionKS.passed()
                && cosineDistributionKS.passed()
                && normDistributionKS.passed();
        }
    }

    /**
     * Aggregate metrics across all dimensions.
     */
    public record AggregateMetrics(
        double meanKSStatistic,
        double maxKSStatistic,
        double meanMeanError,
        double meanVarianceRelError,
        double meanSkewnessError,
        double meanKurtosisError,
        double meanQQCorrelation,
        int dimensionsPassed,
        int dimensionsFailed,
        int totalDimensions
    ) {
        public double passRate() {
            return totalDimensions > 0 ? (double) dimensionsPassed / totalDimensions : 1.0;
        }
    }

    private AccuracyReport(Builder builder) {
        this.datasetName = builder.datasetName;
        this.originalSamples = builder.originalSamples;
        this.syntheticSamples = builder.syntheticSamples;
        this.dimensionResults = new ArrayList<>(builder.dimensionResults);
        this.correlationComparison = builder.correlationComparison;
        this.geometricMetrics = builder.geometricMetrics;
    }

    /**
     * Creates a new builder for AccuracyReport.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Computes aggregate metrics from dimension results.
     */
    public AggregateMetrics computeAggregateMetrics() {
        if (dimensionResults.isEmpty()) {
            return new AggregateMetrics(0, 0, 0, 0, 0, 0, 1.0, 0, 0, 0);
        }

        double sumKS = 0, maxKS = 0;
        double sumMeanErr = 0, sumVarErr = 0, sumSkewErr = 0, sumKurtErr = 0;
        double sumQQ = 0;
        int passed = 0, failed = 0;

        for (var result : dimensionResults) {
            double ks = result.ksTest().statistic();
            sumKS += ks;
            maxKS = Math.max(maxKS, ks);

            sumMeanErr += result.moments().meanError();
            sumVarErr += result.moments().varianceRelError();
            sumSkewErr += result.moments().skewnessError();
            sumKurtErr += result.moments().kurtosisError();
            sumQQ += result.qqCorrelation();

            if (result.passed()) {
                passed++;
            } else {
                failed++;
            }
        }

        int n = dimensionResults.size();
        return new AggregateMetrics(
            sumKS / n,
            maxKS,
            sumMeanErr / n,
            sumVarErr / n,
            sumSkewErr / n,
            sumKurtErr / n,
            sumQQ / n,
            passed,
            failed,
            n
        );
    }

    /**
     * Returns true if all tests passed.
     */
    public boolean allTestsPassed() {
        AggregateMetrics agg = computeAggregateMetrics();
        boolean dimsPassed = agg.passRate() >= 0.95;
        boolean geomPassed = geometricMetrics == null || geometricMetrics.allPassed();
        return dimsPassed && geomPassed;
    }

    /**
     * Formats the detailed accuracy report.
     */
    public String formatDetailedReport() {
        StringBuilder sb = new StringBuilder();
        AggregateMetrics agg = computeAggregateMetrics();

        // Header
        sb.append(repeat("=", 70)).append("\n");
        sb.append(center("CLOSED-LOOP ACCURACY REPORT", 70)).append("\n");
        sb.append(repeat("=", 70)).append("\n");
        sb.append(String.format("Dataset: %s\n", datasetName));
        sb.append(String.format("Samples: %,d original, %,d synthetic\n",
            originalSamples, syntheticSamples));
        sb.append(repeat("-", 70)).append("\n");

        // Summary
        sb.append("\nSUMMARY\n");
        sb.append(String.format("  Dimensions: %d\n", agg.totalDimensions()));
        sb.append(String.format("  Pass Rate: %d/%d (%.1f%%)\n",
            agg.dimensionsPassed(), agg.totalDimensions(), agg.passRate() * 100));
        sb.append(String.format("  Mean K-S: %.4f\n", agg.meanKSStatistic()));
        sb.append(String.format("  Max K-S: %.4f\n", agg.maxKSStatistic()));

        // Moment matching
        sb.append("\nMOMENT MATCHING (aggregate)\n");
        sb.append(String.format("  Mean error:      %.4f\n", agg.meanMeanError()));
        sb.append(String.format("  Variance error:  %.1f%%\n", agg.meanVarianceRelError() * 100));
        sb.append(String.format("  Skewness error:  %.4f\n", agg.meanSkewnessError()));
        sb.append(String.format("  Kurtosis error:  %.4f\n", agg.meanKurtosisError()));
        sb.append(String.format("  Q-Q correlation: %.4f\n", agg.meanQQCorrelation()));

        // Distribution tests
        sb.append("\nDISTRIBUTION TESTS\n");
        int ksPassed = (int) dimensionResults.stream()
            .filter(d -> d.ksTest().passed()).count();
        int momentsPassed = (int) dimensionResults.stream()
            .filter(d -> d.moments().allPassed()).count();
        sb.append(String.format("  K-S test passed:     %d/%d (%.1f%%)\n",
            ksPassed, agg.totalDimensions(), 100.0 * ksPassed / agg.totalDimensions()));
        sb.append(String.format("  Moments passed:      %d/%d (%.1f%%)\n",
            momentsPassed, agg.totalDimensions(), 100.0 * momentsPassed / agg.totalDimensions()));

        // Correlation structure
        if (correlationComparison != null) {
            sb.append("\nCORRELATION STRUCTURE\n");
            sb.append(String.format("  Significant correlations (|r|>0.1): %d\n",
                correlationComparison.significantCorrelations()));
            sb.append(String.format("  Frobenius norm diff: %.4f\n",
                correlationComparison.frobeniusNormDiff()));
            sb.append(String.format("  Max correlation error: %.4f\n",
                correlationComparison.maxAbsDiff()));
            sb.append(String.format("  Mean correlation error: %.4f\n",
                correlationComparison.meanAbsDiff()));
            if (correlationComparison.lossRate() > 0.1) {
                sb.append("  Note: Independence assumption causes correlation loss\n");
            }
        }

        // Geometric properties
        if (geometricMetrics != null) {
            sb.append("\nGEOMETRIC PROPERTIES\n");
            sb.append(String.format("  Distance dist K-S:   %.4f (%s)\n",
                geometricMetrics.distanceDistributionKS().statistic(),
                geometricMetrics.distanceDistributionKS().passed() ? "PASS" : "FAIL"));
            sb.append(String.format("  Cosine sim dist K-S: %.4f (%s)\n",
                geometricMetrics.cosineDistributionKS().statistic(),
                geometricMetrics.cosineDistributionKS().passed() ? "PASS" : "FAIL"));
            sb.append(String.format("  Norm dist K-S:       %.4f (%s)\n",
                geometricMetrics.normDistributionKS().statistic(),
                geometricMetrics.normDistributionKS().passed() ? "PASS" : "FAIL"));
        }

        // Failed dimensions
        List<StatisticalTestSuite.DimensionAccuracy> failed = dimensionResults.stream()
            .filter(d -> !d.passed())
            .limit(10)
            .toList();

        if (!failed.isEmpty()) {
            sb.append("\nFAILED DIMENSIONS");
            if (agg.dimensionsFailed() > 10) {
                sb.append(String.format(" (showing 10 of %d)", agg.dimensionsFailed()));
            }
            sb.append("\n");
            for (var dim : failed) {
                sb.append(String.format("  Dim %3d: K-S=%.4f, type=%s",
                    dim.dimension(), dim.ksTest().statistic(), dim.modelType()));
                if (!dim.moments().skewnessPassed()) {
                    sb.append(String.format(", skew_err=%.2f", dim.moments().skewnessError()));
                }
                if (!dim.moments().kurtosisPassed()) {
                    sb.append(String.format(", kurt_err=%.2f", dim.moments().kurtosisError()));
                }
                sb.append("\n");
            }
        }

        sb.append(repeat("=", 70)).append("\n");

        return sb.toString();
    }

    /**
     * Formats a summary table of results.
     */
    public String formatSummaryTable() {
        AggregateMetrics agg = computeAggregateMetrics();
        StringBuilder sb = new StringBuilder();

        sb.append(String.format("%-20s %s\n", "Dataset:", datasetName));
        sb.append(String.format("%-20s %d / %d (%.1f%%)\n", "Pass Rate:",
            agg.dimensionsPassed(), agg.totalDimensions(), agg.passRate() * 100));
        sb.append(String.format("%-20s %.4f\n", "Mean K-S:", agg.meanKSStatistic()));
        sb.append(String.format("%-20s %.4f\n", "Max K-S:", agg.maxKSStatistic()));
        sb.append(String.format("%-20s %.4f\n", "Mean Q-Q Corr:", agg.meanQQCorrelation()));
        sb.append(String.format("%-20s %s\n", "Overall:",
            allTestsPassed() ? "PASS" : "FAIL"));

        return sb.toString();
    }

    private static String repeat(String s, int n) {
        return s.repeat(n);
    }

    private static String center(String s, int width) {
        int padding = (width - s.length()) / 2;
        return " ".repeat(Math.max(0, padding)) + s;
    }

    // Getters
    public String getDatasetName() { return datasetName; }
    public int getOriginalSamples() { return originalSamples; }
    public int getSyntheticSamples() { return syntheticSamples; }
    public List<StatisticalTestSuite.DimensionAccuracy> getDimensionResults() {
        return new ArrayList<>(dimensionResults);
    }
    public CorrelationAnalysis.CorrelationComparison getCorrelationComparison() {
        return correlationComparison;
    }
    public GeometricMetrics getGeometricMetrics() { return geometricMetrics; }

    /**
     * Builder for AccuracyReport.
     */
    public static class Builder {
        private String datasetName = "unknown";
        private int originalSamples;
        private int syntheticSamples;
        private final List<StatisticalTestSuite.DimensionAccuracy> dimensionResults = new ArrayList<>();
        private CorrelationAnalysis.CorrelationComparison correlationComparison;
        private GeometricMetrics geometricMetrics;

        public Builder datasetName(String name) {
            this.datasetName = name;
            return this;
        }

        public Builder sampleCounts(int original, int synthetic) {
            this.originalSamples = original;
            this.syntheticSamples = synthetic;
            return this;
        }

        public Builder addDimensionResult(StatisticalTestSuite.DimensionAccuracy result) {
            this.dimensionResults.add(result);
            return this;
        }

        public Builder dimensionResults(List<StatisticalTestSuite.DimensionAccuracy> results) {
            this.dimensionResults.clear();
            this.dimensionResults.addAll(results);
            return this;
        }

        public Builder correlationComparison(CorrelationAnalysis.CorrelationComparison comparison) {
            this.correlationComparison = comparison;
            return this;
        }

        public Builder geometricMetrics(GeometricMetrics metrics) {
            this.geometricMetrics = metrics;
            return this;
        }

        public AccuracyReport build() {
            return new AccuracyReport(this);
        }
    }
}
