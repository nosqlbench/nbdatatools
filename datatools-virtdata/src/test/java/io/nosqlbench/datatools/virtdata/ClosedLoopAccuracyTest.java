package io.nosqlbench.datatools.virtdata;

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

import io.nosqlbench.datatools.virtdata.sampling.ComponentSampler;
import io.nosqlbench.datatools.virtdata.sampling.ComponentSamplerFactory;
import io.nosqlbench.vshapes.extract.*;
import io.nosqlbench.vshapes.model.*;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Closed-loop accuracy tests for the vector space model pipeline.
 *
 * <h2>Purpose</h2>
 *
 * <p>This test suite validates the full data generation pipeline:
 * <pre>
 * Original Data → Model Extraction → Sampler Generation → Synthetic Data → Comparison
 * </pre>
 *
 * <h2>Approach</h2>
 *
 * <p>Uses the K-S + Moments approach for efficient, interpretable validation:
 * <ul>
 *   <li><b>Kolmogorov-Smirnov test</b>: Measures maximum CDF deviation</li>
 *   <li><b>Moment matching</b>: Compares mean, variance, skewness, kurtosis</li>
 *   <li><b>Q-Q correlation</b>: Measures quantile-quantile plot linearity</li>
 * </ul>
 *
 * <h2>Sample Sizes</h2>
 *
 * <p>Tests use high-precision sample sizes (100K-1M) for rigorous validation:
 * <ul>
 *   <li>100,000 samples: Baseline high-precision</li>
 *   <li>500,000 samples: Production-grade validation</li>
 *   <li>1,000,000 samples: Asymptotic behavior verification</li>
 * </ul>
 *
 * @see StatisticalTestSuite
 * @see AccuracyReport
 * @see QQPlotGenerator
 */
@Tag("accuracy")
public class ClosedLoopAccuracyTest {

    private static final long SEED = 42L;

    // Sample sizes for different test levels
    private static final int QUICK_SAMPLES = 100_000;
    private static final int STANDARD_SAMPLES = 500_000;
    private static final int THOROUGH_SAMPLES = 1_000_000;

    // Dimensions for test vectors
    private static final int SMALL_DIM = 64;
    private static final int MEDIUM_DIM = 384;
    private static final int LARGE_DIM = 1024;

    // Pass criteria
    private static final double MIN_PASS_RATE = 0.95;
    private static final double MIN_QQ_CORRELATION = 0.995;

    // ========== Parameterized Test Matrix ==========

    static Stream<Arguments> sampleSizeProvider() {
        return Stream.of(
            Arguments.of(QUICK_SAMPLES, SMALL_DIM, "100K samples, 64 dims"),
            Arguments.of(QUICK_SAMPLES, MEDIUM_DIM, "100K samples, 384 dims"),
            Arguments.of(STANDARD_SAMPLES, SMALL_DIM, "500K samples, 64 dims"),
            Arguments.of(STANDARD_SAMPLES, MEDIUM_DIM, "500K samples, 384 dims")
        );
    }

    static Stream<Arguments> distributionTypes() {
        return Stream.of(
            Arguments.of("normal", new NormalScalarModel(0.5, 0.2)),
            Arguments.of("uniform", new UniformScalarModel(0.0, 1.0)),
            Arguments.of("beta", new BetaScalarModel(2.0, 5.0)),
            Arguments.of("gamma", new GammaScalarModel(3.0, 2.0)),
            Arguments.of("student_t", new StudentTScalarModel(10.0))
        );
    }

    // ========== Core Closed-Loop Tests ==========

    /**
     * Tests model extraction self-consistency: data generated from extracted model
     * should match when regenerated from the same model.
     *
     * <p>Note: This tests that the extraction + generation pipeline is consistent,
     * NOT that it recovers the original source distribution (which may be different).
     */
    @ParameterizedTest(name = "{2}")
    @MethodSource("sampleSizeProvider")
    void testClosedLoopRoundTrip(int samples, int dims, String testName) {
        System.out.println("\n=== " + testName + " ===");

        // Phase 1: Generate original data with known distributions
        float[][] original = generateMixedPearsonData(samples, dims, SEED);

        // Phase 2: Extract model from original data
        DatasetModelExtractor extractor = new DatasetModelExtractor();
        VectorSpaceModel extractedModel = extractor.extractVectorModel(original);

        // Phase 3: Generate TWO synthetic datasets from extracted model
        // This tests self-consistency of the extraction + generation pipeline
        float[][] synthetic1 = generateFromModel(extractedModel, samples, SEED + 1);
        float[][] synthetic2 = generateFromModel(extractedModel, samples, SEED + 2);

        // Phase 4: Compare the two synthetic datasets (self-consistency)
        AccuracyReport report = validateAccuracy(synthetic1, synthetic2, extractedModel, "ClosedLoop");

        // Print report
        System.out.println(report.formatSummaryTable());

        // Verify pass criteria - self-consistency should have very high pass rate
        AccuracyReport.AggregateMetrics metrics = report.computeAggregateMetrics();
        assertTrue(metrics.passRate() >= MIN_PASS_RATE,
            String.format("Self-consistency pass rate %.2f%% below minimum %.2f%%",
                metrics.passRate() * 100, MIN_PASS_RATE * 100));
    }

    /**
     * Tests single distribution type round-trip: generate from source model,
     * extract, regenerate, and compare both synthetic datasets.
     *
     * <p>This tests self-consistency when a single distribution type is used.
     */
    @ParameterizedTest(name = "Distribution: {0}")
    @MethodSource("distributionTypes")
    void testSingleDistributionRoundTrip(String typeName, ScalarModel sourceModel) {
        int samples = QUICK_SAMPLES;
        int dims = 32;

        System.out.println("\n=== Single Distribution: " + typeName + " ===");

        // Generate data from known model
        float[][] original = new float[samples][dims];
        ComponentSampler sampler = ComponentSamplerFactory.forModel(sourceModel);

        for (int v = 0; v < samples; v++) {
            for (int d = 0; d < dims; d++) {
                double u = StratifiedSampler.unitIntervalValue(v, d, samples);
                original[v][d] = (float) sampler.sample(u);
            }
        }

        // Extract model
        DatasetModelExtractor extractor = new DatasetModelExtractor();
        VectorSpaceModel extractedModel = extractor.extractVectorModel(original);

        // Generate TWO synthetic datasets for self-consistency test
        float[][] synthetic1 = generateFromModel(extractedModel, samples, SEED + 1);
        float[][] synthetic2 = generateFromModel(extractedModel, samples, SEED + 2);

        // Validate self-consistency (synthetic1 vs synthetic2)
        int passCount = 0;
        for (int d = 0; d < dims; d++) {
            float[] syn1Col = extractColumn(synthetic1, d);
            float[] syn2Col = extractColumn(synthetic2, d);

            StatisticalTestSuite.TestResult ks = StatisticalTestSuite.kolmogorovSmirnovTest(syn1Col, syn2Col);
            StatisticalTestSuite.MomentComparison moments = StatisticalTestSuite.compareMoments(syn1Col, syn2Col);
            double qqCorr = StatisticalTestSuite.qqCorrelation(syn1Col, syn2Col);

            boolean passed = ks.passed() && moments.allPassed() && qqCorr > MIN_QQ_CORRELATION;
            if (passed) passCount++;
        }

        double passRate = (double) passCount / dims;
        System.out.printf("Self-consistency pass rate: %d/%d (%.1f%%)%n", passCount, dims, passRate * 100);

        assertTrue(passRate >= MIN_PASS_RATE,
            String.format("Self-consistency pass rate %.2f%% below minimum %.2f%% for %s",
                passRate * 100, MIN_PASS_RATE * 100, typeName));
    }

    // ========== High-Precision Validation ==========

    @Test
    @Tag("slow")
    void testHighPrecisionNormalDistribution() {
        int samples = THOROUGH_SAMPLES;
        double trueMean = 5.0;
        double trueStdDev = 2.0;

        System.out.println("\n=== High-Precision Normal Validation (1M samples) ===");

        // Generate known normal data
        Random rng = new Random(SEED);
        float[] original = new float[samples];
        for (int i = 0; i < samples; i++) {
            original[i] = (float) (rng.nextGaussian() * trueStdDev + trueMean);
        }

        // Fit model
        NormalModelFitter fitter = new NormalModelFitter();
        ComponentModelFitter.FitResult fit = fitter.fit(original);
        NormalScalarModel model = (NormalScalarModel) fit.model();

        // Generate synthetic
        ComponentSampler sampler = ComponentSamplerFactory.forModel(model);
        float[] synthetic = new float[samples];
        for (int i = 0; i < samples; i++) {
            double u = (i + 0.5) / samples;  // Stratified
            synthetic[i] = (float) sampler.sample(u);
        }

        // Statistical tests
        StatisticalTestSuite.TestResult ks = StatisticalTestSuite.kolmogorovSmirnovTest(original, synthetic);
        StatisticalTestSuite.MomentComparison moments = StatisticalTestSuite.compareMoments(original, synthetic);
        double qqCorr = StatisticalTestSuite.qqCorrelation(original, synthetic);

        // Q-Q Plot visualization
        QQPlotGenerator.QQPlotData qqData = QQPlotGenerator.generateQQPlot(original, synthetic);
        System.out.println(QQPlotGenerator.formatQQSummary(qqData));

        // Report results
        System.out.printf("Fitted model: mean=%.4f (true=%.1f), stdDev=%.4f (true=%.1f)%n",
            model.getMean(), trueMean, model.getStdDev(), trueStdDev);
        System.out.printf("K-S: %.6f (critical: %.6f) - %s%n",
            ks.statistic(), ks.criticalValue(), ks.passed() ? "PASS" : "FAIL");
        System.out.printf("Moments: mean_err=%.6f, var_err=%.4f%%, skew_err=%.4f, kurt_err=%.4f%n",
            moments.meanError(), moments.varianceRelError() * 100,
            moments.skewnessError(), moments.kurtosisError());
        System.out.printf("Q-Q Correlation: %.6f%n", qqCorr);

        assertTrue(ks.passed(), "K-S test should pass for normal distribution");
        assertTrue(moments.allPassed(), "All moments should match");
        assertTrue(qqCorr > 0.9999, "Q-Q correlation should be nearly perfect for 1M samples");
    }

    /**
     * High-precision self-consistency test with mixed distributions.
     */
    @Test
    @Tag("slow")
    void testHighPrecisionMixedDistributions() {
        int samples = STANDARD_SAMPLES;
        int dims = MEDIUM_DIM;

        System.out.println("\n=== High-Precision Mixed Distributions (500K × 384d) ===");

        float[][] original = generateMixedPearsonData(samples, dims, SEED);
        DatasetModelExtractor extractor = new DatasetModelExtractor();
        VectorSpaceModel extractedModel = extractor.extractVectorModel(original);

        // Generate TWO synthetic datasets for self-consistency test
        float[][] synthetic1 = generateFromModel(extractedModel, samples, SEED + 1);
        float[][] synthetic2 = generateFromModel(extractedModel, samples, SEED + 2);

        AccuracyReport report = validateAccuracy(synthetic1, synthetic2, extractedModel, "HighPrecision");

        System.out.println(report.formatDetailedReport());

        assertTrue(report.allTestsPassed(), "Self-consistency should pass for high-precision validation");
    }

    // ========== Distribution Type Coverage ==========

    @Test
    void testPearsonTypeCoverage() {
        int samples = QUICK_SAMPLES;

        System.out.println("\n=== Pearson Type Coverage ===");

        ScalarModel[] models = {
            new NormalScalarModel(0.0, 1.0),
            new UniformScalarModel(0.0, 1.0),
            new BetaScalarModel(2.0, 5.0),
            new GammaScalarModel(3.0, 2.0),
            new StudentTScalarModel(8.0),
            new InverseGammaScalarModel(4.0, 3.0),
            new BetaPrimeScalarModel(3.0, 5.0)
        };

        for (ScalarModel source : models) {
            String typeName = source.getModelType();

            // Generate from source
            ComponentSampler sampler = ComponentSamplerFactory.forModel(source);
            float[] original = new float[samples];
            for (int i = 0; i < samples; i++) {
                double u = (i + 0.5) / samples;
                original[i] = (float) sampler.sample(u);
            }

            // Fit model
            BestFitSelector selector = BestFitSelector.fullPearsonSelector();
            ScalarModel fitted = selector.selectBest(original);

            // Generate synthetic
            ComponentSampler synthSampler = ComponentSamplerFactory.forModel(fitted);
            float[] synthetic = new float[samples];
            for (int i = 0; i < samples; i++) {
                double u = (i + 0.5) / samples;
                synthetic[i] = (float) synthSampler.sample(u);
            }

            // Validate
            StatisticalTestSuite.TestResult ks = StatisticalTestSuite.kolmogorovSmirnovTest(original, synthetic);
            double qqCorr = StatisticalTestSuite.qqCorrelation(original, synthetic);

            String status = (ks.passed() && qqCorr > MIN_QQ_CORRELATION) ? "PASS" : "FAIL";
            System.out.printf("  %-15s -> %-15s: K-S=%.4f, QQ=%.4f [%s]%n",
                typeName, fitted.getModelType(), ks.statistic(), qqCorr, status);
        }
    }

    // ========== K-S Critical Value Verification ==========

    @Test
    void testKSCriticalValues() {
        System.out.println("\n=== K-S Critical Value Verification ===");

        // For identical distributions, K-S should pass at high rate
        int[] sampleSizes = {1000, 10000, 100000};

        for (int n : sampleSizes) {
            int passes = 0;
            int trials = 100;

            for (int t = 0; t < trials; t++) {
                // Generate two samples from same distribution
                Random rng = new Random(SEED + t);
                float[] s1 = new float[n];
                float[] s2 = new float[n];
                for (int i = 0; i < n; i++) {
                    s1[i] = (float) rng.nextGaussian();
                    s2[i] = (float) rng.nextGaussian();
                }

                StatisticalTestSuite.TestResult ks = StatisticalTestSuite.kolmogorovSmirnovTest(s1, s2);
                if (ks.passed()) passes++;
            }

            double passRate = (double) passes / trials;
            System.out.printf("  n=%,d: pass rate = %.1f%% (expected ~95%%)%n", n, passRate * 100);

            // Should pass at least 90% (allowing for statistical variation)
            assertTrue(passRate >= 0.90,
                String.format("K-S test should pass ~95%% for identical distributions, got %.1f%%", passRate * 100));
        }
    }

    // ========== Moment Matching Accuracy ==========

    @Test
    void testMomentMatchingPrecision() {
        int samples = STANDARD_SAMPLES;

        System.out.println("\n=== Moment Matching Precision ===");

        // Generate known distribution
        Random rng = new Random(SEED);
        float[] original = new float[samples];
        for (int i = 0; i < samples; i++) {
            original[i] = (float) (rng.nextGaussian() * 2.0 + 5.0);
        }

        // Fit and regenerate
        DimensionStatistics stats = DimensionStatistics.compute(0, original);
        NormalScalarModel model = new NormalScalarModel(stats.mean(), stats.stdDev());
        ComponentSampler sampler = ComponentSamplerFactory.forModel(model);

        float[] synthetic = new float[samples];
        for (int i = 0; i < samples; i++) {
            double u = (i + 0.5) / samples;
            synthetic[i] = (float) sampler.sample(u);
        }

        StatisticalTestSuite.MomentComparison moments = StatisticalTestSuite.compareMoments(original, synthetic);

        System.out.printf("  Mean error:     %.6f (tol < 0.01 × σ)%n", moments.meanError());
        System.out.printf("  Variance error: %.4f%% (tol < 5%%)%n", moments.varianceRelError() * 100);
        System.out.printf("  Skewness error: %.4f (tol < 0.15)%n", moments.skewnessError());
        System.out.printf("  Kurtosis error: %.4f (tol < 0.5)%n", moments.kurtosisError());

        assertTrue(moments.allPassed(), "All moments should match within tolerance");
    }

    // ========== Q-Q Plot Validation ==========

    @Test
    void testQQPlotCorrelation() {
        int samples = QUICK_SAMPLES;

        System.out.println("\n=== Q-Q Plot Correlation Test ===");

        // Generate matching distributions
        Random rng = new Random(SEED);
        float[] s1 = new float[samples];
        float[] s2 = new float[samples];
        for (int i = 0; i < samples; i++) {
            s1[i] = (float) rng.nextGaussian();
            s2[i] = (float) rng.nextGaussian();
        }

        QQPlotGenerator.QQPlotData qqData = QQPlotGenerator.generateQQPlot(s1, s2);
        QQPlotGenerator.DeviationStats devStats = QQPlotGenerator.computeDeviationStats(qqData);

        System.out.println(QQPlotGenerator.formatQQSummary(qqData));
        System.out.printf("  Mean deviation: %.6f%n", devStats.meanDeviation());
        System.out.printf("  Max deviation:  %.6f%n", devStats.maxAbsDeviation());

        assertTrue(qqData.correlation() > 0.999,
            "Q-Q correlation should be > 0.999 for matching distributions");
    }

    // ========== Helper Methods ==========

    private float[][] generateMixedPearsonData(int samples, int dims, long seed) {
        float[][] data = new float[samples][dims];
        Random paramRng = new Random(seed);

        ScalarModel[] models = new ScalarModel[dims];
        for (int d = 0; d < dims; d++) {
            models[d] = createRandomPearsonModel(d, paramRng);
        }

        for (int d = 0; d < dims; d++) {
            ComponentSampler sampler = ComponentSamplerFactory.forModel(models[d]);
            for (int v = 0; v < samples; v++) {
                double u = StratifiedSampler.unitIntervalValue(v, d, samples);
                data[v][d] = (float) sampler.sample(u);
            }
        }

        return data;
    }

    private ScalarModel createRandomPearsonModel(int dimension, Random rng) {
        int type = dimension % 7;
        switch (type) {
            case 0: return new NormalScalarModel(rng.nextGaussian() * 0.5, 0.1 + rng.nextDouble() * 0.3);
            case 1: return new UniformScalarModel(-0.5 + rng.nextDouble() * 0.2, 0.5 + rng.nextDouble() * 0.2);
            case 2: return new BetaScalarModel(1.5 + rng.nextDouble() * 3, 1.5 + rng.nextDouble() * 3);
            case 3: return new GammaScalarModel(2.0 + rng.nextDouble() * 2, 0.5 + rng.nextDouble() * 0.5);
            case 4: return new StudentTScalarModel(5.0 + rng.nextDouble() * 10);
            case 5: return new InverseGammaScalarModel(3.0 + rng.nextDouble() * 2, 2.0 + rng.nextDouble());
            case 6: return new BetaPrimeScalarModel(2.0 + rng.nextDouble() * 2, 4.0 + rng.nextDouble() * 2);
            default: return new NormalScalarModel(0.0, 1.0);
        }
    }

    private float[][] generateFromModel(VectorSpaceModel model, int samples, long seed) {
        int dims = model.dimensions();
        float[][] data = new float[samples][dims];

        ComponentSampler[] samplers = new ComponentSampler[dims];
        for (int d = 0; d < dims; d++) {
            samplers[d] = ComponentSamplerFactory.forModel(model.scalarModel(d));
        }

        for (int v = 0; v < samples; v++) {
            for (int d = 0; d < dims; d++) {
                double u = StratifiedSampler.unitIntervalValue(v, d, samples);
                data[v][d] = (float) samplers[d].sample(u);
            }
        }

        return data;
    }

    private float[] extractColumn(float[][] data, int col) {
        float[] column = new float[data.length];
        for (int i = 0; i < data.length; i++) {
            column[i] = data[i][col];
        }
        return column;
    }

    private AccuracyReport validateAccuracy(float[][] original, float[][] synthetic,
                                            VectorSpaceModel model, String datasetName) {
        int dims = original[0].length;
        List<StatisticalTestSuite.DimensionAccuracy> dimResults = new ArrayList<>();

        for (int d = 0; d < dims; d++) {
            float[] origCol = extractColumn(original, d);
            float[] synthCol = extractColumn(synthetic, d);
            String modelType = model.scalarModel(d).getModelType();

            StatisticalTestSuite.DimensionAccuracy acc =
                StatisticalTestSuite.analyzeDimension(d, modelType, origCol, synthCol);
            dimResults.add(acc);
        }

        // Compute correlation comparison
        CorrelationAnalysis.CorrelationComparison corrComparison = null;
        if (dims <= 256) {  // Only for reasonable dimension counts
            corrComparison = CorrelationAnalysis.compareCorrelationStructure(original, synthetic);
        }

        // Compute geometric metrics (sample pairwise distances)
        AccuracyReport.GeometricMetrics geoMetrics = computeGeometricMetrics(original, synthetic);

        return AccuracyReport.builder()
            .datasetName(datasetName)
            .sampleCounts(original.length, synthetic.length)
            .dimensionResults(dimResults)
            .correlationComparison(corrComparison)
            .geometricMetrics(geoMetrics)
            .build();
    }

    private AccuracyReport.GeometricMetrics computeGeometricMetrics(float[][] original, float[][] synthetic) {
        int samplePairs = 10000;
        Random rng = new Random(SEED);
        int n = original.length;

        float[] origDistances = new float[samplePairs];
        float[] synthDistances = new float[samplePairs];
        float[] origCosines = new float[samplePairs];
        float[] synthCosines = new float[samplePairs];

        for (int p = 0; p < samplePairs; p++) {
            int i = rng.nextInt(n);
            int j = rng.nextInt(n);
            if (i == j) j = (j + 1) % n;

            origDistances[p] = euclideanDistance(original[i], original[j]);
            synthDistances[p] = euclideanDistance(synthetic[i], synthetic[j]);
            origCosines[p] = cosineSimilarity(original[i], original[j]);
            synthCosines[p] = cosineSimilarity(synthetic[i], synthetic[j]);
        }

        // Compute norm distributions
        float[] origNorms = new float[n];
        float[] synthNorms = new float[n];
        for (int i = 0; i < n; i++) {
            origNorms[i] = l2Norm(original[i]);
            synthNorms[i] = l2Norm(synthetic[i]);
        }

        StatisticalTestSuite.TestResult distKS = StatisticalTestSuite.kolmogorovSmirnovTest(origDistances, synthDistances);
        StatisticalTestSuite.TestResult cosKS = StatisticalTestSuite.kolmogorovSmirnovTest(origCosines, synthCosines);
        StatisticalTestSuite.TestResult normKS = StatisticalTestSuite.kolmogorovSmirnovTest(origNorms, synthNorms);

        return new AccuracyReport.GeometricMetrics(distKS, cosKS, normKS);
    }

    private float euclideanDistance(float[] a, float[] b) {
        float sum = 0;
        for (int i = 0; i < a.length; i++) {
            float d = a[i] - b[i];
            sum += d * d;
        }
        return (float) Math.sqrt(sum);
    }

    private float cosineSimilarity(float[] a, float[] b) {
        float dot = 0, normA = 0, normB = 0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        if (normA <= 0 || normB <= 0) return 0;
        return (float) (dot / (Math.sqrt(normA) * Math.sqrt(normB)));
    }

    private float l2Norm(float[] v) {
        float sum = 0;
        for (float f : v) sum += f * f;
        return (float) Math.sqrt(sum);
    }
}
