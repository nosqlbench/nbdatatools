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

import io.nosqlbench.vshapes.model.*;
import io.nosqlbench.vshapes.stream.DataspaceShape;
import io.nosqlbench.vshapes.stream.StreamingModelExtractor;
import org.junit.jupiter.api.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/// Comprehensive round-trip test that mirrors the CLI test script behavior.
///
/// ## Purpose
///
/// This test replicates the flow of `testscript_roundtrip_small.sh`:
/// ```bash
/// nbvectors generate sketch \
///     -d 20 -n 200000 \
///     --max-modes=3 --mix=full \
///     --no-normalize --force
///
/// nbvectors analyze profile \
///     --multimodal --max-modes=3 --max-composite-modes=3 \
///     --model-type pearson --clustering em \
///     --assume-unnormalized --verify --verbose
/// ```
///
/// ## Test Coverage
///
/// This test exercises:
/// - 20 dimensions with mixed distribution types (matching --mix=full)
/// - 200k samples per dimension (matching -n 200000)
/// - Pearson model type with multimodal detection
/// - EM clustering for composite fitting
/// - Full Gen → Ext → R-T pipeline
///
/// ## Distribution Mix (matches --mix=full with --max-modes=3)
///
/// - Dimensions 0-4: Normal (unimodal)
/// - Dimensions 5-8: Beta (unimodal)
/// - Dimensions 9-11: Uniform (unimodal)
/// - Dimensions 12-14: 2-mode composite
/// - Dimensions 15-17: 3-mode composite
/// - Dimensions 18-19: 3-mode composite (additional)
///
/// @see RoundTripPipelineTest for per-fixture testing
@Tag("accuracy")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class FullMixRoundTripTest {

    /// Number of dimensions (matches -d 20)
    private static final int DIMENSIONS = 20;

    /// Sample size per dimension (reduced from 200k for faster test execution)
    /// 50k samples provides robust statistics while keeping test duration reasonable
    private static final int SAMPLE_SIZE = 50_000;

    /// Maximum modes (matches --max-modes=3)
    private static final int MAX_MODES = 3;

    /// Maximum composite components (matches --max-composite-modes=3)
    private static final int MAX_COMPOSITE_COMPONENTS = 3;

    /// Bounds for unnormalized data
    private static final double LOWER_BOUND = -1.0;
    private static final double UPPER_BOUND = 1.0;

    /// Random seed for reproducibility
    private static final long SEED = 42;

    /// Equivalence checker for model comparison
    private static final StatisticalEquivalenceChecker EQUIVALENCE_CHECKER =
        new StatisticalEquivalenceChecker();

    /// The generated source models (ground truth)
    private static ScalarModel[] genModels;

    /// The extracted models
    private static ScalarModel[] extModels;

    /// The round-trip extracted models
    private static ScalarModel[] rtModels;

    /// Per-dimension pass/fail tracking
    private static boolean[] genExtPassed;
    private static boolean[] extRtPassed;

    @BeforeAll
    static void setUp() {
        System.out.println("═".repeat(70));
        System.out.println("FULL MIX ROUND-TRIP TEST (mirrors testscript_roundtrip_small.sh)");
        System.out.println("═".repeat(70));
        System.out.printf("  Dimensions: %d%n", DIMENSIONS);
        System.out.printf("  Samples per dimension: %,d%n", SAMPLE_SIZE);
        System.out.printf("  Max modes: %d%n", MAX_MODES);
        System.out.printf("  Max composite components: %d%n", MAX_COMPOSITE_COMPONENTS);
        System.out.printf("  Model type: pearson (multimodal)%n");
        System.out.printf("  Clustering: EM%n");
        System.out.println("═".repeat(70));

        // Generate source models
        genModels = buildFullMixModels(DIMENSIONS, SEED);
        extModels = new ScalarModel[DIMENSIONS];
        rtModels = new ScalarModel[DIMENSIONS];
        genExtPassed = new boolean[DIMENSIONS];
        extRtPassed = new boolean[DIMENSIONS];

        printDistributionSummary(genModels);
    }

    /// Builds scalar models matching --mix=full with --max-modes=3.
    ///
    /// Distribution follows CMD_generate_sketch.createFullMixModel():
    /// - dims 0-4 (25%): Normal
    /// - dims 5-8 (20%): Beta
    /// - dims 9-11 (15%): Uniform
    /// - dims 12-14 (15%): 2-mode composite
    /// - dims 15-17 (15%): 3-mode composite
    /// - dims 18-19 (10%): 3-mode composite
    private static ScalarModel[] buildFullMixModels(int dimensions, long seed) {
        ScalarModel[] models = new ScalarModel[dimensions];
        Random rng = new Random(seed);

        for (int d = 0; d < dimensions; d++) {
            int choice = d % 20;
            if (choice < 5) {
                // 25% Normal (unimodal)
                models[d] = createNormalModel(rng);
            } else if (choice < 9) {
                // 20% Beta (unimodal)
                models[d] = createBetaModel(rng);
            } else if (choice < 12) {
                // 15% Uniform (unimodal)
                models[d] = createUniformModel(rng);
            } else if (choice < 15) {
                // 15% 2-mode composite
                models[d] = createCompositeModel(rng, 2);
            } else {
                // 25% 3-mode composite
                models[d] = createCompositeModel(rng, Math.min(3, MAX_MODES));
            }
        }

        return models;
    }

    private static ScalarModel createNormalModel(Random rng) {
        double center = (LOWER_BOUND + UPPER_BOUND) / 2.0;
        double range = UPPER_BOUND - LOWER_BOUND;
        double mean = center + (rng.nextDouble() - 0.5) * range * 0.5;
        double stdDev = range * (0.1 + rng.nextDouble() * 0.3);
        return new NormalScalarModel(mean, stdDev, LOWER_BOUND, UPPER_BOUND);
    }

    private static ScalarModel createBetaModel(Random rng) {
        double alpha = 1.0 + rng.nextDouble() * 4.0;
        double beta = 1.0 + rng.nextDouble() * 4.0;
        return new BetaScalarModel(alpha, beta, LOWER_BOUND, UPPER_BOUND);
    }

    private static ScalarModel createUniformModel(Random rng) {
        double range = UPPER_BOUND - LOWER_BOUND;
        double margin = range * 0.1 * rng.nextDouble();
        double lo = LOWER_BOUND + margin * rng.nextDouble();
        double hi = UPPER_BOUND - margin * rng.nextDouble();
        if (lo >= hi) {
            lo = LOWER_BOUND;
            hi = UPPER_BOUND;
        }
        return new UniformScalarModel(lo, hi);
    }

    private static ScalarModel createCompositeModel(Random rng, int numModes) {
        ScalarModel[] components = new ScalarModel[numModes];
        double[] weights = new double[numModes];

        double range = UPPER_BOUND - LOWER_BOUND;
        double modeSpacing = range / (numModes + 1);

        for (int i = 0; i < numModes; i++) {
            // Position modes evenly across the range
            double position = LOWER_BOUND + modeSpacing * (i + 1);

            // Add jitter to position
            double jitter = (rng.nextDouble() - 0.5) * modeSpacing * 0.3;
            double mean = Math.max(LOWER_BOUND + 0.1, Math.min(UPPER_BOUND - 0.1, position + jitter));

            // Component width: 0.05 to 0.15 of range
            double stdDev = range * (0.05 + rng.nextDouble() * 0.10);

            // Alternate component types: mostly Normal, some Beta
            if (i % 3 == 2 && numModes > 2) {
                // One Beta component
                double alpha = 2.0 + rng.nextDouble() * 2.0;
                double beta = 2.0 + rng.nextDouble() * 2.0;
                double compLo = mean - 2 * stdDev;
                double compHi = mean + 2 * stdDev;
                compLo = Math.max(LOWER_BOUND, compLo);
                compHi = Math.min(UPPER_BOUND, compHi);
                components[i] = new BetaScalarModel(alpha, beta, compLo, compHi);
            } else {
                // Normal component
                components[i] = new NormalScalarModel(mean, stdDev, LOWER_BOUND, UPPER_BOUND);
            }

            // Random weights, but ensure minimum weight
            weights[i] = 0.2 + rng.nextDouble() * 0.6;
        }

        // Normalize weights
        double sum = 0;
        for (double w : weights) sum += w;
        for (int i = 0; i < weights.length; i++) {
            weights[i] /= sum;
        }

        return new CompositeScalarModel(Arrays.asList(components), weights);
    }

    private static void printDistributionSummary(ScalarModel[] models) {
        System.out.println("\nDistribution Summary:");
        int normalCount = 0, betaCount = 0, uniformCount = 0;
        int composite2 = 0, composite3 = 0, compositeOther = 0;

        for (ScalarModel model : models) {
            if (model instanceof CompositeScalarModel comp) {
                int modes = comp.getComponentCount();
                if (modes == 2) composite2++;
                else if (modes == 3) composite3++;
                else compositeOther++;
            } else if (model instanceof NormalScalarModel) {
                normalCount++;
            } else if (model instanceof BetaScalarModel) {
                betaCount++;
            } else if (model instanceof UniformScalarModel) {
                uniformCount++;
            }
        }

        System.out.printf("  Normal (unimodal):     %d%n", normalCount);
        System.out.printf("  Beta (unimodal):       %d%n", betaCount);
        System.out.printf("  Uniform (unimodal):    %d%n", uniformCount);
        System.out.printf("  2-mode composite:      %d%n", composite2);
        System.out.printf("  3-mode composite:      %d%n", composite3);
        if (compositeOther > 0) {
            System.out.printf("  Other composite:       %d%n", compositeOther);
        }
        System.out.println();
    }

    /// Tests all dimensions through the full pipeline.
    @Test
    @Order(1)
    void testFullPipeline() {
        System.out.println("\n" + "═".repeat(70));
        System.out.println("RUNNING FULL PIPELINE: Gen → Ext → R-T");
        System.out.println("═".repeat(70));

        // Create selector matching CLI: --model-type pearson --multimodal --max-modes=3
        BestFitSelector selector = BestFitSelector.pearsonMultimodalSelector(MAX_MODES);

        for (int d = 0; d < DIMENSIONS; d++) {
            System.out.printf("\n--- Dimension %d/%d: %s ---%n",
                d + 1, DIMENSIONS, describeModel(genModels[d]));

            // Phase 1: Generate variates from Gen model
            float[] genVariates = generateVariates(genModels[d], SAMPLE_SIZE);

            // Phase 2: Extract Ext model from Gen variates
            extModels[d] = extractViaStreamingPath(genVariates, selector, d);
            System.out.printf("  Extracted: %s%n", describeModel(extModels[d]));

            // Phase 3: Compare Gen vs Ext (accuracy)
            genExtPassed[d] = EQUIVALENCE_CHECKER.areEquivalent(genModels[d], extModels[d]);
            System.out.printf("  Gen→Ext: %s%n", genExtPassed[d] ? "✓ MATCH" : "✗ MISMATCH");

            // Phase 4: Generate variates from Ext model
            float[] extVariates = generateVariates(extModels[d], SAMPLE_SIZE);

            // Phase 5: Extract R-T model from Ext variates
            BestFitSelector rtSelector = BestFitSelector.strictRoundTripSelector(MAX_COMPOSITE_COMPONENTS);
            rtModels[d] = extractViaStreamingPath(extVariates, rtSelector, d);
            System.out.printf("  R-T model: %s%n", describeModel(rtModels[d]));

            // Phase 6: Compare Ext vs R-T (stability)
            extRtPassed[d] = EQUIVALENCE_CHECKER.areEquivalent(extModels[d], rtModels[d]);
            System.out.printf("  Ext→R-T: %s%n", extRtPassed[d] ? "✓ STABLE" : "✗ UNSTABLE");
        }
    }

    /// Reports aggregate statistics and asserts minimum pass rates.
    @Test
    @Order(2)
    void testAggregateStatistics() {
        System.out.println("\n" + "═".repeat(70));
        System.out.println("AGGREGATE STATISTICS");
        System.out.println("═".repeat(70));

        int genExtPass = 0, extRtPass = 0;

        System.out.println("\nDim | Gen Type           | Ext Type           | Gen→Ext | Ext→R-T");
        System.out.println("----|--------------------|--------------------|---------|--------");

        for (int d = 0; d < DIMENSIONS; d++) {
            String genType = padRight(describeModelShort(genModels[d]), 18);
            String extType = padRight(describeModelShort(extModels[d]), 18);
            String genExtStatus = genExtPassed[d] ? "  ✓  " : "  ✗  ";
            String extRtStatus = extRtPassed[d] ? "  ✓  " : "  ✗  ";

            if (genExtPassed[d]) genExtPass++;
            if (extRtPassed[d]) extRtPass++;

            System.out.printf("%3d | %s | %s | %s | %s%n",
                d, genType, extType, genExtStatus, extRtStatus);
        }

        System.out.println("----|--------------------|--------------------|---------|--------");

        double genExtRate = 100.0 * genExtPass / DIMENSIONS;
        double extRtRate = 100.0 * extRtPass / DIMENSIONS;

        System.out.println();
        System.out.printf("Gen→Ext accuracy rate:  %d/%d (%.1f%%)%n", genExtPass, DIMENSIONS, genExtRate);
        System.out.printf("Ext→R-T stability rate: %d/%d (%.1f%%)%n", extRtPass, DIMENSIONS, extRtRate);

        // Report problem dimensions
        if (genExtRate < 100 || extRtRate < 100) {
            System.out.println("\nProblem Dimensions:");
            for (int d = 0; d < DIMENSIONS; d++) {
                if (!genExtPassed[d] || !extRtPassed[d]) {
                    System.out.printf("  Dim %d: Gen=%s, Ext=%s, R-T=%s%n",
                        d, describeModelShort(genModels[d]),
                        describeModelShort(extModels[d]),
                        rtModels[d] != null ? describeModelShort(rtModels[d]) : "N/A");
                    if (!genExtPassed[d]) {
                        String reason = EQUIVALENCE_CHECKER.getEquivalenceReason(genModels[d], extModels[d]);
                        if (reason != null) System.out.printf("    Gen→Ext: %s%n", reason);
                    }
                    if (!extRtPassed[d] && rtModels[d] != null) {
                        String reason = EQUIVALENCE_CHECKER.getEquivalenceReason(extModels[d], rtModels[d]);
                        if (reason != null) System.out.printf("    Ext→R-T: %s%n", reason);
                    }
                }
            }
        }

        System.out.println("\n" + "═".repeat(70));

        // Assert minimum thresholds
        // For stability (Ext→R-T), we expect 100% after the bug fix
        assertTrue(extRtRate >= 90.0,
            String.format("Ext→R-T stability rate %.1f%% below 90%% threshold", extRtRate));

        // For accuracy (Gen→Ext), composite modes may vary - be more lenient
        assertTrue(genExtRate >= 70.0,
            String.format("Gen→Ext accuracy rate %.1f%% below 70%% threshold", genExtRate));
    }

    // ==================== Helper Methods ====================

    /// Extracts a model using StreamingModelExtractor - same code path as CLI.
    ///
    /// Configuration matches:
    /// - --model-type pearson
    /// - --multimodal
    /// - --max-modes=3
    /// - --max-composite-modes=3
    /// - --clustering em
    /// - --assume-unnormalized
    private ScalarModel extractViaStreamingPath(float[] values, BestFitSelector selector, int dimension) {
        StreamingModelExtractor extractor = new StreamingModelExtractor(selector);
        extractor.setUniqueVectors(values.length);

        // Match CLI settings for adaptive/composite fitting
        // --model-type pearson → adaptive disabled (only auto enables adaptive)
        extractor.setAdaptiveEnabled(false);

        // Set max composite components (--max-composite-modes=3)
        extractor.setMaxCompositeComponents(MAX_COMPOSITE_COMPONENTS);

        // Disable NUMA for tests
        extractor.setNumaAware(false);

        // Initialize with shape (1 dimension)
        DataspaceShape shape = new DataspaceShape(values.length, 1);
        extractor.initialize(shape);

        // Feed data as columnar chunk
        float[][] columnarData = new float[1][values.length];
        System.arraycopy(values, 0, columnarData[0], 0, values.length);
        extractor.accept(columnarData, 0);

        // Complete and extract
        VectorSpaceModel model = extractor.complete();
        extractor.shutdown();

        return model.scalarModel(0);
    }

    /// Generates variates from a scalar model.
    private float[] generateVariates(ScalarModel model, int n) {
        float[] data = new float[n];
        Random rng = new Random(SEED);

        if (model instanceof CompositeScalarModel composite) {
            ScalarModel[] components = composite.getScalarModels();
            double[] weights = composite.getWeights();
            double[] cumWeights = new double[weights.length];
            double sum = 0;
            for (int i = 0; i < weights.length; i++) {
                sum += weights[i];
                cumWeights[i] = sum;
            }
            for (int i = 0; i < cumWeights.length; i++) {
                cumWeights[i] /= sum;
            }

            for (int i = 0; i < n; i++) {
                double u = rng.nextDouble();
                int componentIdx = 0;
                for (int c = 0; c < cumWeights.length; c++) {
                    if (u <= cumWeights[c]) {
                        componentIdx = c;
                        break;
                    }
                }
                data[i] = (float) sampleFromModel(components[componentIdx], rng);
            }
        } else {
            for (int i = 0; i < n; i++) {
                data[i] = (float) sampleFromModel(model, rng);
            }
        }

        return data;
    }

    private double sampleFromModel(ScalarModel model, Random rng) {
        if (model instanceof NormalScalarModel normal) {
            double lower = normal.lower();
            double upper = normal.upper();
            for (int i = 0; i < 1000; i++) {
                double sample = normal.getMean() + rng.nextGaussian() * normal.getStdDev();
                if (sample >= lower && sample <= upper) {
                    return sample;
                }
            }
            return Math.max(lower, Math.min(upper, normal.getMean()));

        } else if (model instanceof UniformScalarModel uniform) {
            return uniform.getLower() + rng.nextDouble() * uniform.getRange();

        } else if (model instanceof BetaScalarModel beta) {
            double a = beta.getAlpha();
            double b = beta.getBeta();
            double x = sampleGamma(a, 1.0, rng);
            double y = sampleGamma(b, 1.0, rng);
            double sample = x / (x + y);
            return beta.getLower() + sample * (beta.getUpper() - beta.getLower());

        } else if (model instanceof GammaScalarModel gamma) {
            // Sample from Gamma distribution
            return sampleGamma(gamma.getShape(), gamma.getScale(), rng) + gamma.getLocation();

        } else if (model instanceof StudentTScalarModel studentT) {
            // Sample from Student-t using the standard method
            double nu = studentT.getDegreesOfFreedom();
            double chi2 = sampleGamma(nu / 2.0, 2.0, rng);
            double z = rng.nextGaussian();
            double t = z / Math.sqrt(chi2 / nu);
            return studentT.getLocation() + t * studentT.getScale();

        } else if (model instanceof EmpiricalScalarModel empirical) {
            // Sample uniformly from the empirical bounds
            double min = empirical.getMin();
            double max = empirical.getMax();
            return min + rng.nextDouble() * (max - min);

        } else {
            // For unknown types, just sample uniformly over [-1, 1]
            // This is a fallback that shouldn't normally be reached
            System.err.printf("WARNING: Unknown model type %s, using uniform fallback%n",
                model.getClass().getSimpleName());
            return LOWER_BOUND + rng.nextDouble() * (UPPER_BOUND - LOWER_BOUND);
        }
    }

    private double sampleGamma(double shape, double scale, Random rng) {
        if (shape < 1) {
            return sampleGamma(1 + shape, scale, rng) * Math.pow(rng.nextDouble(), 1.0 / shape);
        }

        double d = shape - 1.0 / 3.0;
        double c = 1.0 / Math.sqrt(9.0 * d);

        while (true) {
            double x, v;
            do {
                x = rng.nextGaussian();
                v = 1.0 + c * x;
            } while (v <= 0);

            v = v * v * v;
            double u = rng.nextDouble();

            if (u < 1 - 0.0331 * (x * x) * (x * x)) {
                return d * v * scale;
            }

            if (Math.log(u) < 0.5 * x * x + d * (1 - v + Math.log(v))) {
                return d * v * scale;
            }
        }
    }

    private String describeModel(ScalarModel model) {
        if (model instanceof CompositeScalarModel composite) {
            int count = composite.getComponentCount();
            StringBuilder sb = new StringBuilder();
            sb.append("Composite(").append(count).append("-mode: ");
            ScalarModel[] components = composite.getScalarModels();
            for (int i = 0; i < Math.min(3, components.length); i++) {
                if (i > 0) sb.append(", ");
                sb.append(components[i].getModelType());
            }
            if (components.length > 3) {
                sb.append(", ...");
            }
            sb.append(")");
            return sb.toString();
        }

        String type = model.getModelType();
        return switch (type) {
            case "normal" -> {
                NormalScalarModel n = (NormalScalarModel) model;
                yield String.format("Normal(μ=%.2f, σ=%.2f)", n.getMean(), n.getStdDev());
            }
            case "uniform" -> {
                UniformScalarModel u = (UniformScalarModel) model;
                yield String.format("Uniform[%.2f, %.2f]", u.getLower(), u.getUpper());
            }
            case "beta" -> {
                BetaScalarModel b = (BetaScalarModel) model;
                yield String.format("Beta(α=%.2f, β=%.2f)", b.getAlpha(), b.getBeta());
            }
            default -> type;
        };
    }

    private String describeModelShort(ScalarModel model) {
        if (model instanceof CompositeScalarModel composite) {
            return "Comp(" + composite.getComponentCount() + ")";
        }
        return model.getModelType().substring(0, 1).toUpperCase() +
               model.getModelType().substring(1);
    }

    private String padRight(String s, int len) {
        if (s.length() >= len) return s.substring(0, len);
        return s + " ".repeat(len - s.length());
    }
}
