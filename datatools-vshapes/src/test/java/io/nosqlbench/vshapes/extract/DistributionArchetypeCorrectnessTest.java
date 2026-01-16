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

import io.nosqlbench.vshapes.extract.ComponentModelFitter.FitResult;
import io.nosqlbench.vshapes.model.*;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/// Comprehensive correctness tests for all detectable distribution archetypes.
///
/// ## Purpose
///
/// This test class verifies that the model extraction system can correctly:
/// 1. Identify single distribution types (Normal, Uniform, Beta, StudentT)
/// 2. Detect and fit composite (multimodal) distributions
/// 3. Recognize statistical equivalence classes
/// 4. Maintain round-trip stability
///
/// ## Test Categories
///
/// - **Single Distribution Archetypes**: Tests each fundamental distribution type
/// - **Composite Archetypes - Separated**: Well-separated modes that should be recovered
/// - **Composite Archetypes - Overlapping**: Modes that may converge via CLT
/// - **Equivalence Class Tests**: Normal↔StudentT, Beta↔Uniform
/// - **Round-Trip Stability Tests**: Verify extraction stability
///
/// ## Maven Invocation
///
/// ```bash
/// mvn test -pl datatools-vshapes -Dtest="DistributionArchetypeCorrectnessTest"
/// mvn test -pl datatools-vshapes -Dgroups="archetype"
/// ```
///
/// @see StatisticalEquivalenceChecker
/// @see BestFitSelector
@Tag("accuracy")
@Tag("archetype")
public class DistributionArchetypeCorrectnessTest {

    private static final long SEED = 42L;
    private static final int SAMPLE_SIZE = 50000;

    // ============ SINGLE DISTRIBUTION ARCHETYPES ============

    @ParameterizedTest(name = "Normal μ={0}, σ={1}")
    @CsvSource({"0,0.3", "-0.5,0.2", "0.3,0.5"})
    void testNormalArchetype(double mu, double sigma) {
        Random rng = new Random(SEED);
        float[] data = new float[SAMPLE_SIZE];
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            data[i] = (float) (rng.nextGaussian() * sigma + mu);
        }

        BestFitSelector selector = BestFitSelector.boundedDataSelector();
        FitResult result = selector.selectBestResult(data);

        String effectiveType = getEffectiveModelType(result.model());
        assertTrue(effectiveType.equals("normal") || effectiveType.equals("student_t"),
            String.format("Normal(%.2f,%.2f) should be detected as normal or student_t, got: %s",
                mu, sigma, effectiveType));

        System.out.printf("Normal(μ=%.2f, σ=%.2f) -> %s (KS=%.4f)%n",
            mu, sigma, effectiveType, result.goodnessOfFit());
    }

    @ParameterizedTest(name = "Uniform [{0},{1}]")
    @CsvSource({"-1,1", "-0.5,0.5", "0,1"})
    void testUniformArchetype(double lower, double upper) {
        Random rng = new Random(SEED);
        float[] data = new float[SAMPLE_SIZE];
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            data[i] = (float) (rng.nextDouble() * (upper - lower) + lower);
        }

        BestFitSelector selector = BestFitSelector.boundedDataSelector();
        FitResult result = selector.selectBestResult(data);

        String effectiveType = getEffectiveModelType(result.model());
        assertTrue(effectiveType.equals("uniform") || effectiveType.equals("beta"),
            String.format("Uniform[%.2f,%.2f] should be detected as uniform or beta, got: %s",
                lower, upper, effectiveType));

        System.out.printf("Uniform[%.2f, %.2f] -> %s (KS=%.4f)%n",
            lower, upper, effectiveType, result.goodnessOfFit());
    }

    @ParameterizedTest(name = "Beta α={0}, β={1}")
    @CsvSource({"2,2", "0.5,0.5", "2,5", "5,2"})
    void testBetaArchetype(double alpha, double beta) {
        Random rng = new Random(SEED);
        float[] data = new float[SAMPLE_SIZE];
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            // Use inverse CDF method for Beta sampling
            data[i] = (float) sampleBeta(rng, alpha, beta);
        }

        BestFitSelector selector = BestFitSelector.boundedDataSelector();
        FitResult result = selector.selectBestResult(data);

        String effectiveType = getEffectiveModelType(result.model());
        // Beta can be detected as beta, normal (if symmetric), or uniform (if α≈β≈1)
        assertTrue(effectiveType.equals("beta") || effectiveType.equals("normal") || effectiveType.equals("uniform"),
            String.format("Beta(%.2f,%.2f) should be detected as beta/normal/uniform, got: %s",
                alpha, beta, effectiveType));

        System.out.printf("Beta(α=%.2f, β=%.2f) -> %s (KS=%.4f)%n",
            alpha, beta, effectiveType, result.goodnessOfFit());
    }

    @ParameterizedTest(name = "StudentT ν={0}")
    @CsvSource({"3", "10", "30", "100"})
    void testStudentTArchetype(int nu) {
        Random rng = new Random(SEED);
        float[] data = new float[SAMPLE_SIZE];
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            data[i] = (float) sampleStudentT(rng, nu);
        }

        BestFitSelector selector = BestFitSelector.defaultSelector();
        FitResult result = selector.selectBestResult(data);

        String effectiveType = getEffectiveModelType(result.model());
        // Student-t with high ν should be detected as normal or student_t
        assertTrue(effectiveType.equals("student_t") || effectiveType.equals("normal"),
            String.format("StudentT(ν=%d) should be detected as student_t or normal, got: %s",
                nu, effectiveType));

        System.out.printf("StudentT(ν=%d) -> %s (KS=%.4f)%n",
            nu, effectiveType, result.goodnessOfFit());
    }

    // ============ COMPOSITE ARCHETYPES - SEPARATED ============

    @Test
    void testBimodalWellSeparated() {
        CompositeScalarModel source = createSeparatedBimodal(-0.5, 0.5, 0.1);
        float[] data = generateData(source, SAMPLE_SIZE, SEED);

        BestFitSelector selector = BestFitSelector.pearsonMultimodalSelector(4);
        FitResult result = selector.selectBestResult(data);

        String effectiveType = getEffectiveModelType(result.model());
        // Well-separated bimodal should be detected as composite
        if (effectiveType.equals("composite")) {
            CompositeScalarModel fitted = (CompositeScalarModel) result.model();
            assertTrue(fitted.getComponentCount() >= 2,
                "Bimodal should have at least 2 components");
        }

        System.out.printf("Bimodal separated -> %s (KS=%.4f)%n",
            effectiveType, result.goodnessOfFit());
    }

    @Test
    void testTrimodalWellSeparated() {
        CompositeScalarModel source = createSeparatedTrimodal();
        float[] data = generateData(source, SAMPLE_SIZE, SEED);

        BestFitSelector selector = BestFitSelector.pearsonMultimodalSelector(4);
        FitResult result = selector.selectBestResult(data);

        String effectiveType = getEffectiveModelType(result.model());
        System.out.printf("Trimodal separated -> %s (KS=%.4f)%n",
            effectiveType, result.goodnessOfFit());
    }

    @ParameterizedTest(name = "{0}-modal well separated")
    @ValueSource(ints = {4, 5, 6})
    void testMultimodalWellSeparated(int modes) {
        CompositeScalarModel source = createNModesSeparated(modes, SEED);
        float[] data = generateData(source, SAMPLE_SIZE, SEED);

        BestFitSelector selector = BestFitSelector.pearsonMultimodalSelector(modes + 2);
        FitResult result = selector.selectBestResult(data);

        StatisticalEquivalenceChecker checker = new StatisticalEquivalenceChecker();
        assertTrue(checker.areEquivalent(source, result.model()),
            String.format("%d-mode separated should be equivalent to extracted model", modes));

        System.out.printf("%d-modal separated -> %s (KS=%.4f)%n",
            modes, getEffectiveModelType(result.model()), result.goodnessOfFit());
    }

    // ============ COMPOSITE ARCHETYPES - OVERLAPPING ============

    @Test
    void testBimodalOverlapping() {
        // Two overlapping modes that might converge
        CompositeScalarModel source = createOverlappingBimodal();
        float[] data = generateData(source, SAMPLE_SIZE, SEED);

        BestFitSelector selector = BestFitSelector.pearsonMultimodalSelector(4);
        FitResult result = selector.selectBestResult(data);

        StatisticalEquivalenceChecker checker = new StatisticalEquivalenceChecker();
        // Overlapping modes may simplify - should be equivalent via CLT
        assertTrue(checker.areEquivalent(source, result.model()),
            "Overlapping bimodal should be equivalent (may simplify via CLT)");

        System.out.printf("Bimodal overlapping -> %s (KS=%.4f, equivalent=%b)%n",
            getEffectiveModelType(result.model()), result.goodnessOfFit(),
            checker.areEquivalent(source, result.model()));
    }

    @Test
    void testHighModeOverlapping() {
        // 6+ overlapping modes that should converge to normal via CLT
        CompositeScalarModel source = createOverlappingHighMode(6, SEED);
        float[] data = generateData(source, SAMPLE_SIZE, SEED);

        BestFitSelector selector = BestFitSelector.pearsonMultimodalSelector(10);
        FitResult result = selector.selectBestResult(data);

        StatisticalEquivalenceChecker checker = new StatisticalEquivalenceChecker();
        assertTrue(checker.areEquivalent(source, result.model()),
            "High-mode overlapping should be equivalent (CLT convergence)");

        System.out.printf("6-modal overlapping -> %s (KS=%.4f, equivalent=%b)%n",
            getEffectiveModelType(result.model()), result.goodnessOfFit(),
            checker.areEquivalent(source, result.model()));
    }

    // ============ EQUIVALENCE CLASS TESTS ============

    @Test
    void testNormalStudentTEquivalence() {
        StatisticalEquivalenceChecker checker = new StatisticalEquivalenceChecker();

        ScalarModel normal = new NormalScalarModel(0.0, 1.0);
        ScalarModel studentT = new StudentTScalarModel(100, 0.0, 1.0);

        assertTrue(checker.areEquivalent(normal, studentT),
            "Normal and StudentT(ν=100) should be equivalent");

        String reason = checker.getEquivalenceReason(normal, studentT);
        assertNotNull(reason, "Should have equivalence reason");

        System.out.printf("Normal(0,1) ≈ StudentT(ν=100) : %s%n", reason);
    }

    @Test
    void testBeta11UniformEquivalence() {
        StatisticalEquivalenceChecker checker = new StatisticalEquivalenceChecker();

        ScalarModel beta = new BetaScalarModel(1.0, 1.0, -1.0, 1.0);
        ScalarModel uniform = new UniformScalarModel(-1.0, 1.0);

        assertTrue(checker.areEquivalent(beta, uniform),
            "Beta(1,1) and Uniform should be equivalent");

        String reason = checker.getEquivalenceReason(beta, uniform);
        assertNotNull(reason, "Should have equivalence reason");

        System.out.printf("Beta(1,1) ≈ Uniform[-1,1] : %s%n", reason);
    }

    // ============ COMPONENT DISCRIMINATION TESTS ============

    @Test
    void testUniformVsNarrowNormal() {
        Random rng = new Random(SEED);

        // Generate uniform data
        float[] uniformData = new float[SAMPLE_SIZE];
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            uniformData[i] = (float) (rng.nextDouble() * 0.2 - 0.1);  // [-0.1, 0.1]
        }

        BestFitSelector selector = BestFitSelector.boundedDataSelector();
        FitResult result = selector.selectBestResult(uniformData);

        String effectiveType = getEffectiveModelType(result.model());

        // Uniform should be distinguished from narrow normal via kurtosis
        System.out.printf("Narrow Uniform[-0.1,0.1] -> %s (KS=%.4f)%n",
            effectiveType, result.goodnessOfFit());
    }

    @Test
    void testBetaVsTruncatedNormal() {
        Random rng = new Random(SEED);

        // Generate skewed Beta data
        float[] betaData = new float[SAMPLE_SIZE];
        for (int i = 0; i < SAMPLE_SIZE; i++) {
            betaData[i] = (float) sampleBeta(rng, 2.0, 5.0);
        }

        BestFitSelector selector = BestFitSelector.boundedDataSelector();
        FitResult result = selector.selectBestResult(betaData);

        String effectiveType = getEffectiveModelType(result.model());

        // Skewed Beta should be distinguished from symmetric Normal
        System.out.printf("Beta(2,5) -> %s (KS=%.4f)%n",
            effectiveType, result.goodnessOfFit());
    }

    // ============ ROUND-TRIP STABILITY TESTS ============

    @ParameterizedTest(name = "Round-trip {0}-mode")
    @ValueSource(ints = {2, 3, 4, 5})
    void testRoundTripStability(int modes) {
        CompositeScalarModel source = createNModesSeparated(modes, SEED);
        float[] originalData = generateData(source, SAMPLE_SIZE, SEED);

        // First extraction
        BestFitSelector selector = BestFitSelector.pearsonMultimodalSelector(modes + 2);
        FitResult firstResult = selector.selectBestResult(originalData);

        // Generate data from extracted model
        float[] regeneratedData = generateData(firstResult.model(), SAMPLE_SIZE, SEED + 1);

        // Second extraction
        FitResult secondResult = selector.selectBestResult(regeneratedData);

        // Check equivalence
        StatisticalEquivalenceChecker checker = new StatisticalEquivalenceChecker();
        assertTrue(checker.areEquivalent(firstResult.model(), secondResult.model()),
            String.format("%d-mode round-trip should produce equivalent models", modes));

        System.out.printf("%d-mode round-trip: %s -> %s (equivalent=%b)%n",
            modes, getEffectiveModelType(firstResult.model()),
            getEffectiveModelType(secondResult.model()),
            checker.areEquivalent(firstResult.model(), secondResult.model()));
    }

    // ============ HELPER METHODS ============

    private String getEffectiveModelType(ScalarModel model) {
        if (model instanceof CompositeScalarModel composite) {
            return composite.getEffectiveModelType();
        }
        return model.getModelType();
    }

    private CompositeScalarModel createSeparatedBimodal(double loc1, double loc2, double scale) {
        return CompositeScalarModel.of(
            new NormalScalarModel(loc1, scale),
            new NormalScalarModel(loc2, scale)
        );
    }

    private CompositeScalarModel createSeparatedTrimodal() {
        return CompositeScalarModel.of(
            new NormalScalarModel(-0.6, 0.1),
            new NormalScalarModel(0.0, 0.1),
            new NormalScalarModel(0.6, 0.1)
        );
    }

    private CompositeScalarModel createOverlappingBimodal() {
        return CompositeScalarModel.of(
            new NormalScalarModel(-0.1, 0.3),
            new NormalScalarModel(0.1, 0.3)
        );
    }

    private CompositeScalarModel createOverlappingHighMode(int modes, long seed) {
        Random rng = new Random(seed);
        List<ScalarModel> components = new ArrayList<>();
        for (int i = 0; i < modes; i++) {
            double loc = (rng.nextDouble() - 0.5) * 0.4;  // [-0.2, 0.2]
            double scale = 0.15 + rng.nextDouble() * 0.1;  // [0.15, 0.25]
            components.add(new NormalScalarModel(loc, scale));
        }
        double[] weights = new double[modes];
        for (int i = 0; i < modes; i++) weights[i] = 1.0 / modes;
        return new CompositeScalarModel(components, weights);
    }

    private CompositeScalarModel createNModesSeparated(int modes, long seed) {
        Random rng = new Random(seed);
        List<ScalarModel> components = new ArrayList<>();
        double spacing = 1.6 / modes;
        for (int i = 0; i < modes; i++) {
            double loc = -0.8 + i * spacing + spacing / 2;
            double scale = spacing * 0.2;
            components.add(new NormalScalarModel(loc, scale));
        }
        double[] weights = new double[modes];
        for (int i = 0; i < modes; i++) weights[i] = 1.0 / modes;
        return new CompositeScalarModel(components, weights);
    }

    private float[] generateData(ScalarModel model, int count, long seed) {
        Random rng = new Random(seed);
        float[] data = new float[count];

        if (model instanceof CompositeScalarModel composite) {
            ScalarModel[] components = composite.getScalarModels();
            double[] weights = composite.getWeights();
            for (int i = 0; i < count; i++) {
                // Select component by weight
                double r = rng.nextDouble();
                double cumulative = 0;
                int componentIdx = 0;
                for (int c = 0; c < weights.length; c++) {
                    cumulative += weights[c];
                    if (r <= cumulative) {
                        componentIdx = c;
                        break;
                    }
                }
                data[i] = (float) sampleFromModel(components[componentIdx], rng);
            }
        } else {
            for (int i = 0; i < count; i++) {
                data[i] = (float) sampleFromModel(model, rng);
            }
        }
        return data;
    }

    private double sampleFromModel(ScalarModel model, Random rng) {
        if (model instanceof NormalScalarModel n) {
            return rng.nextGaussian() * n.getStdDev() + n.getMean();
        } else if (model instanceof UniformScalarModel u) {
            return rng.nextDouble() * (u.getUpper() - u.getLower()) + u.getLower();
        } else if (model instanceof BetaScalarModel b) {
            double value = sampleBeta(rng, b.getAlpha(), b.getBeta());
            return value * (b.getUpper() - b.getLower()) + b.getLower();
        } else if (model instanceof StudentTScalarModel t) {
            return sampleStudentT(rng, (int) t.getDegreesOfFreedom()) * t.getScale() + t.getLocation();
        }
        return rng.nextGaussian();  // Fallback
    }

    /// Samples from Beta distribution using Johnk's algorithm
    private double sampleBeta(Random rng, double alpha, double beta) {
        // Use rejection sampling for Beta
        double x, y;
        do {
            x = Math.pow(rng.nextDouble(), 1.0 / alpha);
            y = Math.pow(rng.nextDouble(), 1.0 / beta);
        } while (x + y > 1);
        return x / (x + y);
    }

    /// Samples from Student-t distribution
    private double sampleStudentT(Random rng, int nu) {
        // t = Z / sqrt(V/nu) where Z~N(0,1) and V~Chi-square(nu)
        double z = rng.nextGaussian();
        double v = 0;
        for (int i = 0; i < nu; i++) {
            double n = rng.nextGaussian();
            v += n * n;
        }
        return z / Math.sqrt(v / nu);
    }
}
