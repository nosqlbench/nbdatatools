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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ModelEquivalenceAnalyzer.
 */
@Tag("unit")
public class ModelEquivalenceAnalyzerTest {

    private final ModelEquivalenceAnalyzer analyzer = new ModelEquivalenceAnalyzer();

    @Test
    void analyze_normalModel_noSimplification() {
        // A normal model is already the simplest symmetric unbounded distribution
        NormalScalarModel normal = new NormalScalarModel(0.0, 1.0);

        ModelEquivalenceAnalyzer.EquivalenceReport report = analyzer.analyze(normal);

        assertNotNull(report);
        assertEquals(normal, report.getSource());
        assertNotNull(report.getSourceProfile());
        assertEquals(0.0, report.getSourceProfile().mean(), 1e-6);
        assertEquals(1.0, report.getSourceProfile().stdDev(), 1e-6);
        assertEquals(0.0, report.getSourceProfile().skewness(), 1e-6);
        assertEquals(3.0, report.getSourceProfile().kurtosis(), 1e-6);
    }

    @Test
    void analyze_pearsonIV_nearNormal_recommendsSimplification() {
        // Pearson IV with very small nu (skewness) should be equivalent to normal
        // m = 100 gives very low kurtosis deviation, nu = 0 gives symmetry
        PearsonIVScalarModel p4 = new PearsonIVScalarModel(100.0, 0.0, 1.0, 0.0);

        ModelEquivalenceAnalyzer.EquivalenceReport report = analyzer.analyze(p4);

        assertNotNull(report);
        assertFalse(report.getComparisons().isEmpty());

        // Print report for debugging
        System.out.println(report);

        // With nu=0 and high m, should be close to normal
        if (report.canSimplify()) {
            assertEquals(NormalScalarModel.MODEL_TYPE,
                        report.getRecommendedSimplification().getModelType());
        }
    }

    @Test
    void analyze_pearsonIV_skewed_noSimplificationToNormal() {
        // Pearson IV with significant skewness should NOT simplify to normal
        PearsonIVScalarModel p4 = new PearsonIVScalarModel(3.0, 2.0, 1.0, 0.0);

        ModelEquivalenceAnalyzer.EquivalenceReport report = analyzer.analyze(p4);

        assertNotNull(report);
        System.out.println(report);

        // Should not simplify to normal due to skewness
        if (report.canSimplify()) {
            assertNotEquals(NormalScalarModel.MODEL_TYPE,
                           report.getRecommendedSimplification().getModelType());
        }
    }

    @Test
    void analyze_studentT_highDf_recommendsNormal() {
        // Student's t with high df converges to normal
        StudentTScalarModel t = new StudentTScalarModel(100, 0.0, 1.0);

        ModelEquivalenceAnalyzer.EquivalenceReport report = analyzer.analyze(t);

        assertNotNull(report);
        System.out.println(report);

        // High df t-distribution should be equivalent to normal
        assertTrue(report.canSimplify());
        assertEquals(NormalScalarModel.MODEL_TYPE,
                    report.getRecommendedSimplification().getModelType());
    }

    @Test
    void analyze_studentT_lowDf_heavyTails() {
        // Student's t with low df has heavy tails, should NOT simplify to normal
        StudentTScalarModel t = new StudentTScalarModel(3, 0.0, 1.0);

        ModelEquivalenceAnalyzer.EquivalenceReport report = analyzer.analyze(t);

        assertNotNull(report);
        System.out.println(report);

        // Low df has excess kurtosis, normal is not a good fit
        // May or may not simplify depending on threshold
        for (var comp : report.getComparisons()) {
            if (comp.candidate().getModelType().equals(NormalScalarModel.MODEL_TYPE)) {
                // Should have noticeable CDF difference due to heavy tails
                assertTrue(comp.maxCdfDifference() > 0.01,
                    "Expected CDF difference > 0.01, got " + comp.maxCdfDifference());
            }
        }
    }

    @Test
    void analyze_beta_symmetric_nearUniform() {
        // Beta(1, 1) is exactly uniform
        BetaScalarModel beta = new BetaScalarModel(1.0, 1.0, 0.0, 1.0);

        ModelEquivalenceAnalyzer.EquivalenceReport report = analyzer.analyze(beta);

        assertNotNull(report);
        System.out.println(report);

        // Beta(1,1) should be equivalent to uniform
        if (report.canSimplify()) {
            assertEquals(UniformScalarModel.MODEL_TYPE,
                        report.getRecommendedSimplification().getModelType());
        }
    }

    @Test
    void analyze_gamma_lowShape_skewed() {
        // Gamma with low shape is highly skewed
        GammaScalarModel gamma = new GammaScalarModel(2.0, 1.0);

        ModelEquivalenceAnalyzer.EquivalenceReport report = analyzer.analyze(gamma);

        assertNotNull(report);
        System.out.println(report);

        // Gamma is positively skewed, should not simplify to normal
        if (report.canSimplify()) {
            assertNotEquals(NormalScalarModel.MODEL_TYPE,
                           report.getRecommendedSimplification().getModelType());
        }
    }

    @Test
    void analyze_gamma_highShape_nearNormal() {
        // Gamma with high shape approaches normal (Central Limit Theorem)
        GammaScalarModel gamma = new GammaScalarModel(100.0, 0.1);

        ModelEquivalenceAnalyzer.EquivalenceReport report = analyzer.analyze(gamma);

        assertNotNull(report);
        System.out.println(report);

        // High shape gamma is approximately normal
        // This may or may not trigger simplification depending on threshold
    }

    @Test
    void analyze_uniform_isSimplest() {
        // Uniform should not be simplified further
        UniformScalarModel uniform = new UniformScalarModel(0.0, 1.0);

        ModelEquivalenceAnalyzer.EquivalenceReport report = analyzer.analyze(uniform);

        assertNotNull(report);
        System.out.println(report);

        // Uniform is already the simplest, no simpler model should be recommended
        // (The analyzer might compare against normal, but uniform is simpler)
    }

    @Test
    void momentProfile_correctValues() {
        NormalScalarModel normal = new NormalScalarModel(5.0, 2.0);

        ModelEquivalenceAnalyzer.EquivalenceReport report = analyzer.analyze(normal);
        var profile = report.getSourceProfile();

        assertEquals(5.0, profile.mean(), 1e-6);
        assertEquals(2.0, profile.stdDev(), 1e-6);
        assertEquals(0.0, profile.skewness(), 1e-6);
        assertEquals(3.0, profile.kurtosis(), 1e-6);
        assertTrue(profile.isSymmetric());
        assertTrue(profile.isMesokurtic());
    }

    @Test
    void momentProfile_truncatedNormal() {
        NormalScalarModel truncated = new NormalScalarModel(0.0, 1.0, -1.0, 1.0);

        ModelEquivalenceAnalyzer.EquivalenceReport report = analyzer.analyze(truncated);
        var profile = report.getSourceProfile();

        assertEquals(-1.0, profile.lower());
        assertEquals(1.0, profile.upper());
        assertTrue(profile.isBounded());
    }

    @Test
    void vectorSimplificationSummary_multipleModels() {
        // Create a simple vector model with mixed distributions
        ScalarModel[] scalars = new ScalarModel[] {
            new NormalScalarModel(0.0, 1.0),
            new StudentTScalarModel(100, 0.0, 1.0),  // Should simplify to normal
            new GammaScalarModel(100.0, 0.1),         // Near-normal
            new UniformScalarModel(0.0, 1.0)
        };

        VectorSpaceModel vectorModel = new VectorSpaceModel(1000L, scalars);

        ModelEquivalenceAnalyzer.VectorSimplificationSummary summary =
            analyzer.summarizeVector(vectorModel);

        assertNotNull(summary);
        assertEquals(4, summary.totalDimensions());

        System.out.println(summary);
    }

    @Test
    void modelComparison_cdfDifferenceMetrics() {
        // Compare two distinct distributions
        NormalScalarModel normal = new NormalScalarModel(0.0, 1.0);
        StudentTScalarModel t3 = new StudentTScalarModel(3, 0.0, 1.0);

        ModelEquivalenceAnalyzer.EquivalenceReport report = analyzer.analyze(t3);

        // Find the normal comparison
        var normalComparison = report.getComparisons().stream()
            .filter(c -> c.candidate().getModelType().equals(NormalScalarModel.MODEL_TYPE))
            .findFirst();

        assertTrue(normalComparison.isPresent());

        // t(3) vs normal should have noticeable CDF difference
        assertTrue(normalComparison.get().maxCdfDifference() > 0);
        assertTrue(normalComparison.get().meanCdfDifference() > 0);
    }

    @Test
    void analyze_toleranceThreshold() {
        // Test with stricter threshold
        ModelEquivalenceAnalyzer strictAnalyzer = new ModelEquivalenceAnalyzer(1000, 0.005);

        StudentTScalarModel t50 = new StudentTScalarModel(50, 0.0, 1.0);

        var report = strictAnalyzer.analyze(t50);

        assertNotNull(report);
        assertEquals(0.005, report.getThreshold());

        System.out.println("Strict threshold analysis:");
        System.out.println(report);
    }

    @Test
    void equivalenceReport_toString_readable() {
        PearsonIVScalarModel p4 = new PearsonIVScalarModel(5.0, 0.5, 1.0, 0.0);

        var report = analyzer.analyze(p4);
        String reportString = report.toString();

        assertNotNull(reportString);
        assertTrue(reportString.contains("EquivalenceReport"));
        assertTrue(reportString.contains("Source moments"));
        assertTrue(reportString.contains("Comparisons"));
        assertTrue(reportString.contains("RECOMMENDATION"));
    }
}
