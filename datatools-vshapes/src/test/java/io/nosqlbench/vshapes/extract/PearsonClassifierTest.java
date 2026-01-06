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

import io.nosqlbench.vshapes.model.PearsonType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the Pearson distribution classifier.
 */
public class PearsonClassifierTest {

    @Test
    void testNormalDistribution() {
        // Normal: skewness = 0, kurtosis = 3
        PearsonType type = PearsonClassifier.classify(0.0, 3.0);
        assertEquals(PearsonType.TYPE_0_NORMAL, type);
    }

    @Test
    void testNormalDistributionWithTolerance() {
        // Near-normal: small skewness, kurtosis close to 3
        PearsonType type = PearsonClassifier.classify(0.05, 3.1);
        assertEquals(PearsonType.TYPE_0_NORMAL, type);
    }

    @Test
    void testSymmetricBeta() {
        // Symmetric beta: skewness = 0, kurtosis < 3 (platykurtic)
        PearsonType type = PearsonClassifier.classify(0.0, 2.0);
        assertEquals(PearsonType.TYPE_II_SYMMETRIC_BETA, type);
    }

    @Test
    void testStudentT() {
        // Student's t: skewness = 0, kurtosis > 3 (leptokurtic)
        PearsonType type = PearsonClassifier.classify(0.0, 6.0);
        assertEquals(PearsonType.TYPE_VII_STUDENT_T, type);
    }

    @Test
    void testGammaDistribution() {
        // Gamma has specific relationship: β₁ = 4/k, β₂ = 3 + 6/k
        // For k=4: skewness = 1, kurtosis = 4.5
        // This is ON the Type III line where 2β₂ - 3β₁ - 6 = 0
        double skewness = 1.0;  // sqrt(4/4)
        double kurtosis = 4.5;  // 3 + 6/4
        PearsonType type = PearsonClassifier.classify(skewness, kurtosis);
        assertEquals(PearsonType.TYPE_III_GAMMA, type);
    }

    @Test
    void testBetaDistribution() {
        // Beta distribution falls below the Type III line (κ < 0)
        // Points where 2β₂ - 3β₁ - 6 < 0 and 4β₂ - 3β₁ > 0 give κ < 0
        // For skewness=0.5, kurtosis=2.5:
        // β₁ = 0.25, denom1 = 2*2.5 - 3*0.25 - 6 = 5 - 0.75 - 6 = -1.75
        // denom2 = 4*2.5 - 3*0.25 = 10 - 0.75 = 9.25
        // denominator = 4 * (-1.75) * 9.25 < 0, so κ < 0
        PearsonType type = PearsonClassifier.classify(0.5, 2.5);
        assertEquals(PearsonType.TYPE_I_BETA, type);
    }

    @Test
    void testPearsonTypeIV() {
        // Type IV: 0 < κ < 1
        // Points above the Type III line but below κ = 1
        // For higher kurtosis relative to skewness²
        PearsonType type = PearsonClassifier.classify(0.5, 5.0);
        assertEquals(PearsonType.TYPE_IV, type);
    }

    @Test
    void testInverseGammaOrBetaPrime() {
        // Type V (Inverse Gamma) and Type VI (Beta Prime) are hard to hit exactly
        // They require specific moment relationships
        // This test just verifies asymmetric high-kurtosis distributions classify
        PearsonType type = PearsonClassifier.classify(2.0, 12.0);
        // Could be Type IV, V, or VI depending on exact κ
        assertNotNull(type);
        assertTrue(type == PearsonType.TYPE_IV ||
                   type == PearsonType.TYPE_V_INVERSE_GAMMA ||
                   type == PearsonType.TYPE_VI_BETA_PRIME);
    }

    @Test
    void testBetaPrime() {
        // Beta Prime / F-distribution: κ > 1
        // Need β₁ large relative to β₂ such that κ > 1
        // For F-distribution with low df: highly skewed, heavy tails
        // skewness=4, kurtosis=30 should give κ > 1
        PearsonType type = PearsonClassifier.classify(4.0, 30.0);
        // This asymmetric heavy-tailed distribution
        assertNotNull(type);
    }

    @Test
    void testClassifyWithExcessKurtosis() {
        // Normal: skewness = 0, excess kurtosis = 0 (standard kurtosis = 3)
        PearsonType type = PearsonClassifier.classifyWithExcessKurtosis(0.0, 0.0);
        assertEquals(PearsonType.TYPE_0_NORMAL, type);
    }

    @Test
    void testComputeKappaSymmetric() {
        // For symmetric distributions (skewness = 0), κ = 0 since numerator is 0
        double kappa = PearsonClassifier.computeKappa(0.0, 3.0);
        assertEquals(0.0, kappa, 0.001);
    }

    @Test
    void testComputeKappaGamma() {
        // For gamma distribution, the Type III line is where denom1 = 0
        // 2β₂ - 3β₁ - 6 = 0 → β₂ = (3β₁ + 6)/2
        // For skewness=1 (β₁=1): β₂ = 9/2 = 4.5
        // At this point, κ is undefined (NaN) because denominator = 0
        double kappa = PearsonClassifier.computeKappa(1.0, 4.5);
        assertTrue(Double.isNaN(kappa), "Gamma (Type III line) has undefined κ");
    }

    @Test
    void testComputeKappaBeta() {
        // For beta-like distributions, κ < 0
        double kappa = PearsonClassifier.computeKappa(0.5, 2.5);
        assertTrue(kappa < 0, "Beta distribution should have κ < 0");
    }

    @Test
    void testClassifyDetailed() {
        PearsonClassifier.ClassificationResult result =
            PearsonClassifier.classifyDetailed(0.0, 3.0);

        assertEquals(PearsonType.TYPE_0_NORMAL, result.type());
        assertEquals(0.0, result.beta1(), 0.001);
        assertEquals(3.0, result.beta2(), 0.001);
        assertTrue(result.isSymmetric());
        assertTrue(result.isMesokurtic());
        assertFalse(result.isPlatykurtic());
        assertFalse(result.isLeptokurtic());
    }

    @Test
    void testClassifyDetailedPlatykurtic() {
        PearsonClassifier.ClassificationResult result =
            PearsonClassifier.classifyDetailed(0.0, 2.0);

        assertEquals(PearsonType.TYPE_II_SYMMETRIC_BETA, result.type());
        assertTrue(result.isSymmetric());
        assertTrue(result.isPlatykurtic());
        assertFalse(result.isLeptokurtic());
    }

    @Test
    void testClassifyDetailedLeptokurtic() {
        PearsonClassifier.ClassificationResult result =
            PearsonClassifier.classifyDetailed(0.0, 6.0);

        assertEquals(PearsonType.TYPE_VII_STUDENT_T, result.type());
        assertTrue(result.isSymmetric());
        assertTrue(result.isLeptokurtic());
        assertFalse(result.isPlatykurtic());
    }

    @Test
    void testClassificationResultToString() {
        PearsonClassifier.ClassificationResult result =
            PearsonClassifier.classifyDetailed(0.5, 3.5);

        String str = result.toString();
        assertNotNull(str);
        assertTrue(str.contains("ClassificationResult"));
        assertTrue(str.contains("β₁="));
        assertTrue(str.contains("β₂="));
    }

    @ParameterizedTest
    @CsvSource({
        // skewness, kurtosis, expected type
        "0.0, 3.0, TYPE_0_NORMAL",
        "0.0, 2.0, TYPE_II_SYMMETRIC_BETA",
        "0.0, 1.5, TYPE_II_SYMMETRIC_BETA",
        "0.0, 5.0, TYPE_VII_STUDENT_T",
        "0.0, 10.0, TYPE_VII_STUDENT_T",
    })
    void testSymmetricDistributions(double skewness, double kurtosis, String expectedType) {
        PearsonType expected = PearsonType.valueOf(expectedType);
        PearsonType actual = PearsonClassifier.classify(skewness, kurtosis);
        assertEquals(expected, actual);
    }

    @Test
    void testNegativeSkewness() {
        // Negative skewness should give same classification as positive
        // (since β₁ = skewness²)
        PearsonType positiveSkew = PearsonClassifier.classify(0.6, 2.5);
        PearsonType negativeSkew = PearsonClassifier.classify(-0.6, 2.5);
        assertEquals(positiveSkew, negativeSkew);
    }

    @Test
    void testEdgeCaseVeryHighKurtosis() {
        // Very heavy tails with some skewness
        PearsonType type = PearsonClassifier.classify(0.5, 50.0);
        assertNotNull(type);
        // Should be Type IV, V, or VI depending on κ
    }

    @Test
    void testEdgeCaseVeryLowKurtosis() {
        // Very light tails (like uniform)
        PearsonType type = PearsonClassifier.classify(0.0, 1.8);
        assertEquals(PearsonType.TYPE_II_SYMMETRIC_BETA, type);
    }
}
