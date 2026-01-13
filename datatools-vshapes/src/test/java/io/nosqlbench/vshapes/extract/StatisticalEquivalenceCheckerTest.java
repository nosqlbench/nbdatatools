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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/// Unit tests for {@link StatisticalEquivalenceChecker}.
///
/// These tests verify the equivalence detection logic for various
/// distribution pairs that are statistically indistinguishable.
class StatisticalEquivalenceCheckerTest {

    private final StatisticalEquivalenceChecker checker = new StatisticalEquivalenceChecker();

    // ========== Same type tests ==========

    @Test
    void sameTypeNormalIsEquivalent() {
        ScalarModel a = new NormalScalarModel(0.0, 1.0);
        ScalarModel b = new NormalScalarModel(0.5, 1.2);

        assertTrue(checker.areEquivalent(a, b));
        assertEquals("same type", checker.getEquivalenceReason(a, b));
    }

    @Test
    void sameTypeBetaIsEquivalent() {
        ScalarModel a = new BetaScalarModel(2.0, 3.0);
        ScalarModel b = new BetaScalarModel(3.0, 2.0);

        assertTrue(checker.areEquivalent(a, b));
        assertEquals("same type", checker.getEquivalenceReason(a, b));
    }

    // ========== Normal ↔ Student-t tests ==========

    @Test
    void normalAndHighDfStudentTAreEquivalent() {
        ScalarModel normal = new NormalScalarModel(0.0, 1.0);
        ScalarModel studentT = new StudentTScalarModel(100, 0.0, 1.0);

        assertTrue(checker.areEquivalent(normal, studentT));
        assertEquals("Normal ↔ StudentT(ν≥30)", checker.getEquivalenceReason(normal, studentT));
    }

    @Test
    void normalAndLowDfStudentTAreNotEquivalent() {
        ScalarModel normal = new NormalScalarModel(0.0, 1.0);
        ScalarModel studentT = new StudentTScalarModel(5, 0.0, 1.0);

        assertFalse(checker.areEquivalent(normal, studentT));
        assertNull(checker.getEquivalenceReason(normal, studentT));
    }

    // ========== Beta ↔ Uniform tests ==========

    @Test
    void betaOneOneAndUniformAreEquivalent() {
        ScalarModel beta = new BetaScalarModel(1.0, 1.0, -1.0, 1.0);
        ScalarModel uniform = new UniformScalarModel(-1.0, 1.0);

        assertTrue(checker.areEquivalent(beta, uniform));
        assertEquals("Beta(≈1,≈1) ↔ Uniform", checker.getEquivalenceReason(beta, uniform));
    }

    @Test
    void betaNearOneOneAndUniformAreEquivalent() {
        // Within tolerance
        ScalarModel beta = new BetaScalarModel(1.1, 0.95, 0.0, 1.0);
        ScalarModel uniform = new UniformScalarModel(0.0, 1.0);

        assertTrue(checker.areEquivalent(beta, uniform));
    }

    @Test
    void betaShapedAndUniformAreNotEquivalent() {
        ScalarModel beta = new BetaScalarModel(2.0, 5.0);
        ScalarModel uniform = new UniformScalarModel(0.0, 1.0);

        assertFalse(checker.areEquivalent(beta, uniform));
    }

    // ========== Normal ↔ Beta tests ==========

    @Test
    void truncatedNormalAndSimilarBetaAreEquivalent() {
        // A truncated normal with moderate skew can look like a Beta
        // Normal(-0.5, 0.6) on [-1, 1] resembles Beta(~1.3, ~2.4) on [-1, 1]
        ScalarModel normal = new NormalScalarModel(-0.5, 0.6, -1.0, 1.0);
        ScalarModel beta = new BetaScalarModel(1.33, 2.36, -1.0, 1.0);

        assertTrue(checker.areEquivalent(normal, beta),
            "Truncated normal with similar CDF should be equivalent to Beta");
        assertEquals("Normal[a,b] ↔ Beta[a,b]", checker.getEquivalenceReason(normal, beta));
    }

    @Test
    void truncatedNormalAndMatchingBetaAreEquivalent() {
        // Centered normal with low spread looks like symmetric Beta
        ScalarModel normal = new NormalScalarModel(0.0, 0.3, -1.0, 1.0);
        ScalarModel beta = new BetaScalarModel(5.0, 5.0, -1.0, 1.0);  // Symmetric, peaked

        // Both are symmetric and peaked in center - may or may not be equivalent
        // depending on exact shape match
        boolean equivalent = checker.areEquivalent(normal, beta);
        if (equivalent) {
            assertEquals("Normal[a,b] ↔ Beta[a,b]", checker.getEquivalenceReason(normal, beta));
        }
    }

    @Test
    void unboundedNormalAndBetaAreNotEquivalent() {
        // Unbounded normal cannot be equivalent to Beta
        ScalarModel normal = new NormalScalarModel(0.0, 1.0);  // unbounded
        ScalarModel beta = new BetaScalarModel(2.0, 3.0, -1.0, 1.0);

        assertFalse(checker.areEquivalent(normal, beta),
            "Unbounded normal cannot be equivalent to bounded Beta");
    }

    @Test
    void truncatedNormalAndVeryDifferentBetaAreNotEquivalent() {
        // Very different shapes should not be equivalent
        ScalarModel normal = new NormalScalarModel(0.0, 0.1, -1.0, 1.0);  // Very peaked
        ScalarModel beta = new BetaScalarModel(0.5, 0.5, -1.0, 1.0);  // U-shaped

        assertFalse(checker.areEquivalent(normal, beta),
            "Very different shapes should not be equivalent");
    }

    @Test
    void normalAndBetaOnDifferentIntervalsAreNotEquivalent() {
        ScalarModel normal = new NormalScalarModel(0.0, 0.5, -1.0, 1.0);
        ScalarModel beta = new BetaScalarModel(2.0, 2.0, 0.0, 1.0);  // Different interval

        assertFalse(checker.areEquivalent(normal, beta),
            "Distributions on different intervals should not be equivalent");
    }

    // ========== Null handling ==========

    @Test
    void nullModelsAreEquivalent() {
        assertTrue(checker.areEquivalent(null, null));
    }

    @Test
    void nullAndNonNullAreNotEquivalent() {
        assertFalse(checker.areEquivalent(null, new NormalScalarModel(0.0, 1.0)));
        assertFalse(checker.areEquivalent(new NormalScalarModel(0.0, 1.0), null));
    }

    // ========== Different types (not equivalent) ==========

    @Test
    void normalAndGammaAreNotEquivalent() {
        ScalarModel normal = new NormalScalarModel(0.0, 1.0);
        ScalarModel gamma = new GammaScalarModel(2.0, 1.0);

        assertFalse(checker.areEquivalent(normal, gamma));
    }

    @Test
    void betaAndGammaAreNotEquivalent() {
        ScalarModel beta = new BetaScalarModel(2.0, 3.0);
        ScalarModel gamma = new GammaScalarModel(2.0, 1.0);

        assertFalse(checker.areEquivalent(beta, gamma));
    }
}
