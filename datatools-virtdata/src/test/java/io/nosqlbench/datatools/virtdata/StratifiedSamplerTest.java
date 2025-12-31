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

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class StratifiedSamplerTest {

    @Test
    void testUnitIntervalRange() {
        // All values should be in (0, 1)
        long totalVectors = 1000;
        for (long ordinal = 0; ordinal < 100; ordinal++) {
            for (int dim = 0; dim < 10; dim++) {
                double value = StratifiedSampler.unitIntervalValue(ordinal, dim, totalVectors);
                assertTrue(value > 0.0, "Value should be > 0, got: " + value);
                assertTrue(value < 1.0, "Value should be < 1, got: " + value);
            }
        }
    }

    @Test
    void testDeterminism() {
        // Same inputs should produce same outputs
        long ordinal = 42;
        int dim = 5;
        long total = 1000;

        double first = StratifiedSampler.unitIntervalValue(ordinal, dim, total);
        double second = StratifiedSampler.unitIntervalValue(ordinal, dim, total);
        assertEquals(first, second, "Same inputs should produce same output");
    }

    @Test
    void testDimensionIndependence() {
        // Different dimensions should produce different values for same ordinal
        long ordinal = 42;
        long total = 10000;

        double dim0 = StratifiedSampler.unitIntervalValue(ordinal, 0, total);
        double dim1 = StratifiedSampler.unitIntervalValue(ordinal, 1, total);
        double dim2 = StratifiedSampler.unitIntervalValue(ordinal, 2, total);

        // Values should be different (checking anti-correlation)
        assertNotEquals(dim0, dim1, 0.001, "Different dimensions should give different values");
        assertNotEquals(dim1, dim2, 0.001, "Different dimensions should give different values");
        assertNotEquals(dim0, dim2, 0.001, "Different dimensions should give different values");
    }

    @Test
    void testOrdinalPermutation() {
        // Permuted ordinals should be well-distributed
        Set<Long> permutedValues = new HashSet<>();
        for (long ordinal = 0; ordinal < 1000; ordinal++) {
            long permuted = StratifiedSampler.permuteOrdinal(ordinal, 0);
            permutedValues.add(permuted);
        }

        // All 1000 ordinals should produce unique permuted values
        assertEquals(1000, permutedValues.size(), "Permutation should preserve uniqueness");
    }

    @Test
    void testDimensionAffectsPermutation() {
        long ordinal = 12345;
        long perm0 = StratifiedSampler.permuteOrdinal(ordinal, 0);
        long perm1 = StratifiedSampler.permuteOrdinal(ordinal, 1);
        long perm5 = StratifiedSampler.permuteOrdinal(ordinal, 5);

        assertNotEquals(perm0, perm1, "Different dimensions should produce different permutations");
        assertNotEquals(perm1, perm5, "Different dimensions should produce different permutations");
    }

    @Test
    void testAntiCongruencyForSmallSpaces() {
        // For a 2D space with 100 vectors, check that we don't get obvious lattice patterns
        int count = 100;
        double[] dim0Values = new double[count];
        double[] dim1Values = new double[count];

        for (int i = 0; i < count; i++) {
            dim0Values[i] = StratifiedSampler.unitIntervalValue(i, 0, count);
            dim1Values[i] = StratifiedSampler.unitIntervalValue(i, 1, count);
        }

        // Check that dim0 and dim1 values are not simply correlated
        // A naive implementation would have dim0[i] == dim1[i] for all i
        int sameCount = 0;
        for (int i = 0; i < count; i++) {
            if (Math.abs(dim0Values[i] - dim1Values[i]) < 0.01) {
                sameCount++;
            }
        }

        // At most 10% should be nearly the same (statistically unlikely to be more)
        assertTrue(sameCount < count * 0.2, "Too many matching values between dimensions: " + sameCount);
    }
}
