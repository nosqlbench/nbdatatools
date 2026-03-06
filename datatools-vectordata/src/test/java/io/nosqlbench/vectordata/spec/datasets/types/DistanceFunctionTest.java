package io.nosqlbench.vectordata.spec.datasets.types;

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

import static org.assertj.core.api.Assertions.*;

/// Tests for {@link DistanceFunction}, covering float, double, and half-precision
/// (f16) distance computation across all metrics: cosine, euclidean, L1, and dot product.
class DistanceFunctionTest {

    // ==================== Helper: float-to-f16 conversion ====================

    /// Convert a float to IEEE 754 binary16 (half-precision) bits.
    /// Mirrors the conversion in DistanceFunction.f16ToF32's inverse.
    private static short floatToHalf(float value) {
        int fbits = Float.floatToIntBits(value);
        int sign = (fbits >>> 16) & 0x8000;
        int exp = ((fbits >>> 23) & 0xFF) - 127;
        int mantissa = fbits & 0x007FFFFF;

        if (exp > 15) {
            return (short) (sign | 0x7C00); // overflow → infinity
        }
        if (exp > -15) {
            int raw = sign | ((exp + 15) << 10) | (mantissa >>> 13);
            int roundBit = 1 << 12;
            if ((mantissa & roundBit) != 0) {
                if ((mantissa & (roundBit - 1)) != 0 || (raw & 1) != 0) {
                    raw++;
                }
            }
            return (short) raw;
        }
        if (exp >= -24) {
            mantissa |= 0x00800000;
            int shift = -1 - exp;
            return (short) (sign | (mantissa >>> (shift + 13)));
        }
        return (short) sign; // underflow → zero
    }

    /// Build a short[] of f16 values from floats.
    private static short[] f16(float... values) {
        short[] result = new short[values.length];
        for (int i = 0; i < values.length; i++) {
            result[i] = floatToHalf(values[i]);
        }
        return result;
    }

    // ==================== Float/Double baseline tests ====================

    @Test
    void euclideanDistance_float() {
        float[] a = {1.0f, 0.0f, 0.0f};
        float[] b = {0.0f, 1.0f, 0.0f};
        double dist = DistanceFunction.EUCLIDEAN.distance(a, b);
        assertThat(dist).isCloseTo(Math.sqrt(2.0), within(1e-9));
    }

    @Test
    void euclideanDistance_double() {
        double[] a = {3.0, 0.0};
        double[] b = {0.0, 4.0};
        double dist = DistanceFunction.EUCLIDEAN.distance(a, b);
        assertThat(dist).isCloseTo(5.0, within(1e-9));
    }

    @Test
    void l2_isSameAsEuclidean() {
        float[] a = {1.0f, 2.0f, 3.0f};
        float[] b = {4.0f, 5.0f, 6.0f};
        assertThat(DistanceFunction.L2.distance(a, b))
            .isEqualTo(DistanceFunction.EUCLIDEAN.distance(a, b));
    }

    @Test
    void cosineDistance_identicalVectors() {
        float[] a = {1.0f, 2.0f, 3.0f};
        double dist = DistanceFunction.COSINE.distance(a, a);
        assertThat(dist).isCloseTo(0.0, within(1e-9));
    }

    @Test
    void cosineDistance_orthogonalVectors() {
        float[] a = {1.0f, 0.0f};
        float[] b = {0.0f, 1.0f};
        double dist = DistanceFunction.COSINE.distance(a, b);
        assertThat(dist).isCloseTo(1.0, within(1e-9));
    }

    @Test
    void dotProduct_distance() {
        // dot(a,b) = 1*4 + 2*5 + 3*6 = 32
        // distance = -dot = -32
        float[] a = {1.0f, 2.0f, 3.0f};
        float[] b = {4.0f, 5.0f, 6.0f};
        double dist = DistanceFunction.DOT_PRODUCT.distance(a, b);
        assertThat(dist).isCloseTo(-32.0, within(1e-9));
    }

    @Test
    void dotProduct_distance_double() {
        double[] a = {1.0, 2.0, 3.0};
        double[] b = {4.0, 5.0, 6.0};
        double dist = DistanceFunction.DOT_PRODUCT.distance(a, b);
        assertThat(dist).isCloseTo(-32.0, within(1e-9));
    }

    @Test
    void manhattanDistance_float() {
        float[] a = {1.0f, 2.0f, 3.0f};
        float[] b = {4.0f, 6.0f, 3.0f};
        // |1-4| + |2-6| + |3-3| = 3 + 4 + 0 = 7
        double dist = DistanceFunction.L1.distance(a, b);
        assertThat(dist).isCloseTo(7.0, within(1e-9));
    }

    @Test
    void nullVectors_throw() {
        assertThatThrownBy(() -> DistanceFunction.EUCLIDEAN.distance((float[]) null, new float[]{1.0f}))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void mismatchedDimensions_throw() {
        assertThatThrownBy(() -> DistanceFunction.COSINE.distance(new float[]{1.0f}, new float[]{1.0f, 2.0f}))
            .isInstanceOf(IllegalArgumentException.class);
    }

    // ==================== Half-precision (f16) tests ====================

    @Test
    void halfEuclidean_matchesFloat() {
        float[] af = {1.0f, 0.0f, 0.0f};
        float[] bf = {0.0f, 1.0f, 0.0f};
        short[] ah = f16(af);
        short[] bh = f16(bf);

        double floatDist = DistanceFunction.EUCLIDEAN.distance(af, bf);
        double halfDist = DistanceFunction.EUCLIDEAN.distance(ah, bh);
        assertThat(halfDist).isCloseTo(floatDist, within(1e-2));
    }

    @Test
    void halfCosine_identicalVectors() {
        short[] a = f16(1.0f, 2.0f, 3.0f);
        double dist = DistanceFunction.COSINE.distance(a, a);
        assertThat(dist).isCloseTo(0.0, within(1e-3));
    }

    @Test
    void halfCosine_orthogonalVectors() {
        short[] a = f16(1.0f, 0.0f);
        short[] b = f16(0.0f, 1.0f);
        double dist = DistanceFunction.COSINE.distance(a, b);
        assertThat(dist).isCloseTo(1.0, within(1e-3));
    }

    @Test
    void halfDotProduct_distance() {
        // dot(a,b) = 1*4 + 2*5 + 3*6 = 32, distance = -32
        short[] a = f16(1.0f, 2.0f, 3.0f);
        short[] b = f16(4.0f, 5.0f, 6.0f);
        double dist = DistanceFunction.DOT_PRODUCT.distance(a, b);
        assertThat(dist).isCloseTo(-32.0, within(0.5));
    }

    @Test
    void halfManhattan_matchesFloat() {
        float[] af = {1.0f, 2.0f, 3.0f};
        float[] bf = {4.0f, 6.0f, 3.0f};
        short[] ah = f16(af);
        short[] bh = f16(bf);

        double floatDist = DistanceFunction.L1.distance(af, bf);
        double halfDist = DistanceFunction.L1.distance(ah, bh);
        assertThat(halfDist).isCloseTo(floatDist, within(0.1));
    }

    @Test
    void halfL2_isSameAsHalfEuclidean() {
        short[] a = f16(1.0f, 2.0f, 3.0f);
        short[] b = f16(4.0f, 5.0f, 6.0f);
        assertThat(DistanceFunction.L2.distance(a, b))
            .isEqualTo(DistanceFunction.EUCLIDEAN.distance(a, b));
    }

    @Test
    void halfDistance_nullVectors_throw() {
        assertThatThrownBy(() -> DistanceFunction.EUCLIDEAN.distance((short[]) null, f16(1.0f)))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void halfDistance_mismatchedDimensions_throw() {
        assertThatThrownBy(() -> DistanceFunction.COSINE.distance(f16(1.0f), f16(1.0f, 2.0f)))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void halfDistance_highDimensional() {
        // Test with a dimension larger than any SIMD lane width to exercise
        // both the vectorized loop and the scalar tail
        int dim = 137; // prime, not a multiple of any lane count
        float[] af = new float[dim];
        float[] bf = new float[dim];
        for (int i = 0; i < dim; i++) {
            af[i] = (float) Math.sin(i * 0.1);
            bf[i] = (float) Math.cos(i * 0.1);
        }

        short[] ah = f16(af);
        short[] bh = f16(bf);

        double floatEuclid = DistanceFunction.EUCLIDEAN.distance(af, bf);
        double halfEuclid = DistanceFunction.EUCLIDEAN.distance(ah, bh);
        // f16 has ~3 decimal digits of precision, so tolerance is relative
        assertThat(halfEuclid).isCloseTo(floatEuclid, within(floatEuclid * 0.02));

        double floatCosine = DistanceFunction.COSINE.distance(af, bf);
        double halfCosine = DistanceFunction.COSINE.distance(ah, bh);
        assertThat(halfCosine).isCloseTo(floatCosine, within(0.01));
    }

    @Test
    void halfDistance_zeroVector() {
        short[] a = f16(0.0f, 0.0f, 0.0f);
        short[] b = f16(1.0f, 2.0f, 3.0f);

        // Euclidean should work
        double eucDist = DistanceFunction.EUCLIDEAN.distance(a, b);
        assertThat(eucDist).isGreaterThan(0.0);

        // Cosine with zero vector should throw
        assertThatThrownBy(() -> DistanceFunction.COSINE.distance(a, b))
            .isInstanceOf(IllegalArgumentException.class);
    }

    // ==================== Consistency: float vs half ====================

    @Test
    void allMetrics_halfConsistentWithFloat() {
        float[] af = {0.5f, 1.5f, 2.5f, 3.5f};
        float[] bf = {1.0f, 2.0f, 3.0f, 4.0f};
        short[] ah = f16(af);
        short[] bh = f16(bf);

        for (DistanceFunction df : DistanceFunction.values()) {
            if (df == DistanceFunction.L2) continue; // same as EUCLIDEAN
            double floatDist = df.distance(af, bf);
            double halfDist = df.distance(ah, bh);
            assertThat(halfDist)
                .as("Metric %s: half (%f) vs float (%f)", df, halfDist, floatDist)
                .isCloseTo(floatDist, within(Math.max(Math.abs(floatDist) * 0.05, 0.01)));
        }
    }
}
