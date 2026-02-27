package io.nosqlbench.vectordata.spec.metadata;

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

import static org.junit.jupiter.api.Assertions.*;

/// Tests for {@link Half}.
class HalfTest {

    @Test
    void roundTripFromFloat() {
        Half h = Half.of(1.0f);
        assertEquals(1.0f, h.toFloat(), 0.0f);
    }

    @Test
    void roundTripFromDouble() {
        Half h = Half.of(2.0);
        assertEquals(2.0f, h.toFloat(), 0.0f);
        assertEquals(2.0, h.toDouble(), 0.0);
    }

    @Test
    void fromBitsKnownValue() {
        // 0x3C00 is 1.0 in IEEE 754 binary16
        Half h = Half.fromBits((short) 0x3C00);
        assertEquals(1.0f, h.toFloat(), 0.0f);
    }

    @Test
    void toBitsRoundTrip() {
        Half h = Half.of(3.14f);
        short bits = h.toBits();
        Half h2 = Half.fromBits(bits);
        assertEquals(h.toFloat(), h2.toFloat(), 0.0f);
    }

    @Test
    void specialValueZero() {
        Half h = Half.of(0.0f);
        assertEquals(0.0f, h.toFloat(), 0.0f);
        assertEquals((short) 0, h.toBits());
    }

    @Test
    void specialValueNegativeZero() {
        Half h = Half.of(-0.0f);
        assertEquals(-0.0f, h.toFloat(), 0.0f);
        assertEquals((short) 0x8000, h.toBits());
    }

    @Test
    void specialValueInfinity() {
        Half pos = Half.of(Float.POSITIVE_INFINITY);
        assertTrue(Float.isInfinite(pos.toFloat()));
        assertTrue(pos.toFloat() > 0);

        Half neg = Half.of(Float.NEGATIVE_INFINITY);
        assertTrue(Float.isInfinite(neg.toFloat()));
        assertTrue(neg.toFloat() < 0);
    }

    @Test
    void specialValueNaN() {
        Half h = Half.of(Float.NaN);
        assertTrue(Float.isNaN(h.toFloat()));
    }

    @Test
    void halfPrecisionLimits() {
        // Half max is 65504.0
        Half h = Half.of(65504.0f);
        assertEquals(65504.0f, h.toFloat(), 0.0f);
    }

    @Test
    void equality() {
        Half a = Half.of(1.5f);
        Half b = Half.of(1.5f);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void inequalityDifferentValues() {
        Half a = Half.of(1.0f);
        Half b = Half.of(2.0f);
        assertNotEquals(a, b);
    }

    @Test
    void comparableOrdering() {
        Half smaller = Half.of(1.0f);
        Half larger = Half.of(2.0f);
        assertTrue(smaller.compareTo(larger) < 0);
        assertTrue(larger.compareTo(smaller) > 0);
        assertEquals(0, smaller.compareTo(Half.of(1.0f)));
    }

    @Test
    void toStringContainsValue() {
        Half h = Half.of(1.5f);
        String s = h.toString();
        assertTrue(s.contains("1.5"));
    }

    @Test
    void smallValuesRoundTrip() {
        // Test subnormal range
        Half h = Half.of(0.0001f);
        float result = h.toFloat();
        // Should be close but precision is limited
        assertTrue(Math.abs(result) < 0.001f);
    }
}
