package io.nosqlbench.vectordata.spec.predicates;

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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.*;

/// Tests for the Comparand typed value hierarchy and wire format encoding/decoding.
class ComparandTest {

    // ==================== IntVal ====================

    @Test
    void intValRoundTrip() {
        Comparand c = new Comparand.IntVal(42L);
        assertEquals(Comparand.TAG_INT, c.tag());
        assertEquals(9, c.encodedSize());
        Comparand decoded = roundTrip(c);
        assertEquals(c, decoded);
        assertEquals(42L, ((Comparand.IntVal) decoded).value());
    }

    @Test
    void intValNegative() {
        Comparand c = new Comparand.IntVal(-1L);
        Comparand decoded = roundTrip(c);
        assertEquals(-1L, ((Comparand.IntVal) decoded).value());
    }

    @Test
    void intValMinMax() {
        for (long val : new long[]{Long.MIN_VALUE, Long.MAX_VALUE, 0L}) {
            Comparand decoded = roundTrip(new Comparand.IntVal(val));
            assertEquals(val, ((Comparand.IntVal) decoded).value());
        }
    }

    // ==================== FloatVal ====================

    @Test
    void floatValRoundTrip() {
        Comparand c = new Comparand.FloatVal(3.14);
        assertEquals(Comparand.TAG_FLOAT, c.tag());
        assertEquals(9, c.encodedSize());
        Comparand decoded = roundTrip(c);
        assertEquals(c, decoded);
        assertEquals(3.14, ((Comparand.FloatVal) decoded).value(), 0.0);
    }

    @Test
    void floatValSpecialValues() {
        for (double val : new double[]{Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0.0, -0.0}) {
            Comparand original = new Comparand.FloatVal(val);
            Comparand decoded = roundTrip(original);
            if (Double.isNaN(val)) {
                assertTrue(Double.isNaN(((Comparand.FloatVal) decoded).value()));
            } else {
                assertEquals(Double.doubleToRawLongBits(val),
                    Double.doubleToRawLongBits(((Comparand.FloatVal) decoded).value()));
            }
        }
    }

    // ==================== TextVal ====================

    @Test
    void textValRoundTrip() {
        Comparand c = new Comparand.TextVal("hello");
        assertEquals(Comparand.TAG_TEXT, c.tag());
        Comparand decoded = roundTrip(c);
        assertEquals(c, decoded);
        assertEquals("hello", ((Comparand.TextVal) decoded).value());
    }

    @Test
    void textValEmpty() {
        Comparand decoded = roundTrip(new Comparand.TextVal(""));
        assertEquals("", ((Comparand.TextVal) decoded).value());
    }

    @Test
    void textValUnicode() {
        String unicode = "café \u00E9\u00E8\u00EA \u2603 \uD83D\uDE00";
        Comparand decoded = roundTrip(new Comparand.TextVal(unicode));
        assertEquals(unicode, ((Comparand.TextVal) decoded).value());
    }

    @Test
    void textValNullThrows() {
        assertThrows(NullPointerException.class, () -> new Comparand.TextVal(null));
    }

    // ==================== BoolVal ====================

    @Test
    void boolValRoundTrip() {
        Comparand t = new Comparand.BoolVal(true);
        Comparand f = new Comparand.BoolVal(false);
        assertEquals(Comparand.TAG_BOOL, t.tag());
        assertEquals(2, t.encodedSize());
        assertTrue(((Comparand.BoolVal) roundTrip(t)).value());
        assertFalse(((Comparand.BoolVal) roundTrip(f)).value());
    }

    // ==================== BytesVal ====================

    @Test
    void bytesValRoundTrip() {
        byte[] data = {1, 2, 3, 4, 5};
        Comparand c = new Comparand.BytesVal(data);
        assertEquals(Comparand.TAG_BYTES, c.tag());
        assertEquals(1 + 4 + 5, c.encodedSize());
        Comparand decoded = roundTrip(c);
        assertArrayEquals(data, ((Comparand.BytesVal) decoded).value());
    }

    @Test
    void bytesValEmpty() {
        Comparand decoded = roundTrip(new Comparand.BytesVal(new byte[0]));
        assertArrayEquals(new byte[0], ((Comparand.BytesVal) decoded).value());
    }

    // ==================== NullVal ====================

    @Test
    void nullValRoundTrip() {
        Comparand c = Comparand.NullVal.INSTANCE;
        assertEquals(Comparand.TAG_NULL, c.tag());
        assertEquals(1, c.encodedSize());
        Comparand decoded = roundTrip(c);
        assertTrue(decoded instanceof Comparand.NullVal);
    }

    // ==================== Equality ====================

    @Test
    void equalityAcrossTypes() {
        assertNotEquals(new Comparand.IntVal(1), new Comparand.FloatVal(1.0));
        assertNotEquals(new Comparand.IntVal(0), new Comparand.BoolVal(false));
        assertNotEquals(new Comparand.TextVal(""), Comparand.NullVal.INSTANCE);
        assertEquals(new Comparand.NullVal(), Comparand.NullVal.INSTANCE);
    }

    // ==================== Unknown tag ====================

    @Test
    void unknownTagThrows() {
        ByteBuffer buf = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN);
        buf.put((byte) 99);
        buf.flip();
        assertThrows(IllegalArgumentException.class, () -> Comparand.decode(buf));
    }

    // ==================== Helpers ====================

    private static Comparand roundTrip(Comparand c) {
        ByteBuffer buf = ByteBuffer.allocate(c.encodedSize()).order(ByteOrder.LITTLE_ENDIAN);
        c.encode(buf);
        buf.flip();
        return Comparand.decode(buf);
    }
}
