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

/// Tests for {@link Ulid}.
class UlidTest {

    @Test
    void roundTripFromString() {
        String input = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        Ulid ulid = Ulid.of(input);
        assertEquals(input, ulid.toString());
    }

    @Test
    void roundTripFromBytes() {
        byte[] bytes = new byte[16];
        for (int i = 0; i < 16; i++) {
            bytes[i] = (byte) (i + 1);
        }
        Ulid ulid = Ulid.of(bytes);
        byte[] result = ulid.toBytes();
        assertArrayEquals(bytes, result);
    }

    @Test
    void bytesToStringRoundTrip() {
        byte[] bytes = new byte[16];
        for (int i = 0; i < 16; i++) {
            bytes[i] = (byte) (i * 17);
        }
        Ulid ulid = Ulid.of(bytes);
        String s = ulid.toString();
        Ulid parsed = Ulid.of(s);
        assertArrayEquals(bytes, parsed.toBytes());
    }

    @Test
    void timestampExtraction() {
        // Construct a ULID where the first 6 bytes encode a known timestamp
        // Timestamp = 1000000 (0x000000000F4240)
        byte[] bytes = new byte[16];
        bytes[0] = 0x00;
        bytes[1] = 0x00;
        bytes[2] = 0x00;
        bytes[3] = 0x0F;
        bytes[4] = 0x42;
        bytes[5] = 0x40;
        Ulid ulid = Ulid.of(bytes);
        assertEquals(1000000L, ulid.timestamp());
    }

    @Test
    void zeroUlid() {
        byte[] zeros = new byte[16];
        Ulid ulid = Ulid.of(zeros);
        assertEquals(0L, ulid.timestamp());
        assertEquals("00000000000000000000000000", ulid.toString());
    }

    @Test
    void equality() {
        String s = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        Ulid a = Ulid.of(s);
        Ulid b = Ulid.of(s);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void inequalityDifferentValues() {
        Ulid a = Ulid.of(new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1});
        Ulid b = Ulid.of(new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2});
        assertNotEquals(a, b);
    }

    @Test
    void comparableOrdering() {
        Ulid smaller = Ulid.of(new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1});
        Ulid larger = Ulid.of(new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2});
        assertTrue(smaller.compareTo(larger) < 0);
        assertTrue(larger.compareTo(smaller) > 0);
        assertEquals(0, smaller.compareTo(Ulid.of(smaller.toBytes())));
    }

    @Test
    void caseInsensitiveParsing() {
        String upper = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        String lower = "01arz3ndektsv4rrffq69g5fav";
        assertEquals(Ulid.of(upper), Ulid.of(lower));
    }

    @Test
    void defensiveCopyOnConstruction() {
        byte[] bytes = new byte[16];
        bytes[0] = 42;
        Ulid ulid = Ulid.of(bytes);
        bytes[0] = 0; // mutate original
        assertEquals(42, ulid.toBytes()[0]); // ULID unchanged
    }

    @Test
    void defensiveCopyOnToBytes() {
        Ulid ulid = Ulid.of(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
        byte[] a = ulid.toBytes();
        byte[] b = ulid.toBytes();
        a[0] = 0;
        assertNotEquals(a[0], b[0]); // independent copies
    }

    @Test
    void rejectsNullString() {
        assertThrows(IllegalArgumentException.class, () -> Ulid.of((String) null));
    }

    @Test
    void rejectsWrongLengthString() {
        assertThrows(IllegalArgumentException.class, () -> Ulid.of("01ARZ3NDEK")); // too short
        assertThrows(IllegalArgumentException.class, () -> Ulid.of("01ARZ3NDEKTSV4RRFFQ69G5FAVX")); // too long
    }

    @Test
    void rejectsInvalidCharacter() {
        // 'U' is not a valid Crockford Base32 character
        assertThrows(IllegalArgumentException.class, () -> Ulid.of("01ARZ3NDEKTSV4RRFFQ69G5FAU"));
    }

    @Test
    void rejectsNullBytes() {
        assertThrows(IllegalArgumentException.class, () -> Ulid.of((byte[]) null));
    }

    @Test
    void rejectsWrongLengthBytes() {
        assertThrows(IllegalArgumentException.class, () -> Ulid.of(new byte[15]));
        assertThrows(IllegalArgumentException.class, () -> Ulid.of(new byte[17]));
    }
}
