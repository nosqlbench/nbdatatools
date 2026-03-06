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

import io.nosqlbench.nbdatatools.api.types.Half;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/// Adversarial tests for MNode covering numeric extrema, float specials,
/// buffer corruption, codec robustness, and edge cases not in the main suite.
class MNodeAdversarialTest {

    // ==================== 1. Numeric Extrema ====================

    @Test
    void longExtrema() {
        MNode node = MNode.of("max", Long.MAX_VALUE, "min", Long.MIN_VALUE);
        MNode rt = roundTrip(node);
        assertEquals(Long.MAX_VALUE, rt.getLong("max"));
        assertEquals(Long.MIN_VALUE, rt.getLong("min"));
    }

    @Test
    void intExtrema() {
        MNode node = MNode.of("max", MNode.int32(Integer.MAX_VALUE), "min", MNode.int32(Integer.MIN_VALUE));
        MNode rt = roundTrip(node);
        assertEquals(Integer.MAX_VALUE, rt.getInt("max"));
        assertEquals(Integer.MIN_VALUE, rt.getInt("min"));
    }

    @Test
    void shortExtrema() {
        MNode node = MNode.of("max", MNode.int16(Short.MAX_VALUE), "min", MNode.int16(Short.MIN_VALUE));
        MNode rt = roundTrip(node);
        assertEquals(Short.MAX_VALUE, rt.getShort("max"));
        assertEquals(Short.MIN_VALUE, rt.getShort("min"));
    }

    @Test
    void doubleExtrema() {
        MNode node = MNode.of(
            "max", Double.MAX_VALUE,
            "min", Double.MIN_VALUE,
            "minNormal", Double.MIN_NORMAL
        );
        MNode rt = roundTrip(node);
        assertEquals(Double.MAX_VALUE, rt.getDouble("max"));
        assertEquals(Double.MIN_VALUE, rt.getDouble("min"));
        assertEquals(Double.MIN_NORMAL, rt.getDouble("minNormal"));
    }

    @Test
    void floatExtrema() {
        MNode node = MNode.of(
            "max", MNode.float32(Float.MAX_VALUE),
            "min", MNode.float32(Float.MIN_VALUE),
            "minNormal", MNode.float32(Float.MIN_NORMAL)
        );
        MNode rt = roundTrip(node);
        assertEquals(Float.MAX_VALUE, rt.getFloat("max"));
        assertEquals(Float.MIN_VALUE, rt.getFloat("min"));
        assertEquals(Float.MIN_NORMAL, rt.getFloat("minNormal"));
    }

    @Test
    void halfExtrema() {
        // half max is 65504, half min subnormal is ~6.0e-8 but the smallest
        // float that survives half round-trip is the half min normal (~6.1e-5)
        float halfMinNormal = 6.103515625e-5f; // 2^-14
        MNode node = MNode.of(
            "max", MNode.half(65504.0f),
            "minNorm", MNode.half(halfMinNormal)
        );
        MNode rt = roundTrip(node);
        assertEquals(65504.0f, rt.getFloat("max"), 0.0f);
        assertEquals(halfMinNormal, rt.getFloat("minNorm"), 0.0f);
    }

    @Test
    void bigDecimalExtremeScales() {
        BigDecimal posScale = new BigDecimal(BigInteger.ONE, Integer.MAX_VALUE);
        BigDecimal negScale = new BigDecimal(BigInteger.ONE, Integer.MIN_VALUE);
        MNode node = MNode.of("pos", posScale, "neg", negScale);
        MNode rt = roundTrip(node);
        assertEquals(posScale, rt.getDecimal("pos"));
        assertEquals(negScale, rt.getDecimal("neg"));
    }

    @Test
    void bigIntegerHundredsOfDigits() {
        BigInteger big = BigInteger.TEN.pow(500);
        MNode node = MNode.of("huge", big);
        MNode rt = roundTrip(node);
        assertEquals(big, rt.getVarint("huge"));
    }

    @Test
    void longZeroAndOne() {
        MNode node = MNode.of("zero", 0L, "one", 1L, "negOne", -1L);
        MNode rt = roundTrip(node);
        assertEquals(0L, rt.getLong("zero"));
        assertEquals(1L, rt.getLong("one"));
        assertEquals(-1L, rt.getLong("negOne"));
    }

    @Test
    void intZeroAndExtrema() {
        MNode node = MNode.of("zero", MNode.int32(0), "one", MNode.int32(1), "negOne", MNode.int32(-1));
        MNode rt = roundTrip(node);
        assertEquals(0, rt.getInt("zero"));
        assertEquals(1, rt.getInt("one"));
        assertEquals(-1, rt.getInt("negOne"));
    }

    @Test
    void shortZero() {
        MNode node = MNode.of("zero", MNode.int16((short) 0));
        MNode rt = roundTrip(node);
        assertEquals((short) 0, rt.getShort("zero"));
    }

    @Test
    void bigIntegerNegative() {
        BigInteger neg = BigInteger.TEN.pow(200).negate();
        MNode node = MNode.of("neg", neg);
        MNode rt = roundTrip(node);
        assertEquals(neg, rt.getVarint("neg"));
    }

    // ==================== 2. Float Special Values ====================

    @Test
    void doubleNaN() {
        MNode node = MNode.of("nan", Double.NaN);
        MNode rt = roundTrip(node);
        assertTrue(Double.isNaN(rt.getDouble("nan")));
    }

    @Test
    void floatNaN() {
        MNode node = MNode.of("nan", MNode.float32(Float.NaN));
        MNode rt = roundTrip(node);
        assertTrue(Float.isNaN(rt.getFloat("nan")));
    }

    @Test
    void halfNaN() {
        MNode node = MNode.of("nan", MNode.half(Float.NaN));
        MNode rt = roundTrip(node);
        assertTrue(Float.isNaN(rt.getFloat("nan")));
    }

    @Test
    void doubleInfinity() {
        MNode node = MNode.of("posInf", Double.POSITIVE_INFINITY, "negInf", Double.NEGATIVE_INFINITY);
        MNode rt = roundTrip(node);
        assertEquals(Double.POSITIVE_INFINITY, rt.getDouble("posInf"));
        assertEquals(Double.NEGATIVE_INFINITY, rt.getDouble("negInf"));
    }

    @Test
    void floatInfinity() {
        MNode node = MNode.of("posInf", MNode.float32(Float.POSITIVE_INFINITY), "negInf", MNode.float32(Float.NEGATIVE_INFINITY));
        MNode rt = roundTrip(node);
        assertEquals(Float.POSITIVE_INFINITY, rt.getFloat("posInf"));
        assertEquals(Float.NEGATIVE_INFINITY, rt.getFloat("negInf"));
    }

    @Test
    void halfInfinity() {
        MNode node = MNode.of("posInf", MNode.half(Float.POSITIVE_INFINITY), "negInf", MNode.half(Float.NEGATIVE_INFINITY));
        MNode rt = roundTrip(node);
        assertEquals(Float.POSITIVE_INFINITY, rt.getFloat("posInf"));
        assertEquals(Float.NEGATIVE_INFINITY, rt.getFloat("negInf"));
    }

    @Test
    void doubleNegativeZero() {
        MNode node = MNode.of("nz", -0.0);
        MNode rt = roundTrip(node);
        double val = rt.getDouble("nz");
        assertEquals(Double.doubleToRawLongBits(-0.0), Double.doubleToRawLongBits(val),
            "negative zero bit pattern must be preserved");
    }

    @Test
    void floatNegativeZero() {
        MNode node = MNode.of("nz", MNode.float32(-0.0f));
        MNode rt = roundTrip(node);
        float val = rt.getFloat("nz");
        assertEquals(Float.floatToRawIntBits(-0.0f), Float.floatToRawIntBits(val),
            "negative zero bit pattern must be preserved");
    }

    @Test
    void halfNegativeZero() {
        MNode node = MNode.of("nz", MNode.half(-0.0f));
        MNode rt = roundTrip(node);
        float val = rt.getFloat("nz");
        // half -0.0 widens to float -0.0
        assertEquals(Float.floatToRawIntBits(-0.0f), Float.floatToRawIntBits(val),
            "half negative zero bit pattern must be preserved");
    }

    @Test
    void arrayOfSpecialFloats() {
        double[] specials = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, -0.0, 0.0, Double.MIN_VALUE};
        MNode node = MNode.of("arr", MNode.array(specials));
        MNode rt = roundTrip(node);
        double[] result = rt.getArray("arr", double[].class);
        assertEquals(specials.length, result.length);
        assertTrue(Double.isNaN(result[0]));
        assertEquals(Double.POSITIVE_INFINITY, result[1]);
        assertEquals(Double.NEGATIVE_INFINITY, result[2]);
        assertEquals(Double.doubleToRawLongBits(-0.0), Double.doubleToRawLongBits(result[3]));
        assertEquals(0.0, result[4]);
        assertEquals(Double.MIN_VALUE, result[5]);
    }

    // ==================== 3. String & Bytes Edge Cases ====================

    @Test
    void emptyStringRoundTrips() {
        MNode node = MNode.of(
            "str", "",
            "txt", MNode.text(""),
            "asc", MNode.ascii(" ") // ascii requires printable, empty might not work — use space
        );
        MNode rt = roundTrip(node);
        assertEquals("", rt.getString("str"));
        assertEquals("", rt.getString("txt"));
        assertEquals(" ", rt.getString("asc"));
    }

    @Test
    void emptyByteArray() {
        MNode node = MNode.of("empty", new byte[0]);
        MNode rt = roundTrip(node);
        assertArrayEquals(new byte[0], rt.getBytes("empty"));
    }

    @Test
    void utf8FourByteSequences() {
        // Emoji and supplementary plane characters (4-byte UTF-8)
        String emoji = "\uD83D\uDE00\uD83D\uDE80\uD83C\uDF1F"; // 😀🚀🌟
        MNode node = MNode.of("emoji", emoji);
        MNode rt = roundTrip(node);
        assertEquals(emoji, rt.getString("emoji"));
    }

    @Test
    void stringWithEmbeddedNull() {
        String withNull = "hello\u0000world";
        MNode node = MNode.of("nulled", withNull);
        MNode rt = roundTrip(node);
        assertEquals(withNull, rt.getString("nulled"));
    }

    @Test
    void asciiBoundaryChars() {
        // ASCII printable range: 0x20 (space) through 0x7E (tilde)
        String ascii = " ~";
        MNode node = MNode.of("bounds", MNode.ascii(ascii));
        MNode rt = roundTrip(node);
        assertEquals(ascii, rt.getString("bounds"));
    }

    @Test
    void largeBlob() {
        byte[] blob = new byte[65536];
        new Random(42).nextBytes(blob);
        MNode node = MNode.of("big", blob);
        MNode rt = roundTrip(node);
        assertArrayEquals(blob, rt.getBytes("big"));
    }

    // ==================== 4. Buffer Corruption / Truncation ====================

    @Test
    void emptyBufferThrows() {
        byte[] empty = new byte[0];
        assertThrows(Exception.class, () -> MNode.fromBytes(empty));
    }

    @Test
    void truncatedFieldCountOnly() {
        // Dialect leader + field count says 1, but no field data follows
        ByteBuffer buf = ByteBuffer.allocate(1 + 2).order(ByteOrder.LITTLE_ENDIAN);
        buf.put(MNode.DIALECT);
        buf.putShort((short) 1);
        assertThrows(Exception.class, () -> MNode.fromBytes(buf.array()));
    }

    @Test
    void truncatedFieldName() {
        // nameLen says 10 but buffer has only 5 bytes of name data
        ByteBuffer buf = ByteBuffer.allocate(1 + 2 + 2 + 5).order(ByteOrder.LITTLE_ENDIAN);
        buf.put(MNode.DIALECT);
        buf.putShort((short) 1); // 1 field
        buf.putShort((short) 10); // nameLen = 10
        buf.put(new byte[5]); // only 5 bytes available
        assertThrows(Exception.class, () -> MNode.fromBytes(buf.array()));
    }

    @Test
    void truncatedValuePayload() {
        // A bytes field claiming length=1000 but buffer is too short
        ByteBuffer buf = ByteBuffer.allocate(1 + 2 + 2 + 1 + 1 + 4 + 3).order(ByteOrder.LITTLE_ENDIAN);
        buf.put(MNode.DIALECT);
        buf.putShort((short) 1); // 1 field
        buf.putShort((short) 1); // nameLen = 1
        buf.put((byte) 'x');    // field name
        buf.put((byte) 4);      // TAG_BYTES
        buf.putInt(1000);        // bytes length = 1000
        buf.put(new byte[3]);   // only 3 bytes
        assertThrows(Exception.class, () -> MNode.fromBytes(buf.array()));
    }

    @Test
    void invalidTypeTagThrows() {
        ByteBuffer buf = ByteBuffer.allocate(1 + 2 + 2 + 1 + 1).order(ByteOrder.LITTLE_ENDIAN);
        buf.put(MNode.DIALECT);
        buf.putShort((short) 1); // 1 field
        buf.putShort((short) 1); // nameLen = 1
        buf.put((byte) 'x');    // field name
        buf.put((byte) 99);     // invalid type tag
        assertThrows(Exception.class, () -> MNode.fromBytes(buf.array()));
    }

    @Test
    void singleByteBufferThrows() {
        // Dialect leader alone is not enough — needs field count
        assertThrows(Exception.class, () -> MNode.fromBytes(new byte[]{0x01}));
    }

    @Test
    void invalidTypedArrayElementTag() {
        // Build a valid MNode with 1 field, type=ARRAY(26), then an invalid element tag
        ByteBuffer buf = ByteBuffer.allocate(1 + 2 + 2 + 1 + 1 + 1 + 4).order(ByteOrder.LITTLE_ENDIAN);
        buf.put(MNode.DIALECT);
        buf.putShort((short) 1); // 1 field
        buf.putShort((short) 1); // nameLen
        buf.put((byte) 'a');    // name
        buf.put((byte) 26);     // TAG_ARRAY
        buf.put((byte) 99);     // invalid element tag
        buf.putInt(0);           // count = 0
        assertThrows(Exception.class, () -> MNode.fromBytes(buf.array()));
    }

    @Test
    void corruptedSetWithDuplicateElement() {
        // Manually craft binary for a SET with duplicate string elements
        byte[] elemBytes = "dup".getBytes();
        int elemSize = 1 + 4 + elemBytes.length; // tag + len + data
        ByteBuffer buf = ByteBuffer.allocate(1 + 2 + 2 + 1 + 1 + 4 + elemSize * 2).order(ByteOrder.LITTLE_ENDIAN);
        buf.put(MNode.DIALECT);
        buf.putShort((short) 1); // 1 field
        buf.putShort((short) 1); // nameLen
        buf.put((byte) 's');    // name
        buf.put((byte) 27);     // TAG_SET
        buf.putInt(2);           // 2 elements
        // element 1
        buf.put((byte) 0);      // TAG_TEXT
        buf.putInt(elemBytes.length);
        buf.put(elemBytes);
        // element 2 (duplicate)
        buf.put((byte) 0);      // TAG_TEXT
        buf.putInt(elemBytes.length);
        buf.put(elemBytes);
        assertThrows(IllegalArgumentException.class, () -> MNode.fromBytes(buf.array()));
    }

    @Test
    void fromBufferPayloadLengthExceedsRemaining() {
        // encode says payload is 10000 bytes but buffer only has 11 (dialect + 10)
        ByteBuffer buf = ByteBuffer.allocate(4 + 1 + 10).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(10000); // payload length
        buf.put(MNode.DIALECT);
        buf.put(new byte[10]);
        buf.flip();
        assertThrows(Exception.class, () -> MNode.fromBuffer(buf));
    }

    @Test
    void fuzzRandomBytesNoCrash() {
        Random rng = new Random(12345);
        for (int i = 0; i < 100; i++) {
            byte[] garbage = new byte[rng.nextInt(200)];
            rng.nextBytes(garbage);
            try {
                MNode.fromBytes(garbage);
                // If it decodes, that's fine — we just care it doesn't hang or OOME
            } catch (Exception e) {
                // Expected for garbage input
            }
        }
    }

    // ==================== 5. Container Edge Cases ====================

    @Test
    void emptyTypedArrays() {
        MNode node = MNode.of(
            "longs", MNode.array(new long[0]),
            "ints", MNode.array(new int[0]),
            "shorts", MNode.array(new short[0]),
            "doubles", MNode.array(new double[0]),
            "floats", MNode.array(new float[0]),
            "bools", MNode.array(new boolean[0]),
            "halves", MNode.array(new Half[0])
        );
        MNode rt = roundTrip(node);
        assertEquals(0, rt.getArray("longs", long[].class).length);
        assertEquals(0, rt.getArray("ints", int[].class).length);
        assertEquals(0, rt.getArray("shorts", short[].class).length);
        assertEquals(0, rt.getArray("doubles", double[].class).length);
        assertEquals(0, rt.getArray("floats", float[].class).length);
        assertEquals(0, rt.getArray("bools", boolean[].class).length);
        assertEquals(0, rt.getArray("halves", Half[].class).length);
    }

    @Test
    void singleElementTypedArrays() {
        MNode node = MNode.of(
            "l", MNode.array(new long[]{42L}),
            "d", MNode.array(new double[]{3.14}),
            "b", MNode.array(new boolean[]{true})
        );
        MNode rt = roundTrip(node);
        assertArrayEquals(new long[]{42L}, rt.getArray("l", long[].class));
        assertArrayEquals(new double[]{3.14}, rt.getArray("d", double[].class), 0.0);
        assertArrayEquals(new boolean[]{true}, rt.getArray("b", boolean[].class));
    }

    @Test
    void emptySetRoundTrip() {
        Set<Object> emptySet = new LinkedHashSet<>();
        MNode node = MNode.of("s", emptySet);
        MNode rt = roundTrip(node);
        assertTrue(rt.getSet("s").isEmpty());
    }

    @Test
    void emptyTypedMapRoundTrip() {
        Map<Object, Object> emptyMap = new LinkedHashMap<>();
        MNode node = MNode.of("m", MNode.typedMap(emptyMap));
        MNode rt = roundTrip(node);
        assertTrue(rt.getTypedMap("m").isEmpty());
    }

    @Test
    void deeplyNestedNodes() {
        MNode inner = MNode.of("val", 0L);
        for (int i = 1; i <= 10; i++) {
            inner = MNode.of("level", (long) i, "child", inner);
        }
        MNode rt = roundTrip(inner);
        // Walk down 10 levels
        MNode cursor = rt;
        for (int i = 10; i >= 1; i--) {
            assertEquals((long) i, cursor.getLong("level"));
            cursor = cursor.getNode("child");
        }
        assertEquals(0L, cursor.getLong("val"));
    }

    @Test
    void nodeWithManyFields() {
        Object[] kvs = new Object[400];
        for (int i = 0; i < 200; i++) {
            kvs[i * 2] = "field" + i;
            kvs[i * 2 + 1] = (long) i;
        }
        MNode node = MNode.of(kvs);
        assertEquals(200, node.size());
        MNode rt = roundTrip(node);
        assertEquals(200, rt.size());
        for (int i = 0; i < 200; i++) {
            assertEquals((long) i, rt.getLong("field" + i));
        }
    }

    @Test
    void listOfAllNulls() {
        List<Object> nulls = new ArrayList<>();
        for (int i = 0; i < 5; i++) nulls.add(null);
        MNode node = MNode.of("nulls", nulls);
        MNode rt = roundTrip(node);
        List<?> result = rt.getList("nulls");
        assertEquals(5, result.size());
        for (Object elem : result) {
            assertNull(elem);
        }
    }

    @Test
    void setDeterminism() {
        // Build the same set of 100 long elements in different random orders,
        // verify they produce identical bytes
        List<Long> elements = new ArrayList<>();
        for (long i = 0; i < 100; i++) elements.add(i);

        List<Long> order1 = new ArrayList<>(elements);
        Collections.shuffle(order1, new Random(1));
        List<Long> order2 = new ArrayList<>(elements);
        Collections.shuffle(order2, new Random(2));

        Set<Object> set1 = new LinkedHashSet<>(order1);
        Set<Object> set2 = new LinkedHashSet<>(order2);

        MNode node1 = MNode.of("s", set1);
        MNode node2 = MNode.of("s", set2);
        assertArrayEquals(node1.toBytes(), node2.toBytes(),
            "Sets with same elements in different insertion order must produce identical bytes");
    }

    @Test
    void typedMapDeterminism() {
        Map<Object, Object> map1 = new LinkedHashMap<>();
        map1.put("beta", 2L);
        map1.put("alpha", 1L);
        map1.put("gamma", 3L);

        Map<Object, Object> map2 = new LinkedHashMap<>();
        map2.put("gamma", 3L);
        map2.put("alpha", 1L);
        map2.put("beta", 2L);

        MNode node1 = MNode.of("m", MNode.typedMap(map1));
        MNode node2 = MNode.of("m", MNode.typedMap(map2));
        assertArrayEquals(node1.toBytes(), node2.toBytes(),
            "Typed maps with same entries in different insertion order must produce identical bytes");
    }

    // ==================== 6. Temporal Extrema ====================

    @Test
    void instantEpoch() {
        Instant epoch = Instant.EPOCH;
        MNode node = MNode.of("millis", MNode.millis(epoch), "nanos", MNode.nanos(epoch));
        MNode rt = roundTrip(node);
        assertEquals(epoch, rt.getMillis("millis"));
        assertEquals(epoch, rt.getNanos("nanos"));
    }

    @Test
    void nanosAdjustmentExtrema() {
        Instant withZeroNano = Instant.ofEpochSecond(1000, 0);
        Instant withMaxNano = Instant.ofEpochSecond(1000, 999_999_999);
        MNode node = MNode.of(
            "zero", MNode.nanos(withZeroNano),
            "max", MNode.nanos(withMaxNano)
        );
        MNode rt = roundTrip(node);
        assertEquals(withZeroNano, rt.getNanos("zero"));
        assertEquals(withMaxNano, rt.getNanos("max"));
    }

    @Test
    void negativeEpochMillisAndNanos() {
        // Pre-1970 timestamp
        Instant pre1970millis = Instant.ofEpochMilli(-86400000L); // 1969-12-31
        Instant pre1970nanos = Instant.ofEpochSecond(-1, 500_000_000); // 0.5s before epoch
        MNode node = MNode.of(
            "millis", MNode.millis(pre1970millis),
            "nanos", MNode.nanos(pre1970nanos)
        );
        MNode rt = roundTrip(node);
        assertEquals(pre1970millis, rt.getMillis("millis"));
        assertEquals(pre1970nanos, rt.getNanos("nanos"));
    }

    @Test
    void millisNearLongMaxBoundary() {
        // Large millis value that doesn't overflow Instant.ofEpochMilli
        long largeMillis = Long.MAX_VALUE / 1000; // avoid ArithmeticException in Instant
        Instant large = Instant.ofEpochMilli(largeMillis);
        MNode node = MNode.of("large", MNode.millis(large));
        MNode rt = roundTrip(node);
        assertEquals(large, rt.getMillis("large"));
    }

    // ==================== 7. ID Type Edge Cases ====================

    @Test
    void uuidV1AllZerosWithVersionBits() {
        // UUID with version 1: version nibble at bits 12-15 of msb
        long msb = 0x0000_0000_0000_1000L; // version 1
        long lsb = 0x8000_0000_0000_0000L; // variant 2 (RFC 4122)
        UUID uuid = new UUID(msb, lsb);
        assertEquals(1, uuid.version());
        MNode node = MNode.of("id", MNode.uuidV1(uuid));
        MNode rt = roundTrip(node);
        assertEquals(uuid, rt.getUuidV1("id"));
    }

    @Test
    void uuidV7AllZerosWithVersionBits() {
        long msb = 0x0000_0000_0000_7000L; // version 7
        long lsb = 0x8000_0000_0000_0000L; // variant 2
        UUID uuid = new UUID(msb, lsb);
        assertEquals(7, uuid.version());
        MNode node = MNode.of("id", MNode.uuidV7(uuid));
        MNode rt = roundTrip(node);
        assertEquals(uuid, rt.getUuidV7("id"));
    }

    @Test
    void ulidAllZeros() {
        byte[] zeros = new byte[16];
        Ulid ulid = Ulid.of(zeros);
        MNode node = MNode.of("id", ulid);
        MNode rt = roundTrip(node);
        assertEquals(ulid, rt.getUlid("id"));
    }

    @Test
    void ulidMaxTimestamp() {
        // ULID max timestamp: 0xFFFFFFFFFFFF in first 6 bytes
        byte[] maxTs = new byte[16];
        Arrays.fill(maxTs, 0, 6, (byte) 0xFF);
        Ulid ulid = Ulid.of(maxTs);
        MNode node = MNode.of("id", ulid);
        MNode rt = roundTrip(node);
        assertEquals(ulid, rt.getUlid("id"));
    }

    // ==================== 8. Equality & HashCode Contracts ====================

    @Test
    void reflexiveSymmetricTransitive() {
        MNode a = MNode.of("x", 1L, "nested", MNode.of("y", "hello"));
        MNode b = MNode.of("x", 1L, "nested", MNode.of("y", "hello"));
        MNode c = MNode.of("x", 1L, "nested", MNode.of("y", "hello"));

        // Reflexive
        assertEquals(a, a);
        // Symmetric
        assertEquals(a, b);
        assertEquals(b, a);
        // Transitive
        assertEquals(b, c);
        assertEquals(a, c);
    }

    @Test
    void hashCodeConsistentAcrossSerializationBoundary() {
        MNode original = MNode.of("k", 42L, "data", new byte[]{1, 2, 3});
        MNode deserialized = roundTrip(original);
        assertEquals(original, deserialized);
        assertEquals(original.hashCode(), deserialized.hashCode());
    }

    @Test
    void differentFieldOrderNotEqual() {
        MNode ab = MNode.of("a", 1L, "b", 2L);
        MNode ba = MNode.of("b", 2L, "a", 1L);
        assertNotEquals(ab, ba, "Nodes with different field order should not be equal");
    }

    @Test
    void byteArrayValueEquality() {
        MNode n1 = MNode.of("data", new byte[]{10, 20, 30});
        MNode n2 = MNode.of("data", new byte[]{10, 20, 30});
        assertEquals(n1, n2, "Nodes with equal byte[] content should be equal");
        assertEquals(n1.hashCode(), n2.hashCode());
    }

    // ==================== 9. CDDL Codec Adversarial ====================

    @Test
    void cddlParseWithExtraWhitespace() {
        String text = "  {  name : text = \"hello\"  ,  val : int = 42  ,  }  ";
        MNode node = MNodeCddlCodec.parse(text);
        assertEquals("hello", node.getString("name"));
        assertEquals(42, node.getInt("val"));
    }

    @Test
    void cddlParseDeeplyNestedNodes() {
        // 5 levels deep
        String text = "{ a: node = { b: node = { c: node = { d: node = { e: long = 99 } } } } }";
        MNode node = MNodeCddlCodec.parse(text);
        assertEquals(99L, node.getNode("a").getNode("b").getNode("c").getNode("d").getLong("e"));
    }

    @Test
    void cddlFormatParseFormatIdempotent() {
        MNode original = MNode.of(
            "name", MNode.text("test"),
            "count", MNode.int32(7),
            "ratio", 3.14,
            "active", true,
            "data", new byte[]{0x0A, 0x0B}
        );
        String formatted1 = MNodeCddlCodec.format(original);
        MNode parsed = MNodeCddlCodec.parse(formatted1);
        String formatted2 = MNodeCddlCodec.format(parsed);
        assertEquals(formatted1, formatted2, "format → parse → format should be idempotent");
    }

    @Test
    void cddlParseEmptyNode() {
        MNode node = MNodeCddlCodec.parse("{}");
        assertEquals(0, node.size());
    }

    @Test
    void cddlRejectsUnclosedBrace() {
        assertThrows(Exception.class, () -> MNodeCddlCodec.parse("{ name: text = \"hello\""));
    }

    @Test
    void cddlRejectsMissingSeparator() {
        assertThrows(Exception.class, () -> MNodeCddlCodec.parse("{ name text \"hello\" }"));
    }

    // ==================== Helpers ====================

    /// Round-trip an MNode through toBytes/fromBytes
    private static MNode roundTrip(MNode node) {
        byte[] bytes = node.toBytes();
        MNode fromBytes = MNode.fromBytes(bytes);

        // Also verify encode/fromBuffer path
        ByteBuffer buf = ByteBuffer.allocate(bytes.length + 4);
        node.encode(buf);
        buf.flip();
        MNode fromBuffer = MNode.fromBuffer(buf);

        assertEquals(fromBytes, fromBuffer, "toBytes and encode paths must produce equivalent results");
        return fromBytes;
    }
}
