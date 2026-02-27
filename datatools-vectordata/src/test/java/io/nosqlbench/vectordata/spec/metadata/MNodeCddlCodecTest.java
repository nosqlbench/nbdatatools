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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/// Tests for {@link MNodeCddlCodec} format and parse round-trips.
class MNodeCddlCodecTest {

    @Test
    void formatSimpleScalars() {
        MNode node = MNode.of(
            "name", MNode.text("glove-100"),
            "dims", MNode.int32(128)
        );
        String text = MNodeCddlCodec.format(node);
        assertTrue(text.contains("name: text = \"glove-100\""));
        assertTrue(text.contains("dims: int = 128"));
    }

    @Test
    void roundTripSimpleScalars() {
        MNode original = MNode.of(
            "name", MNode.text("glove-100"),
            "dims", MNode.int32(128),
            "score", 0.95
        );
        String text = MNodeCddlCodec.format(original);
        MNode parsed = MNodeCddlCodec.parse(text);
        assertEquals("glove-100", parsed.getString("name"));
        assertEquals(128, parsed.getInt("dims"));
        assertEquals(0.95, parsed.getDouble("score"), 0.001);
    }

    @Test
    void roundTripNestedNode() {
        MNode inner = MNode.of("engine", MNode.text("hnsw"));
        MNode original = MNode.of(
            "name", MNode.text("dataset"),
            "config", inner
        );
        String text = MNodeCddlCodec.format(original);
        assertTrue(text.contains("config: node = {"));
        MNode parsed = MNodeCddlCodec.parse(text);
        assertEquals("dataset", parsed.getString("name"));
        assertEquals("hnsw", parsed.getNode("config").getString("engine"));
    }

    @Test
    void roundTripBoolAndNull() {
        MNode original = MNode.of(
            "active", true,
            "absent", (Object) null
        );
        String text = MNodeCddlCodec.format(original);
        assertTrue(text.contains("bool = true"));
        assertTrue(text.contains("null = null"));
        MNode parsed = MNodeCddlCodec.parse(text);
        assertTrue(parsed.getBoolean("active"));
        assertFalse(parsed.has("absent"));
    }

    @Test
    void roundTripBytes() {
        MNode original = MNode.of("data", new byte[]{0x0a, 0x1b, 0x2c});
        String text = MNodeCddlCodec.format(original);
        assertTrue(text.contains("h'0a1b2c'"));
        MNode parsed = MNodeCddlCodec.parse(text);
        assertArrayEquals(new byte[]{0x0a, 0x1b, 0x2c}, parsed.getBytes("data"));
    }

    @Test
    void roundTripFloatTypes() {
        MNode original = MNode.of(
            "f32", MNode.float32(1.5f),
            "f16", MNode.half(2.0f)
        );
        String text = MNodeCddlCodec.format(original);
        assertTrue(text.contains("1.5f"));
        assertTrue(text.contains("2.0h"));
        MNode parsed = MNodeCddlCodec.parse(text);
        assertEquals(1.5f, parsed.getFloat("f32"), 0.01f);
        assertEquals(2.0f, parsed.getFloat("f16"), 0.1f);
    }

    @Test
    void roundTripDecimalAndVarint() {
        MNode original = MNode.of(
            "price", new BigDecimal("123.45"),
            "big", new BigInteger("999999999")
        );
        String text = MNodeCddlCodec.format(original);
        assertTrue(text.contains("decimal(\"123.45\")"));
        assertTrue(text.contains("varint(\"999999999\")"));
        MNode parsed = MNodeCddlCodec.parse(text);
        assertEquals(new BigDecimal("123.45"), parsed.getDecimal("price"));
        assertEquals(new BigInteger("999999999"), parsed.getVarint("big"));
    }

    @Test
    void roundTripTemporalMillisAndNanos() {
        Instant millis = Instant.ofEpochMilli(1708876800000L);
        Instant nanos = Instant.ofEpochSecond(1708876800L, 123456789);
        MNode original = MNode.of(
            "m", MNode.millis(millis),
            "n", MNode.nanos(nanos)
        );
        String text = MNodeCddlCodec.format(original);
        assertTrue(text.contains("millis(\""));
        assertTrue(text.contains("nanos(\""));
        MNode parsed = MNodeCddlCodec.parse(text);
        assertEquals(millis, parsed.getMillis("m"));
        assertEquals(nanos, parsed.getNanos("n"));
    }

    @Test
    void roundTripDateTimeAndDateAndTime() {
        LocalDate date = LocalDate.of(2026, 2, 25);
        LocalTime time = LocalTime.of(14, 30, 0);
        Instant dt = Instant.parse("2026-02-25T14:30:00Z");
        MNode original = MNode.of(
            "d", date,
            "t", time,
            "dt", MNode.datetime(dt)
        );
        String text = MNodeCddlCodec.format(original);
        MNode parsed = MNodeCddlCodec.parse(text);
        assertEquals(date, parsed.getDate("d"));
        assertEquals(time, parsed.getTime("t"));
        assertEquals(dt, parsed.getDateTime("dt"));
    }

    @Test
    void roundTripUuids() {
        UUID v1 = new UUID(0x0000000000001000L, 0x8000000000000000L);
        UUID v7 = new UUID(0x0000000000007000L, 0x8000000000000000L);
        MNode original = MNode.of(
            "v1", MNode.uuidV1(v1),
            "v7", MNode.uuidV7(v7)
        );
        String text = MNodeCddlCodec.format(original);
        MNode parsed = MNodeCddlCodec.parse(text);
        assertEquals(v1, parsed.getUuidV1("v1"));
        assertEquals(v7, parsed.getUuidV7("v7"));
    }

    @Test
    void roundTripUlid() {
        Ulid ulid = Ulid.of("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        MNode original = MNode.of("id", ulid);
        String text = MNodeCddlCodec.format(original);
        assertTrue(text.contains("\"01ARZ3NDEKTSV4RRFFQ69G5FAV\""));
        MNode parsed = MNodeCddlCodec.parse(text);
        assertEquals(ulid, parsed.getUlid("id"));
    }

    @Test
    void roundTripTypedArray() {
        float[] arr = {1.0f, 2.0f, 3.0f};
        MNode original = MNode.of("vecs", MNode.array(arr));
        String text = MNodeCddlCodec.format(original);
        assertTrue(text.contains("[float32]"));
        MNode parsed = MNodeCddlCodec.parse(text);
        assertArrayEquals(arr, parsed.getArray("vecs", float[].class), 0.01f);
    }

    @Test
    void roundTripEnumStr() {
        MNode original = MNode.of("metric", MNode.enumVal("angular"));
        String text = MNodeCddlCodec.format(original);
        assertTrue(text.contains("enum_str = \"angular\""));
        MNode parsed = MNodeCddlCodec.parse(text);
        assertEquals("angular", parsed.getEnum("metric"));
    }

    @Test
    void roundTripEnumOrd() {
        MNode original = MNode.of("metric", MNode.enumOrd(1));
        String text = MNodeCddlCodec.format(original);
        assertTrue(text.contains("enum_ord = 1"));
        MNode parsed = MNodeCddlCodec.parse(text);
        assertEquals(1, parsed.getEnumOrdinal("metric"));
    }

    @Test
    void parseRejectsInvalidInput() {
        assertThrows(IllegalArgumentException.class, () -> MNodeCddlCodec.parse("not a node"));
        assertThrows(IllegalArgumentException.class, () -> MNodeCddlCodec.parse("{ bad }"));
    }

    @Test
    void formatAndParseComplexNode() {
        MNode original = MNode.of(
            "name", MNode.text("benchmark"),
            "dims", MNode.int32(128),
            "score", 0.95,
            "active", true,
            "data", new byte[]{0x0a},
            "config", MNode.of("engine", MNode.text("hnsw")),
            "vectors", MNode.array(new float[]{1.0f, 2.0f})
        );
        String text = MNodeCddlCodec.format(original);
        MNode parsed = MNodeCddlCodec.parse(text);
        assertEquals("benchmark", parsed.getString("name"));
        assertEquals(128, parsed.getInt("dims"));
        assertEquals(0.95, parsed.getDouble("score"), 0.001);
        assertTrue(parsed.getBoolean("active"));
        assertArrayEquals(new byte[]{0x0a}, parsed.getBytes("data"));
        assertEquals("hnsw", parsed.getNode("config").getString("engine"));
        assertArrayEquals(new float[]{1.0f, 2.0f}, parsed.getArray("vectors", float[].class), 0.01f);
    }

    @Test
    void roundTripShortAndLong() {
        MNode original = MNode.of(
            "s", MNode.int16((short) 5),
            "l", 42L
        );
        String text = MNodeCddlCodec.format(original);
        MNode parsed = MNodeCddlCodec.parse(text);
        assertEquals((short) 5, parsed.getShort("s"));
        assertEquals(42L, parsed.getLong("l"));
    }

    @Test
    void roundTripStringEscaping() {
        MNode original = MNode.of("msg", MNode.text("hello \"world\"\nnewline"));
        String text = MNodeCddlCodec.format(original);
        assertTrue(text.contains("\\\"world\\\""));
        assertTrue(text.contains("\\n"));
        MNode parsed = MNodeCddlCodec.parse(text);
        assertEquals("hello \"world\"\nnewline", parsed.getString("msg"));
    }
}
