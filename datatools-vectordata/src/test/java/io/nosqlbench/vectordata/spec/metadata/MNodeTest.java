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
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/// Round-trip and accessor tests for {@link MNode}.
class MNodeTest {

    // ==================== Original tests (backward compatibility) ====================

    @Test
    void roundTripScalars() {
        MNode original = MNode.of(
            "name", "test-dataset",
            "count", 42L,
            "score", 3.14,
            "active", true,
            "blob", new byte[]{1, 2, 3}
        );

        byte[] bytes = original.toBytes();
        MNode decoded = MNode.fromBytes(bytes);

        assertEquals("test-dataset", decoded.getString("name"));
        assertEquals(42L, decoded.getLong("count"));
        assertEquals(3.14, decoded.getDouble("score"), 0.001);
        assertTrue(decoded.getBoolean("active"));
        assertArrayEquals(new byte[]{1, 2, 3}, decoded.getBytes("blob"));
    }

    @Test
    void roundTripByteBuffer() {
        MNode original = MNode.of("key", "value", "num", 100L);

        ByteBuffer buf = ByteBuffer.allocate(1024);
        original.encode(buf);
        buf.flip();

        MNode decoded = MNode.fromBuffer(buf);
        assertEquals("value", decoded.getString("key"));
        assertEquals(100L, decoded.getLong("num"));
        assertEquals(0, buf.remaining());
    }

    @Test
    void roundTripNullValue() {
        MNode original = MNode.of("present", "yes", "absent", null);

        byte[] bytes = original.toBytes();
        MNode decoded = MNode.fromBytes(bytes);

        assertEquals("yes", decoded.getString("present"));
        assertEquals(2, decoded.size());
        assertFalse(decoded.has("absent"));
    }

    @Test
    void integerPromotedToLong() {
        MNode node = MNode.of("val", 7);
        assertEquals(7L, node.getLong("val"));
    }

    @Test
    void floatPromotedToDouble() {
        MNode node = MNode.of("val", 1.5f);
        assertEquals(1.5, node.getDouble("val"), 0.001);
    }

    @Test
    void typedAccessorsThrowOnMismatch() {
        MNode node = MNode.of("name", "hello", "count", 42L);

        assertThrows(IllegalArgumentException.class, () -> node.getLong("name"));
        assertThrows(IllegalArgumentException.class, () -> node.getString("count"));
        assertThrows(IllegalArgumentException.class, () -> node.getBoolean("name"));
        assertThrows(IllegalArgumentException.class, () -> node.getBytes("name"));
    }

    @Test
    void missingKeyThrows() {
        MNode node = MNode.of("a", "b");
        assertThrows(IllegalArgumentException.class, () -> node.getString("missing"));
    }

    @Test
    void optionalVariantsReturnEmptyForMissingKeys() {
        MNode node = MNode.of("name", "hello");

        assertEquals(Optional.of("hello"), node.findString("name"));
        assertEquals(Optional.empty(), node.findString("missing"));
        assertEquals(Optional.empty(), node.findLong("missing"));
        assertEquals(Optional.empty(), node.findDouble("missing"));
        assertEquals(Optional.empty(), node.findBoolean("missing"));
        assertEquals(Optional.empty(), node.findBytes("missing"));
    }

    @Test
    void getLongAcceptsInteger() {
        MNode node = MNode.of("val", 7);
        assertEquals(7L, node.getLong("val"));
    }

    @Test
    void getDoubleAcceptsLong() {
        // Note: with the new widening rules, getDouble no longer accepts LONG tags.
        // LONG is an integer type, DOUBLE is a float type. Cross-family widening is not allowed.
        MNode node = MNode.of("val", 42.0);
        assertEquals(42.0, node.getDouble("val"), 0.001);
    }

    @Test
    void toMapReturnsUnmodifiableView() {
        MNode node = MNode.of("k", "v");
        Map<String, Object> map = node.toMap();
        assertThrows(UnsupportedOperationException.class, () -> map.put("new", "val"));
        assertEquals("v", map.get("k"));
    }

    @Test
    void rawGetAndHas() {
        MNode node = MNode.of("k", "v");
        assertEquals("v", node.get("k"));
        assertNull(node.get("missing"));
        assertTrue(node.has("k"));
        assertFalse(node.has("missing"));
    }

    @Test
    void ofRejectsOddArgs() {
        assertThrows(IllegalArgumentException.class, () -> MNode.of("key"));
    }

    @Test
    void ofRejectsNonStringKey() {
        assertThrows(IllegalArgumentException.class, () -> MNode.of(42, "value"));
    }

    @Test
    void ofRejectsUnsupportedValueType() {
        assertThrows(IllegalArgumentException.class, () -> MNode.of("key", new Object()));
    }

    @Test
    void multipleByteBufferEncodings() {
        MNode a = MNode.of("a", 1L);
        MNode b = MNode.of("b", 2L);

        ByteBuffer buf = ByteBuffer.allocate(1024);
        a.encode(buf);
        b.encode(buf);
        buf.flip();

        MNode da = MNode.fromBuffer(buf);
        MNode db = MNode.fromBuffer(buf);

        assertEquals(1L, da.getLong("a"));
        assertEquals(2L, db.getLong("b"));
    }

    @Test
    void emptyNode() {
        MNode node = MNode.of();
        assertEquals(0, node.size());
        assertFalse(node.has("anything"));

        byte[] bytes = node.toBytes();
        MNode decoded = MNode.fromBytes(bytes);
        assertEquals(0, decoded.size());
    }

    @Test
    void equalityAndHashCode() {
        MNode a = MNode.of("name", "test", "count", 42L);
        MNode b = MNode.of("name", "test", "count", 42L);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        MNode c = MNode.of("name", "test", "count", 99L);
        assertNotEquals(a, c);
    }

    @Test
    void equalityWithByteArrays() {
        MNode a = MNode.of("data", new byte[]{1, 2, 3});
        MNode b = MNode.of("data", new byte[]{1, 2, 3});
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        MNode c = MNode.of("data", new byte[]{4, 5, 6});
        assertNotEquals(a, c);
    }

    @Test
    void roundTripLargeBlob() {
        byte[] blob = new byte[4096];
        for (int i = 0; i < blob.length; i++) {
            blob[i] = (byte) (i & 0xFF);
        }
        MNode original = MNode.of("big", blob);
        MNode decoded = MNode.fromBytes(original.toBytes());
        assertArrayEquals(blob, decoded.getBytes("big"));
    }

    @Test
    void roundTripUnicodeFieldNames() {
        MNode original = MNode.of("日本語", "value", "émoji", 42L);
        MNode decoded = MNode.fromBytes(original.toBytes());
        assertEquals("value", decoded.getString("日本語"));
        assertEquals(42L, decoded.getLong("émoji"));
    }

    @Test
    void toStringShowsFields() {
        MNode node = MNode.of("name", "test", "count", 42L);
        String s = node.toString();
        assertTrue(s.contains("name=test"));
        assertTrue(s.contains("count=42"));
    }

    // ==================== Enum: self-describing (TAG_ENUM_STR) ====================

    @Test
    void enumValRoundTrip() {
        MNode original = MNode.of("status", MNode.enumVal("active"));
        MNode decoded = MNode.fromBytes(original.toBytes());
        assertEquals("active", decoded.getEnum("status"));
    }

    @Test
    void enumValWithOtherFields() {
        MNode original = MNode.of(
            "name", "test",
            "status", MNode.enumVal("active"),
            "count", 42L
        );
        MNode decoded = MNode.fromBytes(original.toBytes());
        assertEquals("test", decoded.getString("name"));
        assertEquals("active", decoded.getEnum("status"));
        assertEquals(42L, decoded.getLong("count"));
    }

    @Test
    void enumValByteBufferRoundTrip() {
        MNode original = MNode.of("color", MNode.enumVal("red"));
        ByteBuffer buf = ByteBuffer.allocate(1024);
        original.encode(buf);
        buf.flip();
        MNode decoded = MNode.fromBuffer(buf);
        assertEquals("red", decoded.getEnum("color"));
        assertEquals(0, buf.remaining());
    }

    @Test
    void enumValIsNotText() {
        MNode node = MNode.of("status", MNode.enumVal("active"));
        assertThrows(IllegalArgumentException.class, () -> node.getString("status"));
    }

    @Test
    void textIsNotEnum() {
        MNode node = MNode.of("name", "hello");
        assertThrows(IllegalArgumentException.class, () -> node.getEnum("name"));
    }

    @Test
    void findEnumOnEnumVal() {
        MNode node = MNode.of("status", MNode.enumVal("active"));
        assertEquals(Optional.of("active"), node.findEnum("status"));
        assertEquals(Optional.empty(), node.findEnum("missing"));
    }

    @Test
    void findEnumOnNonEnumField() {
        MNode node = MNode.of("name", "hello");
        assertEquals(Optional.empty(), node.findEnum("name"));
    }

    @Test
    void enumValEquality() {
        MNode a = MNode.of("status", MNode.enumVal("active"));
        MNode b = MNode.of("status", MNode.enumVal("active"));
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        MNode c = MNode.of("status", MNode.enumVal("inactive"));
        assertNotEquals(a, c);
    }

    @Test
    void enumValRejectsNull() {
        assertThrows(IllegalArgumentException.class, () -> MNode.enumVal(null));
    }

    // ==================== Enum: ordinal-encoded (TAG_ENUM_ORD) ====================

    @Test
    void enumOrdWithInlineDefinition() {
        MNode node = MNode.of("status", MNode.enumOrd(1, "inactive", "active", "suspended"));
        assertEquals("active", node.getEnum("status"));
        assertEquals(1, node.getEnumOrdinal("status"));
    }

    @Test
    void enumOrdRoundTripWithInlineDefinition() {
        MNode original = MNode.of("status", MNode.enumOrd(2, "red", "green", "blue"));
        byte[] bytes = original.toBytes();
        MNode decoded = MNode.fromBytes(bytes);
        assertEquals(2, decoded.getEnumOrdinal("status"));
        assertThrows(IllegalStateException.class, () -> decoded.getEnum("status"));
    }

    @Test
    void enumOrdWithDeferredDefinition() {
        MNode original = MNode.of("status", MNode.enumOrd(1));
        byte[] bytes = original.toBytes();
        MNode decoded = MNode.fromBytes(bytes);
        assertThrows(IllegalStateException.class, () -> decoded.getEnum("status"));
        MNode resolved = decoded.withEnumDef("status", "inactive", "active", "suspended");
        assertEquals("active", resolved.getEnum("status"));
    }

    @Test
    void enumOrdByteBufferRoundTrip() {
        MNode original = MNode.of("color", MNode.enumOrd(0, "red", "green", "blue"));
        ByteBuffer buf = ByteBuffer.allocate(1024);
        original.encode(buf);
        buf.flip();
        MNode decoded = MNode.fromBuffer(buf);
        MNode resolved = decoded.withEnumDef("color", "red", "green", "blue");
        assertEquals("red", resolved.getEnum("color"));
    }

    @Test
    void enumOrdIsNotAccessibleAsOtherTypes() {
        MNode node = MNode.of("status", MNode.enumOrd(1, "a", "b"));
        assertThrows(IllegalArgumentException.class, () -> node.getString("status"));
        assertThrows(IllegalArgumentException.class, () -> node.getLong("status"));
        assertThrows(IllegalArgumentException.class, () -> node.getBoolean("status"));
    }

    @Test
    void getEnumOrdinalOnNonOrdinalThrows() {
        MNode node = MNode.of("status", MNode.enumVal("active"));
        assertThrows(IllegalArgumentException.class, () -> node.getEnumOrdinal("status"));
    }

    @Test
    void withEnumDefOnNonOrdinalThrows() {
        MNode node = MNode.of("name", "hello");
        assertThrows(IllegalArgumentException.class, () -> node.withEnumDef("name", "a", "b"));
    }

    @Test
    void withEnumDefOnMissingKeyThrows() {
        MNode node = MNode.of("a", 1L);
        assertThrows(IllegalArgumentException.class, () -> node.withEnumDef("missing", "x"));
    }

    @Test
    void enumOrdOutOfRangeAtConstructionThrows() {
        assertThrows(IllegalArgumentException.class,
            () -> MNode.enumOrd(5, "a", "b", "c"));
        assertThrows(IllegalArgumentException.class,
            () -> MNode.enumOrd(-1, "a", "b"));
    }

    @Test
    void enumOrdEquality() {
        MNode a = MNode.of("status", MNode.enumOrd(1));
        MNode b = MNode.of("status", MNode.enumOrd(1));
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        MNode c = MNode.of("status", MNode.enumOrd(2));
        assertNotEquals(a, c);
    }

    @Test
    void findEnumOnOrdinal() {
        MNode node = MNode.of("status", MNode.enumOrd(0, "x", "y"));
        assertEquals(Optional.of("x"), node.findEnum("status"));
    }

    @Test
    void mixedEnumAndScalarFields() {
        MNode original = MNode.of(
            "name", "dataset",
            "dims", 128L,
            "metric", MNode.enumVal("angular"),
            "label_count", MNode.enumOrd(2, "few", "moderate", "many"),
            "normalized", true
        );
        MNode decoded = MNode.fromBytes(original.toBytes());

        assertEquals("dataset", decoded.getString("name"));
        assertEquals(128L, decoded.getLong("dims"));
        assertEquals("angular", decoded.getEnum("metric"));
        assertEquals(2, decoded.getEnumOrdinal("label_count"));
        assertTrue(decoded.getBoolean("normalized"));

        MNode resolved = decoded.withEnumDef("label_count", "few", "moderate", "many");
        assertEquals("many", resolved.getEnum("label_count"));
    }

    @Test
    void toStringShowsEnumTypes() {
        MNode node = MNode.of(
            "status", MNode.enumVal("active"),
            "level", MNode.enumOrd(1, "low", "high")
        );
        String s = node.toString();
        assertTrue(s.contains("enum(active)"));
        assertTrue(s.contains("enum#1(high)"));
    }

    // ==================== LIST ====================

    @Test
    void listRoundTripHomogeneous() {
        MNode original = MNode.of("tags", List.of("ann", "benchmark", "glove"));
        MNode decoded = MNode.fromBytes(original.toBytes());
        assertEquals(List.of("ann", "benchmark", "glove"), decoded.getList("tags"));
    }

    @Test
    void listRoundTripHeterogeneous() {
        MNode original = MNode.of("mixed", List.of("hello", 42L, 3.14, true));
        MNode decoded = MNode.fromBytes(original.toBytes());
        List<Object> list = decoded.getList("mixed");
        assertEquals("hello", list.get(0));
        assertEquals(42L, list.get(1));
        assertEquals(3.14, (Double) list.get(2), 0.001);
        assertEquals(true, list.get(3));
    }

    @Test
    void listWithBytesElement() {
        byte[] blob = new byte[]{10, 20, 30};
        MNode original = MNode.of("data", List.of(blob));
        MNode decoded = MNode.fromBytes(original.toBytes());
        List<Object> list = decoded.getList("data");
        assertArrayEquals(blob, (byte[]) list.get(0));
    }

    @Test
    void listWithNullElement() {
        List<Object> input = new java.util.ArrayList<>();
        input.add("before");
        input.add(null);
        input.add("after");
        MNode original = MNode.of("items", input);
        MNode decoded = MNode.fromBytes(original.toBytes());
        List<Object> list = decoded.getList("items");
        assertEquals("before", list.get(0));
        assertNull(list.get(1));
        assertEquals("after", list.get(2));
    }

    @Test
    void emptyListRoundTrip() {
        MNode original = MNode.of("empty", List.of());
        MNode decoded = MNode.fromBytes(original.toBytes());
        assertEquals(List.of(), decoded.getList("empty"));
    }

    @Test
    void listByteBufferRoundTrip() {
        MNode original = MNode.of("nums", List.of(1L, 2L, 3L));
        ByteBuffer buf = ByteBuffer.allocate(1024);
        original.encode(buf);
        buf.flip();
        MNode decoded = MNode.fromBuffer(buf);
        assertEquals(List.of(1L, 2L, 3L), decoded.getList("nums"));
        assertEquals(0, buf.remaining());
    }

    @Test
    void listAccessorThrowsOnNonList() {
        MNode node = MNode.of("name", "hello");
        assertThrows(IllegalArgumentException.class, () -> node.getList("name"));
    }

    @Test
    void findListPresent() {
        MNode node = MNode.of("tags", List.of("a", "b"));
        assertEquals(Optional.of(List.of("a", "b")), node.findList("tags"));
    }

    @Test
    void findListMissing() {
        MNode node = MNode.of("name", "hello");
        assertEquals(Optional.empty(), node.findList("missing"));
        assertEquals(Optional.empty(), node.findList("name"));
    }

    @Test
    void listIntegerPromotion() {
        List<Object> input = new java.util.ArrayList<>();
        input.add(7);       // Integer
        input.add(42L);     // Long
        MNode original = MNode.of("vals", input);
        MNode decoded = MNode.fromBytes(original.toBytes());
        List<Object> list = decoded.getList("vals");
        assertEquals(7L, list.get(0));
        assertEquals(42L, list.get(1));
    }

    @Test
    void nestedListRoundTrip() {
        List<Object> inner = List.of(1L, 2L);
        List<Object> outer = List.of("header", inner);
        MNode original = MNode.of("nested", outer);
        MNode decoded = MNode.fromBytes(original.toBytes());
        List<Object> result = decoded.getList("nested");
        assertEquals("header", result.get(0));
        assertEquals(List.of(1L, 2L), result.get(1));
    }

    @Test
    void listEquality() {
        MNode a = MNode.of("tags", List.of("x", "y"));
        MNode b = MNode.of("tags", List.of("x", "y"));
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        MNode c = MNode.of("tags", List.of("x", "z"));
        assertNotEquals(a, c);
    }

    // ==================== MAP (nested MNode) ====================

    @Test
    void nestedNodeRoundTrip() {
        MNode inner = MNode.of("x", 1L, "y", 2L);
        MNode original = MNode.of("metadata", inner);
        MNode decoded = MNode.fromBytes(original.toBytes());
        MNode nested = decoded.getNode("metadata");
        assertEquals(1L, nested.getLong("x"));
        assertEquals(2L, nested.getLong("y"));
    }

    @Test
    void nestedNodeByteBufferRoundTrip() {
        MNode inner = MNode.of("color", "red");
        MNode original = MNode.of("config", inner);
        ByteBuffer buf = ByteBuffer.allocate(1024);
        original.encode(buf);
        buf.flip();
        MNode decoded = MNode.fromBuffer(buf);
        assertEquals("red", decoded.getNode("config").getString("color"));
        assertEquals(0, buf.remaining());
    }

    @Test
    void nestedNodeAccessorThrowsOnNonMap() {
        MNode node = MNode.of("name", "hello");
        assertThrows(IllegalArgumentException.class, () -> node.getNode("name"));
    }

    @Test
    void findNodePresent() {
        MNode inner = MNode.of("k", "v");
        MNode node = MNode.of("sub", inner);
        assertTrue(node.findNode("sub").isPresent());
        assertEquals("v", node.findNode("sub").get().getString("k"));
    }

    @Test
    void findNodeMissing() {
        MNode node = MNode.of("name", "hello");
        assertEquals(Optional.empty(), node.findNode("missing"));
        assertEquals(Optional.empty(), node.findNode("name"));
    }

    @Test
    void deeplyNestedNodes() {
        MNode level2 = MNode.of("val", 42L);
        MNode level1 = MNode.of("inner", level2, "label", "mid");
        MNode root = MNode.of("outer", level1, "top", true);

        MNode decoded = MNode.fromBytes(root.toBytes());
        assertTrue(decoded.getBoolean("top"));
        assertEquals("mid", decoded.getNode("outer").getString("label"));
        assertEquals(42L, decoded.getNode("outer").getNode("inner").getLong("val"));
    }

    @Test
    void nestedNodeEquality() {
        MNode inner = MNode.of("a", 1L);
        MNode a = MNode.of("sub", inner);
        MNode b = MNode.of("sub", MNode.of("a", 1L));
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void listContainingNestedNode() {
        MNode nested = MNode.of("id", 99L);
        List<Object> listVal = List.of("text", nested);
        MNode original = MNode.of("items", listVal);
        MNode decoded = MNode.fromBytes(original.toBytes());
        List<Object> result = decoded.getList("items");
        assertEquals("text", result.get(0));
        MNode resultNode = (MNode) result.get(1);
        assertEquals(99L, resultNode.getLong("id"));
    }

    @Test
    void mixedScalarsListAndMap() {
        MNode inner = MNode.of("engine", "hnsw");
        MNode original = MNode.of(
            "name", "dataset",
            "dims", 128L,
            "tags", List.of("ann", "benchmark"),
            "config", inner,
            "active", true
        );
        MNode decoded = MNode.fromBytes(original.toBytes());
        assertEquals("dataset", decoded.getString("name"));
        assertEquals(128L, decoded.getLong("dims"));
        assertEquals(List.of("ann", "benchmark"), decoded.getList("tags"));
        assertEquals("hnsw", decoded.getNode("config").getString("engine"));
        assertTrue(decoded.getBoolean("active"));
    }

    @Test
    void toStringShowsListAndMap() {
        MNode inner = MNode.of("k", "v");
        MNode node = MNode.of("tags", List.of("a", "b"), "sub", inner);
        String s = node.toString();
        assertTrue(s.contains("tags="));
        assertTrue(s.contains("sub="));
    }

    // ==================== TEXT types (tags 10, 11) ====================

    @Test
    void textValidatedRoundTrip() {
        MNode original = MNode.of("label", MNode.text("Hello, World!"));
        MNode decoded = MNode.fromBytes(original.toBytes());
        assertEquals("Hello, World!", decoded.getString("label"));
    }

    @Test
    void asciiRoundTrip() {
        MNode original = MNode.of("id", MNode.ascii("ABC-123"));
        MNode decoded = MNode.fromBytes(original.toBytes());
        assertEquals("ABC-123", decoded.getString("id"));
    }

    @Test
    void getStringWidensAcrossTextTypes() {
        MNode node = MNode.of(
            "s", "bare string",
            "t", MNode.text("validated text"),
            "a", MNode.ascii("ascii text")
        );
        // All three should be accessible via getString()
        assertEquals("bare string", node.getString("s"));
        assertEquals("validated text", node.getString("t"));
        assertEquals("ascii text", node.getString("a"));
    }

    @Test
    void findStringWidensAcrossTextTypes() {
        MNode node = MNode.of(
            "s", "bare",
            "t", MNode.text("text"),
            "a", MNode.ascii("ascii"),
            "n", 42L
        );
        assertTrue(node.findString("s").isPresent());
        assertTrue(node.findString("t").isPresent());
        assertTrue(node.findString("a").isPresent());
        assertFalse(node.findString("n").isPresent());
    }

    @Test
    void asciiRejectsNonPrintable() {
        assertThrows(IllegalArgumentException.class, () -> MNode.ascii("hello\nworld"));
        assertThrows(IllegalArgumentException.class, () -> MNode.ascii("tab\there"));
        assertThrows(IllegalArgumentException.class, () -> MNode.ascii("émoji"));
    }

    @Test
    void textRejectsNull() {
        assertThrows(IllegalArgumentException.class, () -> MNode.text(null));
    }

    @Test
    void asciiRejectsNull() {
        assertThrows(IllegalArgumentException.class, () -> MNode.ascii(null));
    }

    @Test
    void textValidatedByteBufferRoundTrip() {
        MNode original = MNode.of("t", MNode.text("Hello"));
        ByteBuffer buf = ByteBuffer.allocate(1024);
        original.encode(buf);
        buf.flip();
        MNode decoded = MNode.fromBuffer(buf);
        assertEquals("Hello", decoded.getString("t"));
    }

    // ==================== Integer types (tags 12, 13) ====================

    @Test
    void int32RoundTrip() {
        MNode original = MNode.of("val", MNode.int32(42));
        MNode decoded = MNode.fromBytes(original.toBytes());
        assertEquals(42, decoded.getInt("val"));
    }

    @Test
    void shortRoundTrip() {
        MNode original = MNode.of("val", MNode.int16((short) 7));
        MNode decoded = MNode.fromBytes(original.toBytes());
        assertEquals((short) 7, decoded.getShort("val"));
    }

    @Test
    void getLongWidensFromInt32() {
        MNode node = MNode.of("val", MNode.int32(100));
        assertEquals(100L, node.getLong("val"));
    }

    @Test
    void getLongWidensFromShort() {
        MNode node = MNode.of("val", MNode.int16((short) 50));
        assertEquals(50L, node.getLong("val"));
    }

    @Test
    void getIntWidensFromShort() {
        MNode node = MNode.of("val", MNode.int16((short) 25));
        assertEquals(25, node.getInt("val"));
    }

    @Test
    void getShortRejectsInt32() {
        MNode node = MNode.of("val", MNode.int32(100));
        assertThrows(IllegalArgumentException.class, () -> node.getShort("val"));
    }

    @Test
    void getLongRejectsFloat() {
        // Cross-family widening is not allowed
        MNode node = MNode.of("val", 3.14);
        assertThrows(IllegalArgumentException.class, () -> node.getLong("val"));
    }

    @Test
    void findLongFindsAllIntegerTypes() {
        MNode node = MNode.of(
            "a", 42L,
            "b", MNode.int32(7),
            "c", MNode.int16((short) 3),
            "d", 1.5
        );
        assertTrue(node.findLong("a").isPresent());
        assertTrue(node.findLong("b").isPresent());
        assertTrue(node.findLong("c").isPresent());
        assertFalse(node.findLong("d").isPresent());
    }

    // ==================== Decimal and Varint (tags 14, 15) ====================

    @Test
    void decimalRoundTrip() {
        BigDecimal bd = new BigDecimal("123456.789");
        MNode original = MNode.of("price", bd);
        MNode decoded = MNode.fromBytes(original.toBytes());
        assertEquals(bd, decoded.getDecimal("price"));
    }

    @Test
    void varintRoundTrip() {
        BigInteger bi = new BigInteger("99999999999999999999");
        MNode original = MNode.of("big", bi);
        MNode decoded = MNode.fromBytes(original.toBytes());
        assertEquals(bi, decoded.getVarint("big"));
    }

    @Test
    void decimalByteBufferRoundTrip() {
        BigDecimal bd = new BigDecimal("-0.001");
        MNode original = MNode.of("d", bd);
        ByteBuffer buf = ByteBuffer.allocate(1024);
        original.encode(buf);
        buf.flip();
        MNode decoded = MNode.fromBuffer(buf);
        assertEquals(bd, decoded.getDecimal("d"));
    }

    @Test
    void findDecimalAndVarint() {
        MNode node = MNode.of("d", new BigDecimal("1.5"), "v", BigInteger.TEN);
        assertTrue(node.findDecimal("d").isPresent());
        assertTrue(node.findVarint("v").isPresent());
        assertFalse(node.findDecimal("v").isPresent());
        assertFalse(node.findVarint("d").isPresent());
    }

    // ==================== Float types (tags 16, 17) ====================

    @Test
    void float32RoundTrip() {
        MNode original = MNode.of("val", MNode.float32(3.14f));
        MNode decoded = MNode.fromBytes(original.toBytes());
        assertEquals(3.14f, decoded.getFloat("val"), 0.001f);
    }

    @Test
    void halfRoundTrip() {
        MNode original = MNode.of("val", MNode.half(1.5f));
        MNode decoded = MNode.fromBytes(original.toBytes());
        assertEquals(1.5f, decoded.getFloat("val"), 0.01f);
    }

    @Test
    void getDoubleWidensFromFloat32() {
        MNode node = MNode.of("val", MNode.float32(2.5f));
        assertEquals(2.5, node.getDouble("val"), 0.001);
    }

    @Test
    void getDoubleWidensFromHalf() {
        MNode node = MNode.of("val", MNode.half(1.0f));
        assertEquals(1.0, node.getDouble("val"), 0.001);
    }

    @Test
    void getFloatWidensFromHalf() {
        MNode node = MNode.of("val", MNode.half(2.0f));
        assertEquals(2.0f, node.getFloat("val"), 0.01f);
    }

    @Test
    void getDoubleRejectsInt() {
        // Cross-family widening is not allowed
        MNode node = MNode.of("val", 42L);
        assertThrows(IllegalArgumentException.class, () -> node.getDouble("val"));
    }

    @Test
    void findDoubleFindsAllFloatTypes() {
        MNode node = MNode.of(
            "a", 1.5,
            "b", MNode.float32(2.5f),
            "c", MNode.half(3.0f),
            "d", 42L
        );
        assertTrue(node.findDouble("a").isPresent());
        assertTrue(node.findDouble("b").isPresent());
        assertTrue(node.findDouble("c").isPresent());
        assertFalse(node.findDouble("d").isPresent());
    }

    @Test
    void halfPrecisionLossy() {
        // 3.14 can't be exactly represented in half precision
        MNode node = MNode.of("val", MNode.half(3.14f));
        MNode decoded = MNode.fromBytes(node.toBytes());
        float result = decoded.getFloat("val");
        // Half precision of 3.14 is approximately 3.140625
        assertEquals(3.14f, result, 0.01f);
    }

    // ==================== Temporal types (tags 18-22) ====================

    @Test
    void millisRoundTrip() {
        Instant now = Instant.ofEpochMilli(1708876800000L);
        MNode original = MNode.of("ts", MNode.millis(now));
        MNode decoded = MNode.fromBytes(original.toBytes());
        assertEquals(now, decoded.getMillis("ts"));
    }

    @Test
    void nanosRoundTrip() {
        Instant precise = Instant.ofEpochSecond(1708876800L, 123456789);
        MNode original = MNode.of("ts", MNode.nanos(precise));
        MNode decoded = MNode.fromBytes(original.toBytes());
        assertEquals(precise, decoded.getNanos("ts"));
    }

    @Test
    void dateRoundTrip() {
        LocalDate date = LocalDate.of(2026, 2, 25);
        MNode original = MNode.of("d", date);
        MNode decoded = MNode.fromBytes(original.toBytes());
        assertEquals(date, decoded.getDate("d"));
    }

    @Test
    void timeRoundTrip() {
        LocalTime time = LocalTime.of(14, 30, 0);
        MNode original = MNode.of("t", time);
        MNode decoded = MNode.fromBytes(original.toBytes());
        assertEquals(time, decoded.getTime("t"));
    }

    @Test
    void datetimeRoundTrip() {
        Instant dt = Instant.parse("2026-02-25T14:30:00Z");
        MNode original = MNode.of("dt", MNode.datetime(dt));
        MNode decoded = MNode.fromBytes(original.toBytes());
        assertEquals(dt, decoded.getDateTime("dt"));
    }

    @Test
    void datetimeWithZoneId() {
        Instant dt = Instant.parse("2026-02-25T14:30:00Z");
        MNode node = MNode.of("dt", MNode.datetime(dt));
        ZonedDateTime zdt = node.getDateTime("dt", ZoneId.of("America/New_York"));
        assertEquals(dt, zdt.toInstant());
        assertEquals(ZoneId.of("America/New_York"), zdt.getZone());
    }

    @Test
    void temporalByteBufferRoundTrip() {
        MNode original = MNode.of(
            "m", MNode.millis(Instant.ofEpochMilli(1000L)),
            "d", LocalDate.of(2026, 1, 1)
        );
        ByteBuffer buf = ByteBuffer.allocate(1024);
        original.encode(buf);
        buf.flip();
        MNode decoded = MNode.fromBuffer(buf);
        assertEquals(Instant.ofEpochMilli(1000L), decoded.getMillis("m"));
        assertEquals(LocalDate.of(2026, 1, 1), decoded.getDate("d"));
    }

    @Test
    void findTemporalTypes() {
        Instant now = Instant.now();
        MNode node = MNode.of(
            "m", MNode.millis(now),
            "n", MNode.nanos(now),
            "d", LocalDate.now(),
            "t", LocalTime.now(),
            "dt", MNode.datetime(now)
        );
        assertTrue(node.findMillis("m").isPresent());
        assertTrue(node.findNanos("n").isPresent());
        assertTrue(node.findDate("d").isPresent());
        assertTrue(node.findTime("t").isPresent());
        assertTrue(node.findDateTime("dt").isPresent());
        assertFalse(node.findMillis("d").isPresent());
    }

    // ==================== ID types (tags 23-25) ====================

    @Test
    void uuidV1RoundTrip() {
        // Construct a v1 UUID: version nibble at bits 48-51 must be 0001
        UUID v1 = makeUuidV1();
        MNode original = MNode.of("id", MNode.uuidV1(v1));
        MNode decoded = MNode.fromBytes(original.toBytes());
        assertEquals(v1, decoded.getUuidV1("id"));
    }

    @Test
    void uuidV7RoundTrip() {
        UUID v7 = makeUuidV7();
        MNode original = MNode.of("id", MNode.uuidV7(v7));
        MNode decoded = MNode.fromBytes(original.toBytes());
        assertEquals(v7, decoded.getUuidV7("id"));
    }

    @Test
    void ulidRoundTrip() {
        Ulid ulid = Ulid.of("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        MNode original = MNode.of("id", ulid);
        MNode decoded = MNode.fromBytes(original.toBytes());
        assertEquals(ulid, decoded.getUlid("id"));
    }

    @Test
    void uuidV1RejectsV4() {
        UUID v4 = UUID.randomUUID(); // v4
        assertThrows(IllegalArgumentException.class, () -> MNode.uuidV1(v4));
    }

    @Test
    void uuidV7RejectsV4() {
        UUID v4 = UUID.randomUUID(); // v4
        assertThrows(IllegalArgumentException.class, () -> MNode.uuidV7(v4));
    }

    @Test
    void findIdTypes() {
        UUID v1 = makeUuidV1();
        UUID v7 = makeUuidV7();
        Ulid ulid = Ulid.of(new byte[16]);
        MNode node = MNode.of("v1", MNode.uuidV1(v1), "v7", MNode.uuidV7(v7), "u", ulid);
        assertTrue(node.findUuidV1("v1").isPresent());
        assertTrue(node.findUuidV7("v7").isPresent());
        assertTrue(node.findUlid("u").isPresent());
        assertFalse(node.findUuidV1("v7").isPresent());
    }

    @Test
    void uuidByteBufferRoundTrip() {
        UUID v1 = makeUuidV1();
        MNode original = MNode.of("id", MNode.uuidV1(v1));
        ByteBuffer buf = ByteBuffer.allocate(1024);
        original.encode(buf);
        buf.flip();
        MNode decoded = MNode.fromBuffer(buf);
        assertEquals(v1, decoded.getUuidV1("id"));
    }

    // ==================== Array type (tag 26) ====================

    @Test
    void longArrayRoundTrip() {
        long[] arr = {1L, 2L, 3L, 4L, 5L};
        MNode original = MNode.of("data", MNode.array(arr));
        MNode decoded = MNode.fromBytes(original.toBytes());
        assertArrayEquals(arr, decoded.getArray("data", long[].class));
    }

    @Test
    void intArrayRoundTrip() {
        int[] arr = {10, 20, 30};
        MNode original = MNode.of("data", MNode.array(arr));
        MNode decoded = MNode.fromBytes(original.toBytes());
        assertArrayEquals(arr, decoded.getArray("data", int[].class));
    }

    @Test
    void shortArrayRoundTrip() {
        short[] arr = {1, 2, 3};
        MNode original = MNode.of("data", MNode.array(arr));
        MNode decoded = MNode.fromBytes(original.toBytes());
        assertArrayEquals(arr, decoded.getArray("data", short[].class));
    }

    @Test
    void doubleArrayRoundTrip() {
        double[] arr = {1.1, 2.2, 3.3};
        MNode original = MNode.of("data", MNode.array(arr));
        MNode decoded = MNode.fromBytes(original.toBytes());
        assertArrayEquals(arr, decoded.getArray("data", double[].class), 0.001);
    }

    @Test
    void floatArrayRoundTrip() {
        float[] arr = {1.1f, 2.2f, 3.3f};
        MNode original = MNode.of("data", MNode.array(arr));
        MNode decoded = MNode.fromBytes(original.toBytes());
        assertArrayEquals(arr, decoded.getArray("data", float[].class), 0.001f);
    }

    @Test
    void booleanArrayRoundTrip() {
        boolean[] arr = {true, false, true, true};
        MNode original = MNode.of("flags", MNode.array(arr));
        MNode decoded = MNode.fromBytes(original.toBytes());
        assertArrayEquals(arr, decoded.getArray("flags", boolean[].class));
    }

    @Test
    void getArrayElementType() {
        MNode node = MNode.of(
            "longs", MNode.array(new long[]{1, 2}),
            "floats", MNode.array(new float[]{1.0f})
        );
        assertEquals(long.class, node.getArrayElementType("longs"));
        assertEquals(float.class, node.getArrayElementType("floats"));
    }

    @Test
    void getArrayWrongTypeThrows() {
        MNode node = MNode.of("data", MNode.array(new long[]{1, 2}));
        assertThrows(IllegalArgumentException.class, () -> node.getArray("data", int[].class));
    }

    @Test
    void arrayEquality() {
        MNode a = MNode.of("d", MNode.array(new float[]{1.0f, 2.0f}));
        MNode b = MNode.of("d", MNode.array(new float[]{1.0f, 2.0f}));
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        MNode c = MNode.of("d", MNode.array(new float[]{1.0f, 3.0f}));
        assertNotEquals(a, c);
    }

    @Test
    void arrayByteBufferRoundTrip() {
        int[] arr = {10, 20, 30};
        MNode original = MNode.of("data", MNode.array(arr));
        ByteBuffer buf = ByteBuffer.allocate(1024);
        original.encode(buf);
        buf.flip();
        MNode decoded = MNode.fromBuffer(buf);
        assertArrayEquals(arr, decoded.getArray("data", int[].class));
    }

    // ==================== Set type (tag 27) ====================

    @Test
    void setRoundTrip() {
        Set<Object> input = new LinkedHashSet<>();
        input.add("a");
        input.add("b");
        input.add("c");
        MNode original = MNode.of("tags", input);
        MNode decoded = MNode.fromBytes(original.toBytes());
        Set<Object> result = decoded.getSet("tags");
        assertEquals(3, result.size());
        assertTrue(result.contains("a"));
        assertTrue(result.contains("b"));
        assertTrue(result.contains("c"));
    }

    @Test
    void setWithMixedScalars() {
        Set<Object> input = new LinkedHashSet<>();
        input.add("text");
        input.add(42L);
        input.add(true);
        MNode original = MNode.of("mixed", input);
        MNode decoded = MNode.fromBytes(original.toBytes());
        Set<Object> result = decoded.getSet("mixed");
        assertEquals(3, result.size());
        assertTrue(result.contains("text"));
        assertTrue(result.contains(42L));
        assertTrue(result.contains(true));
    }

    @Test
    void findSet() {
        Set<Object> input = Set.of("x", "y");
        MNode node = MNode.of("s", input);
        assertTrue(node.findSet("s").isPresent());
        assertFalse(node.findSet("missing").isPresent());
    }

    // ==================== Typed Map type (tag 28) ====================

    @Test
    void typedMapRoundTrip() {
        Map<Object, Object> input = new LinkedHashMap<>();
        input.put("key1", "value1");
        input.put("key2", 42L);
        MNode original = MNode.of("m", MNode.typedMap(input));
        MNode decoded = MNode.fromBytes(original.toBytes());
        Map<Object, Object> result = decoded.getTypedMap("m");
        assertEquals("value1", result.get("key1"));
        assertEquals(42L, result.get("key2"));
    }

    @Test
    void typedMapByteBufferRoundTrip() {
        Map<Object, Object> input = new LinkedHashMap<>();
        input.put("a", 1L);
        MNode original = MNode.of("m", MNode.typedMap(input));
        ByteBuffer buf = ByteBuffer.allocate(1024);
        original.encode(buf);
        buf.flip();
        MNode decoded = MNode.fromBuffer(buf);
        assertEquals(1L, decoded.getTypedMap("m").get("a"));
    }

    @Test
    void findTypedMap() {
        Map<Object, Object> input = Map.of("k", "v");
        MNode node = MNode.of("m", MNode.typedMap(input));
        assertTrue(node.findTypedMap("m").isPresent());
        assertFalse(node.findTypedMap("missing").isPresent());
    }

    // ==================== Backward compatibility ====================

    @Test
    void bareIntegerStillBecomesLong() {
        MNode node = MNode.of("val", 7);
        assertEquals(7L, node.getLong("val"));
    }

    @Test
    void bareFloatStillBecomesDouble() {
        MNode node = MNode.of("val", 1.5f);
        assertEquals(1.5, node.getDouble("val"), 0.001);
    }

    @Test
    void existingTagBehaviorUnchanged() {
        // All original tag 0-9 behavior should remain identical
        MNode original = MNode.of(
            "str", "hello",
            "num", 42L,
            "dbl", 3.14,
            "bool", true,
            "blob", new byte[]{1},
            "nil", null,
            "ev", MNode.enumVal("x"),
            "eo", MNode.enumOrd(0, "y"),
            "list", List.of(1L),
            "map", MNode.of("k", "v")
        );
        MNode decoded = MNode.fromBytes(original.toBytes());
        assertEquals("hello", decoded.getString("str"));
        assertEquals(42L, decoded.getLong("num"));
        assertEquals(3.14, decoded.getDouble("dbl"), 0.001);
        assertTrue(decoded.getBoolean("bool"));
        assertArrayEquals(new byte[]{1}, decoded.getBytes("blob"));
        assertFalse(decoded.has("nil"));
        assertEquals("x", decoded.getEnum("ev"));
        assertEquals(0, decoded.getEnumOrdinal("eo"));
        assertEquals(List.of(1L), decoded.getList("list"));
        assertEquals("v", decoded.getNode("map").getString("k"));
    }

    // ==================== Mixed-type nodes ====================

    @Test
    void mixedOldAndNewTypes() {
        MNode original = MNode.of(
            "name", "dataset",
            "count", 42L,
            "score", MNode.float32(0.95f),
            "dims", MNode.int32(128),
            "date", LocalDate.of(2026, 2, 25),
            "tags", List.of("ann", "benchmark"),
            "id", Ulid.of(new byte[16])
        );
        MNode decoded = MNode.fromBytes(original.toBytes());
        assertEquals("dataset", decoded.getString("name"));
        assertEquals(42L, decoded.getLong("count"));
        assertEquals(0.95f, decoded.getFloat("score"), 0.01f);
        assertEquals(128, decoded.getInt("dims"));
        assertEquals(LocalDate.of(2026, 2, 25), decoded.getDate("date"));
        assertEquals(List.of("ann", "benchmark"), decoded.getList("tags"));
        assertEquals(Ulid.of(new byte[16]), decoded.getUlid("id"));
    }

    @Test
    void mixedTypesWithByteBuffer() {
        BigDecimal bd = new BigDecimal("99.99");
        MNode original = MNode.of(
            "price", bd,
            "ts", MNode.millis(Instant.ofEpochMilli(1000L)),
            "data", MNode.array(new int[]{1, 2, 3})
        );
        ByteBuffer buf = ByteBuffer.allocate(4096);
        original.encode(buf);
        buf.flip();
        MNode decoded = MNode.fromBuffer(buf);
        assertEquals(bd, decoded.getDecimal("price"));
        assertEquals(Instant.ofEpochMilli(1000L), decoded.getMillis("ts"));
        assertArrayEquals(new int[]{1, 2, 3}, decoded.getArray("data", int[].class));
    }

    // ==================== toString for new types ====================

    @Test
    void toStringShowsNewTypes() {
        MNode node = MNode.of(
            "t", MNode.text("hi"),
            "a", MNode.ascii("yo"),
            "i", MNode.int32(5),
            "s", MNode.int16((short) 3),
            "f", MNode.float32(1.5f),
            "h", MNode.half(2.0f)
        );
        String s = node.toString();
        assertTrue(s.contains("text(hi)"));
        assertTrue(s.contains("ascii(yo)"));
        assertTrue(s.contains("int32(5)"));
        assertTrue(s.contains("short(3)"));
        assertTrue(s.contains("float32(1.5)"));
        assertTrue(s.contains("half(2.0)"));
    }

    // ==================== Gap 1: Typed array extensions ====================

    @Test
    void halfArrayRoundTrip() {
        Half[] arr = {Half.of(1.0f), Half.of(2.5f), Half.of(-0.5f)};
        MNode original = MNode.of("data", MNode.array(arr));
        MNode decoded = MNode.fromBytes(original.toBytes());
        Half[] result = decoded.getArray("data", Half[].class);
        assertEquals(arr.length, result.length);
        for (int i = 0; i < arr.length; i++) {
            assertEquals(arr[i], result[i]);
        }
    }

    @Test
    void millisArrayRoundTrip() {
        Instant[] arr = {
            Instant.ofEpochMilli(1000L),
            Instant.ofEpochMilli(2000L),
            Instant.ofEpochMilli(3000L)
        };
        MNode original = MNode.of("ts", MNode.arrayMillis(arr));
        MNode decoded = MNode.fromBytes(original.toBytes());
        Instant[] result = decoded.getArray("ts", Instant[].class);
        assertArrayEquals(arr, result);
        assertEquals(Instant.class, decoded.getArrayElementType("ts"));
    }

    @Test
    void nanosArrayRoundTrip() {
        Instant[] arr = {
            Instant.ofEpochSecond(1000L, 123456789),
            Instant.ofEpochSecond(2000L, 999999999)
        };
        MNode original = MNode.of("ts", MNode.arrayNanos(arr));
        MNode decoded = MNode.fromBytes(original.toBytes());
        Instant[] result = decoded.getArray("ts", Instant[].class);
        assertArrayEquals(arr, result);
    }

    @Test
    void enumOrdArrayRoundTrip() {
        int[] arr = {0, 2, 1, 0};
        MNode original = MNode.of("ords", MNode.arrayEnumOrd(arr));
        MNode decoded = MNode.fromBytes(original.toBytes());
        int[] result = decoded.getArray("ords", int[].class);
        assertArrayEquals(arr, result);
    }

    @Test
    void uuidV1ArrayRoundTrip() {
        UUID v1a = makeUuidV1();
        UUID v1b = new UUID(0x1234567890001000L, 0x8000000000000001L);
        UUID[] arr = {v1a, v1b};
        MNode original = MNode.of("ids", MNode.arrayUuidV1(arr));
        MNode decoded = MNode.fromBytes(original.toBytes());
        UUID[] result = decoded.getArray("ids", UUID[].class);
        assertArrayEquals(arr, result);
    }

    @Test
    void uuidV7ArrayRoundTrip() {
        UUID v7a = makeUuidV7();
        UUID v7b = new UUID(0xABCDEF0000007000L, 0x8000000000000001L);
        UUID[] arr = {v7a, v7b};
        MNode original = MNode.of("ids", MNode.arrayUuidV7(arr));
        MNode decoded = MNode.fromBytes(original.toBytes());
        UUID[] result = decoded.getArray("ids", UUID[].class);
        assertArrayEquals(arr, result);
    }

    @Test
    void ulidArrayRoundTrip() {
        Ulid u1 = Ulid.of("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        Ulid u2 = Ulid.of(new byte[]{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16});
        Ulid[] arr = {u1, u2};
        MNode original = MNode.of("ids", MNode.array(arr));
        MNode decoded = MNode.fromBytes(original.toBytes());
        Ulid[] result = decoded.getArray("ids", Ulid[].class);
        assertArrayEquals(arr, result);
    }

    @Test
    void uuidV1ArrayRejectsWrongVersion() {
        UUID v4 = UUID.randomUUID();
        assertThrows(IllegalArgumentException.class, () -> MNode.arrayUuidV1(new UUID[]{v4}));
    }

    @Test
    void uuidV7ArrayRejectsWrongVersion() {
        UUID v4 = UUID.randomUUID();
        assertThrows(IllegalArgumentException.class, () -> MNode.arrayUuidV7(new UUID[]{v4}));
    }

    @Test
    void objectArrayEquality() {
        Half[] a = {Half.of(1.0f), Half.of(2.0f)};
        Half[] b = {Half.of(1.0f), Half.of(2.0f)};
        MNode ma = MNode.of("h", MNode.array(a));
        MNode mb = MNode.of("h", MNode.array(b));
        assertEquals(ma, mb);
        assertEquals(ma.hashCode(), mb.hashCode());
    }

    // ==================== Gap 2: Missing find* methods ====================

    @Test
    void findIntFindsInt32AndShort() {
        MNode node = MNode.of(
            "a", MNode.int32(42),
            "b", MNode.int16((short) 7),
            "c", 100L
        );
        assertEquals(Optional.of(42), node.findInt("a"));
        assertEquals(Optional.of(7), node.findInt("b"));
        assertFalse(node.findInt("c").isPresent());
        assertFalse(node.findInt("missing").isPresent());
    }

    @Test
    void findShortFindsOnlyShort() {
        MNode node = MNode.of("a", MNode.int16((short) 5), "b", MNode.int32(10));
        assertEquals(Optional.of((short) 5), node.findShort("a"));
        assertFalse(node.findShort("b").isPresent());
    }

    @Test
    void findFloatFindsFloat32AndHalf() {
        MNode node = MNode.of(
            "a", MNode.float32(1.5f),
            "b", MNode.half(2.0f),
            "c", 3.14
        );
        assertEquals(Optional.of(1.5f), node.findFloat("a"));
        assertTrue(node.findFloat("b").isPresent());
        assertFalse(node.findFloat("c").isPresent());
    }

    @Test
    void findEnumOrdinal() {
        MNode node = MNode.of("e", MNode.enumOrd(2, "a", "b", "c"), "s", MNode.enumVal("x"));
        assertEquals(Optional.of(2), node.findEnumOrdinal("e"));
        assertFalse(node.findEnumOrdinal("s").isPresent());
    }

    @Test
    void findArrayPresent() {
        MNode node = MNode.of("f", MNode.array(new float[]{1.0f, 2.0f}));
        assertTrue(node.findArray("f", float[].class).isPresent());
        assertFalse(node.findArray("f", int[].class).isPresent());
        assertFalse(node.findArray("missing", float[].class).isPresent());
    }

    @Test
    void findArrayElementTypePresent() {
        MNode node = MNode.of("f", MNode.array(new long[]{1, 2}));
        assertEquals(Optional.of(long.class), node.findArrayElementType("f"));
        assertFalse(node.findArrayElementType("missing").isPresent());
    }

    // ==================== Gap 4: UTC enforcement ====================

    @Test
    void instantsAreUtcOnWire() {
        Instant now = Instant.parse("2026-02-25T14:30:00.123456789Z");
        MNode node = MNode.of(
            "m", MNode.millis(now),
            "n", MNode.nanos(now),
            "dt", MNode.datetime(now)
        );
        MNode decoded = MNode.fromBytes(node.toBytes());
        // millis loses sub-millis precision
        assertEquals(now.toEpochMilli(), decoded.getMillis("m").toEpochMilli());
        // nanos preserves full precision
        assertEquals(now, decoded.getNanos("n"));
        // datetime uses ISO string which is always UTC
        assertTrue(decoded.getDateTime("dt").toString().endsWith("Z"));
    }

    // ==================== Gap 5: Set/Map deterministic sorting ====================

    @Test
    void setDeterministicEncoding() {
        Set<Object> set1 = new LinkedHashSet<>();
        set1.add("b");
        set1.add("a");
        set1.add("c");
        Set<Object> set2 = new LinkedHashSet<>();
        set2.add("c");
        set2.add("a");
        set2.add("b");
        MNode node1 = MNode.of("s", set1);
        MNode node2 = MNode.of("s", set2);
        assertArrayEquals(node1.toBytes(), node2.toBytes());
    }

    @Test
    void typedMapDeterministicEncoding() {
        Map<Object, Object> map1 = new LinkedHashMap<>();
        map1.put("b", 2L);
        map1.put("a", 1L);
        Map<Object, Object> map2 = new LinkedHashMap<>();
        map2.put("a", 1L);
        map2.put("b", 2L);
        MNode node1 = MNode.of("m", MNode.typedMap(map1));
        MNode node2 = MNode.of("m", MNode.typedMap(map2));
        assertArrayEquals(node1.toBytes(), node2.toBytes());
    }

    // ==================== Gap 3: Type aliases + CDDL names ====================

    @Test
    void resolveTagPrimaryNames() {
        assertEquals(MNode.TAG_TEXT, MNode.resolveTag("string"));
        assertEquals(MNode.TAG_INT, MNode.resolveTag("long"));
        assertEquals(MNode.TAG_FLOAT, MNode.resolveTag("double"));
        assertEquals(MNode.TAG_BOOL, MNode.resolveTag("bool"));
        assertEquals(MNode.TAG_BYTES, MNode.resolveTag("byte"));
        assertEquals(MNode.TAG_INT32, MNode.resolveTag("int"));
        assertEquals(MNode.TAG_SHORT, MNode.resolveTag("short"));
        assertEquals(MNode.TAG_FLOAT32, MNode.resolveTag("float32"));
        assertEquals(MNode.TAG_HALF, MNode.resolveTag("half"));
    }

    @Test
    void resolveTagAliases() {
        assertEquals(MNode.TAG_INT, MNode.resolveTag("bigint"));
        assertEquals(MNode.TAG_SHORT, MNode.resolveTag("smallint"));
        assertEquals(MNode.TAG_BYTES, MNode.resolveTag("bytes"));
        assertEquals(MNode.TAG_BYTES, MNode.resolveTag("blob"));
        assertEquals(MNode.TAG_BYTES, MNode.resolveTag("binary"));
        assertEquals(MNode.TAG_MILLIS, MNode.resolveTag("epochmillis"));
        assertEquals(MNode.TAG_NANOS, MNode.resolveTag("epochnanos"));
        assertEquals(MNode.TAG_DATETIME, MNode.resolveTag("timestamp"));
        assertEquals(MNode.TAG_UUIDV1, MNode.resolveTag("timeuuid"));
        assertEquals(MNode.TAG_FLOAT32, MNode.resolveTag("float"));
        assertEquals(MNode.TAG_HALF, MNode.resolveTag("float16"));
        assertEquals(MNode.TAG_FLOAT, MNode.resolveTag("float64"));
        assertEquals(MNode.TAG_BOOL, MNode.resolveTag("boolean"));
        assertEquals(MNode.TAG_TEXT, MNode.resolveTag("tstr"));
    }

    @Test
    void cddlNameRoundTrip() {
        // Every tag 0-28 has a cddlName, and resolving that name back gives the same tag
        for (byte tag = 0; tag <= 28; tag++) {
            String name = MNode.cddlName(tag);
            assertNotNull(name, "Tag " + tag + " should have a cddlName");
            assertEquals(tag, MNode.resolveTag(name), "cddlName round-trip failed for tag " + tag);
        }
    }

    @Test
    void resolveTagUnknownThrows() {
        assertThrows(IllegalArgumentException.class, () -> MNode.resolveTag("unknown_type"));
    }

    // ==================== Helper methods ====================

    /// Create a UUID with version nibble set to 1
    private static UUID makeUuidV1() {
        long msb = 0x0000000000001000L; // version 1 at bits 48-51
        long lsb = 0x8000000000000000L; // variant 2 (RFC 4122)
        return new UUID(msb, lsb);
    }

    /// Create a UUID with version nibble set to 7
    private static UUID makeUuidV7() {
        long msb = 0x0000000000007000L; // version 7 at bits 48-51
        long lsb = 0x8000000000000000L; // variant 2 (RFC 4122)
        return new UUID(msb, lsb);
    }
}
