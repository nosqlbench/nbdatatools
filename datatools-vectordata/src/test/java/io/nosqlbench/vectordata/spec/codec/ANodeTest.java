package io.nosqlbench.vectordata.spec.codec;

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

import io.nosqlbench.vectordata.spec.metadata.MNode;
import io.nosqlbench.vectordata.spec.predicates.*;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.*;

/// Tests for ANode unified envelope — dispatching between MNode and PNode based on dialect leader.
class ANodeTest {

    // ==================== MNode round-trip ====================

    @Test
    void mnodeRoundTripBytes() {
        MNode original = MNode.of("name", "alice", "age", 42L);
        ANode a = ANode.ofMetadata(original);
        assertTrue(a.isMetadata());

        byte[] bytes = a.toBytes();
        assertEquals(MNode.DIALECT, bytes[0]);

        ANode decoded = ANode.fromBytes(bytes);
        assertTrue(decoded.isMetadata());
        assertEquals("alice", decoded.asMNode().getString("name"));
        assertEquals(42L, decoded.asMNode().getLong("age"));
    }

    @Test
    void mnodeRoundTripBuffer() {
        MNode original = MNode.of("score", 3.14);
        ANode a = ANode.ofMetadata(original);
        byte[] bytes = a.toBytes();

        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        ANode decoded = ANode.fromBuffer(buf);
        assertTrue(decoded.isMetadata());
        assertEquals(3.14, decoded.asMNode().getDouble("score"), 0.001);
    }

    // ==================== PNode round-trip ====================

    @Test
    void pnodeRoundTripBytes() {
        // Use indexed predicate for standard wire format round-trip
        PredicateNode pred = new PredicateNode(0, OpType.GT, 42L);
        ANode a = ANode.ofPredicate(pred);
        assertFalse(a.isMetadata());

        byte[] bytes = a.toBytes();
        assertEquals(PNode.DIALECT, bytes[0]);

        ANode decoded = ANode.fromBytes(bytes);
        assertFalse(decoded.isMetadata());
    }

    @Test
    void pnodeRoundTripBuffer() {
        // Use indexed predicate for standard wire format round-trip
        PredicateNode pred = new PredicateNode(0, OpType.LE, 100L);
        ANode a = ANode.ofPredicate(pred);
        byte[] bytes = a.toBytes();

        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        ANode decoded = ANode.fromBuffer(buf);
        assertFalse(decoded.isMetadata());
    }

    // ==================== Accessor guards ====================

    @Test
    void metadataRecordPNodeThrows() {
        ANode a = ANode.ofMetadata(MNode.of("x", 1L));
        assertThrows(IllegalStateException.class, a::asPNode);
    }

    @Test
    void predicateTreeMNodeThrows() {
        ANode a = ANode.ofPredicate(new PredicateNode("x", OpType.EQ, 1L));
        assertThrows(IllegalStateException.class, a::asMNode);
    }

    // ==================== Congruence ====================

    @Test
    void congruentMetadataRecords() {
        ANode a1 = ANode.ofMetadata(MNode.of("name", "alice", "age", 42L));
        ANode a2 = ANode.ofMetadata(MNode.of("name", "bob", "age", 99L));
        assertTrue(a1.isCongruent(a2));
    }

    @Test
    void nonCongruentMetadataRecords() {
        ANode a1 = ANode.ofMetadata(MNode.of("name", "alice"));
        ANode a2 = ANode.ofMetadata(MNode.of("age", 42L));
        assertFalse(a1.isCongruent(a2));
    }

    @Test
    void predicateTreesNotCongruent() {
        ANode a1 = ANode.ofPredicate(new PredicateNode("x", OpType.EQ, 1L));
        ANode a2 = ANode.ofPredicate(new PredicateNode("x", OpType.EQ, 1L));
        assertFalse(a1.isCongruent(a2));
    }

    // ==================== Error cases ====================

    @Test
    void emptyBytesThrows() {
        assertThrows(IllegalArgumentException.class, () -> ANode.fromBytes(new byte[0]));
    }

    @Test
    void unknownDialectThrows() {
        assertThrows(IllegalArgumentException.class, () -> ANode.fromBytes(new byte[]{(byte) 0xFF}));
    }

    // ==================== Factory inner classes ====================

    @Test
    void metadataRecordNodeAccessor() {
        MNode m = MNode.of("k", "v");
        ANode.MetadataRecord mr = (ANode.MetadataRecord) ANode.ofMetadata(m);
        assertSame(m, mr.node());
    }

    @Test
    void predicateTreeTreeAccessor() {
        PredicateNode pred = new PredicateNode("x", OpType.EQ, 1L);
        ANode.PredicateTree pt = (ANode.PredicateTree) ANode.ofPredicate(pred);
        assertSame(pred, pt.tree());
    }
}
