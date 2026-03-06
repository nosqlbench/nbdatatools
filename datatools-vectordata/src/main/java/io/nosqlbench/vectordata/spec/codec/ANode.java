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
import io.nosqlbench.vectordata.spec.predicates.PNode;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/// Unified envelope for auto-detecting MNode and PNode records based on
/// the dialect leader byte.
///
/// ## Dialect leader bytes
///
/// | Byte   | Type  |
/// |--------|-------|
/// | 0x01   | MNode |
/// | 0x02   | PNode |
///
/// ANode dispatches encoding and decoding to the appropriate inner type
/// based on the first byte of the record.
public abstract class ANode {

    ANode() {}

    /// Returns whether this is a metadata record (MNode).
    /// @return true if MNode
    public abstract boolean isMetadata();

    /// Returns this as a metadata record.
    /// @return the MNode
    /// @throws IllegalStateException if this is not a metadata record
    public abstract MNode asMNode();

    /// Returns this as a predicate tree.
    /// @return the PNode
    /// @throws IllegalStateException if this is not a predicate tree
    public abstract PNode<?> asPNode();

    /// Encodes this ANode to a byte array including the dialect leader.
    /// @return the encoded bytes
    public abstract byte[] toBytes();

    /// An MNode metadata record.
    public static final class MetadataRecord extends ANode {
        private final MNode node;

        /// Creates a metadata record wrapper.
        /// @param node the MNode
        public MetadataRecord(MNode node) { this.node = node; }

        @Override public boolean isMetadata() { return true; }
        @Override public MNode asMNode() { return node; }
        @Override public PNode<?> asPNode() {
            throw new IllegalStateException("ANode is a MetadataRecord, not a PredicateTree");
        }
        @Override public byte[] toBytes() { return node.toBytes(); }

        /// Returns the inner MNode.
        /// @return the MNode
        public MNode node() { return node; }
    }

    /// A PNode predicate tree.
    public static final class PredicateTree extends ANode {
        private final PNode<?> tree;

        /// Creates a predicate tree wrapper.
        /// @param tree the PNode
        public PredicateTree(PNode<?> tree) { this.tree = tree; }

        @Override public boolean isMetadata() { return false; }
        @Override public MNode asMNode() {
            throw new IllegalStateException("ANode is a PredicateTree, not a MetadataRecord");
        }
        @Override public PNode<?> asPNode() { return tree; }
        @Override public byte[] toBytes() { return tree.toFramedBytes(); }

        /// Returns the inner PNode.
        /// @return the PNode tree
        public PNode<?> tree() { return tree; }
    }

    /// Wraps an MNode as an ANode.
    /// @param node the MNode
    /// @return the wrapped ANode
    public static ANode ofMetadata(MNode node) {
        return new MetadataRecord(node);
    }

    /// Wraps a PNode as an ANode.
    /// @param tree the PNode tree
    /// @return the wrapped ANode
    public static ANode ofPredicate(PNode<?> tree) {
        return new PredicateTree(tree);
    }

    /// Decodes an ANode from a byte array by inspecting the dialect leader byte.
    ///
    /// @param bytes the encoded bytes
    /// @return the decoded ANode
    /// @throws IllegalArgumentException if the dialect byte is not recognized
    public static ANode fromBytes(byte[] bytes) {
        if (bytes.length == 0) {
            throw new IllegalArgumentException("Cannot decode ANode from empty byte array");
        }
        switch (bytes[0]) {
            case MNode.DIALECT:
                return new MetadataRecord(MNode.fromBytes(bytes));
            case PNode.DIALECT:
                return new PredicateTree(PNode.fromFramedBytes(bytes));
            default:
                throw new IllegalArgumentException(
                    "Unknown ANode dialect leader: 0x" + Integer.toHexString(bytes[0] & 0xFF));
        }
    }

    /// Decodes an ANode from a ByteBuffer by inspecting the dialect leader byte.
    ///
    /// @param buf the buffer to read from
    /// @return the decoded ANode
    /// @throws IllegalArgumentException if the dialect byte is not recognized
    public static ANode fromBuffer(ByteBuffer buf) {
        buf.order(ByteOrder.LITTLE_ENDIAN);
        byte leader = buf.get(buf.position());
        switch (leader) {
            case MNode.DIALECT: {
                // Read all remaining bytes including the dialect leader
                byte[] all = new byte[buf.remaining()];
                buf.get(all);
                return new MetadataRecord(MNode.fromBytes(all));
            }
            case PNode.DIALECT:
                return new PredicateTree(PNode.fromFramedBuffer(buf));
            default:
                throw new IllegalArgumentException(
                    "Unknown ANode dialect leader: 0x" + Integer.toHexString(leader & 0xFF));
        }
    }

    /// Returns whether this ANode has the same structural type and schema
    /// as another.
    ///
    /// @param other the other ANode
    /// @return true if structurally congruent
    public boolean isCongruent(ANode other) {
        if (this instanceof MetadataRecord && other instanceof MetadataRecord) {
            return ((MetadataRecord) this).node().isCongruent(((MetadataRecord) other).node());
        }
        return false;
    }
}
