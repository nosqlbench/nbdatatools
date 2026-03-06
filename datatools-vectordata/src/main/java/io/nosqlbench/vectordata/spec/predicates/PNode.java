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


import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/// A predicate node is a node in a predicate tree.
///
/// ## Dialect leader
///
/// When embedded in a mixed stream (alongside MNode records), PNode trees
/// are prefixed with a dialect leader byte ({@code 0x02}). Use
/// {@link #encodeFramed(ByteBuffer)} and {@link #fromFramedBuffer(ByteBuffer)}
/// for this framed format.
///
/// @param <T> the type of self
public interface PNode<T> extends BBWriter<T> {

    /// Dialect leader byte identifying PNode records in mixed streams.
    byte DIALECT = 0x02;

    /// Version marker byte for the typed comparand wire format.
    byte TYPED_VERSION_MARKER = (byte) 0xFF;

    /// Creates a predicate node from a byte buffer using the indexed wire format.
    ///
    /// This method assumes all predicates use positional field indices. For
    /// context-aware decoding that supports both indexed and named fields, use
    /// {@link PredicateContext#decode(ByteBuffer)} instead.
    ///
    /// @param b the byte buffer to decode the predicate node from
    /// @return a predicate node
    static PNode<?> fromBuffer(ByteBuffer b) {
        byte typeOrdinal = b.get(b.position());
        switch(ConjugateType.values()[typeOrdinal]) {
            case AND:
            case OR:
                return new ConjugateNode(b);
            case PRED:
                return new PredicateNode(b);
            default:
                throw new IllegalArgumentException("Unknown ConjugateType: " + ConjugateType.values()[typeOrdinal]);
        }
    }

    /// Encodes this PNode tree with a dialect leader byte prefix.
    ///
    /// Wire format: {@code [DIALECT=0x02][tree body...]}
    ///
    /// @param out the output buffer
    /// @return the output buffer, for chaining
    default ByteBuffer encodeFramed(ByteBuffer out) {
        out.order(ByteOrder.LITTLE_ENDIAN);
        out.put(DIALECT);
        return encode(out);
    }

    /// Decodes a PNode tree from a buffer that starts with the dialect leader byte.
    ///
    /// Auto-detects the format: if the byte after the dialect leader is
    /// {@code 0xFF}, uses the typed named format; otherwise delegates to
    /// the standard indexed format.
    ///
    /// @param buf the buffer to decode from
    /// @return the decoded predicate tree
    /// @throws IllegalArgumentException if the dialect leader is not {@code 0x02}
    static PNode<?> fromFramedBuffer(ByteBuffer buf) {
        buf.order(ByteOrder.LITTLE_ENDIAN);
        byte leader = buf.get();
        if (leader != DIALECT) {
            throw new IllegalArgumentException(
                "Expected PNode dialect leader 0x02, got 0x" + Integer.toHexString(leader & 0xFF));
        }
        if (buf.remaining() > 0 && buf.get(buf.position()) == TYPED_VERSION_MARKER) {
            buf.get(); // consume 0xFF marker
            return PredicateContext.namedTyped().decode(buf);
        }
        return fromBuffer(buf);
    }

    /// Encodes this PNode tree to a byte array with dialect leader.
    ///
    /// @return the encoded bytes including dialect leader
    default byte[] toFramedBytes() {
        ByteBuffer buf = ByteBuffer.allocate(4096).order(ByteOrder.LITTLE_ENDIAN);
        encodeFramed(buf);
        buf.flip();
        byte[] result = new byte[buf.remaining()];
        buf.get(result);
        return result;
    }

    /// Decodes a PNode tree from a byte array with dialect leader.
    ///
    /// @param bytes the encoded bytes
    /// @return the decoded predicate tree
    static PNode<?> fromFramedBytes(byte[] bytes) {
        return fromFramedBuffer(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN));
    }
}
