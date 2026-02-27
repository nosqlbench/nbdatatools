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

import io.nosqlbench.vectordata.discovery.metadata.FieldDescriptor;
import io.nosqlbench.vectordata.discovery.metadata.MetadataLayout;
import io.nosqlbench.vectordata.discovery.metadata.MetadataLayoutImpl;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/// Determines the field representation mode for predicates and provides
/// context-aware decoding and field access.
///
/// A `PredicateContext` is created once with a specific mode — indexed or named —
/// and all predicates within that context use the same representation. Consumer
/// code calls {@link #fieldName(PredicateNode)} or {@link #fieldIndex(PredicateNode)}
/// without type checks or conditionals; the mode decision is baked into the
/// context at creation time.
///
/// ## Usage
///
/// ```java
/// PredicateContext ctx = PredicateContext.named(layout);
/// PNode<?> tree = ctx.decode(buf);
///
/// PredicateNode pred = ...;
/// String name = ctx.fieldName(pred);
/// int index = ctx.fieldIndex(pred);
/// FieldDescriptor fd = ctx.fieldDescriptor(pred);
/// ```
///
/// @see PredicateNode
/// @see MetadataLayout
public abstract class PredicateContext {

    /// Creates a new PredicateContext.
    protected PredicateContext() {
    }

    /// Decodes a full predicate tree from a buffer.
    ///
    /// The tag byte at the current position determines whether this is a
    /// conjugate node or a leaf predicate. Conjugate children are decoded
    /// recursively.
    ///
    /// @param buf the buffer to decode from
    /// @return the decoded predicate tree
    public PNode<?> decode(ByteBuffer buf) {
        byte tag = buf.get(buf.position());
        ConjugateType type = ConjugateType.values()[tag];
        switch (type) {
            case AND:
            case OR:
                return decodeConjugate(buf);
            case PRED:
                return decodePredicate(buf);
            default:
                throw new IllegalArgumentException("Unknown ConjugateType ordinal: " + tag);
        }
    }

    /// Decodes a leaf predicate node using the context-specific wire format.
    ///
    /// @param buf the buffer to decode from
    /// @return the decoded predicate node
    protected abstract PredicateNode decodePredicate(ByteBuffer buf);

    /// Returns the field name for a predicate node.
    ///
    /// @param pred the predicate node
    /// @return the field name
    public abstract String fieldName(PredicateNode pred);

    /// Returns the field index for a predicate node.
    ///
    /// @param pred the predicate node
    /// @return the field index, or -1 if no layout is available for resolution
    public abstract int fieldIndex(PredicateNode pred);

    /// Returns the field descriptor for a predicate node.
    ///
    /// @param pred the predicate node
    /// @return the field descriptor
    /// @throws IllegalStateException if no layout is available for resolution
    public abstract FieldDescriptor fieldDescriptor(PredicateNode pred);

    /// Creates a context for indexed predicates where fields are identified by
    /// positional index and the layout resolves names.
    ///
    /// @param layout the metadata layout
    /// @return an indexed predicate context
    public static PredicateContext indexed(MetadataLayout layout) {
        return new Indexed(layout);
    }

    /// Creates a context for named predicates without a layout.
    /// Field index and descriptor lookups will return -1 or throw.
    ///
    /// @return a named predicate context
    public static PredicateContext named() {
        return new Named(null);
    }

    /// Creates a context for named predicates with a layout for index resolution.
    ///
    /// @param layout the metadata layout for resolving names to indices
    /// @return a named predicate context
    public static PredicateContext named(MetadataLayout layout) {
        return new Named(layout);
    }

    /// Decodes a conjugate node, delegating child decoding back through
    /// {@link #decode(ByteBuffer)} for recursive dispatch.
    private ConjugateNode decodeConjugate(ByteBuffer buf) {
        ConjugateType ctype = ConjugateType.values()[buf.get()];
        byte count = buf.get();
        PNode<?>[] children = new PNode[count];
        for (int i = 0; i < count; i++) {
            children[i] = decode(buf);
        }
        return new ConjugateNode(ctype, children);
    }

    /// Reads comparand values from a buffer in the standard format: `[vLen:2][v:8*n]`.
    static long[] readValues(ByteBuffer buf) {
        int len = buf.getShort();
        long[] values = new long[len];
        for (int i = 0; i < values.length; i++) {
            values[i] = buf.getLong();
        }
        return values;
    }

    /// Indexed context: fields are identified by a 1-byte positional index.
    ///
    /// Wire format: `[PRED:1][field:1][op:1][vLen:2][v:8*n]`
    static final class Indexed extends PredicateContext {
        private final MetadataLayout layout;

        Indexed(MetadataLayout layout) {
            this.layout = Objects.requireNonNull(layout, "layout is required for indexed context");
        }

        @Override
        protected PredicateNode decodePredicate(ByteBuffer buf) {
            buf.get(); // consume PRED tag
            int field = Byte.toUnsignedInt(buf.get());
            OpType op = OpType.values()[buf.get()];
            long[] values = readValues(buf);
            return new PredicateNode(field, null, op, values);
        }

        @Override
        public String fieldName(PredicateNode pred) {
            return layout.getField(pred.field()).name();
        }

        @Override
        public int fieldIndex(PredicateNode pred) {
            return pred.field();
        }

        @Override
        public FieldDescriptor fieldDescriptor(PredicateNode pred) {
            return layout.getField(pred.field());
        }
    }

    /// Named context: fields are identified by a UTF-8 name string.
    ///
    /// Wire format: `[PRED:1][nameLen:2][nameBytes:N][op:1][vLen:2][v:8*n]`
    static final class Named extends PredicateContext {
        private final MetadataLayout layout;

        Named(MetadataLayout layout) {
            this.layout = layout;
        }

        @Override
        protected PredicateNode decodePredicate(ByteBuffer buf) {
            buf.get(); // consume PRED tag
            int nameLen = Short.toUnsignedInt(buf.getShort());
            byte[] nameBytes = new byte[nameLen];
            buf.get(nameBytes);
            String name = new String(nameBytes, StandardCharsets.UTF_8);
            OpType op = OpType.values()[buf.get()];
            long[] values = readValues(buf);
            return new PredicateNode(-1, name, op, values);
        }

        @Override
        public String fieldName(PredicateNode pred) {
            return pred.fieldName();
        }

        @Override
        public int fieldIndex(PredicateNode pred) {
            if (layout == null) {
                return -1;
            }
            if (layout instanceof MetadataLayoutImpl) {
                return ((MetadataLayoutImpl) layout).indexOfField(pred.fieldName());
            }
            return layout.getFieldByName(pred.fieldName())
                .map(fd -> layout.getFields().indexOf(fd))
                .orElse(-1);
        }

        @Override
        public FieldDescriptor fieldDescriptor(PredicateNode pred) {
            if (layout == null) {
                throw new IllegalStateException(
                    "No layout available to resolve field descriptor for '" + pred.fieldName() + "'");
            }
            return layout.getFieldByName(pred.fieldName())
                .orElseThrow(() -> new IllegalArgumentException(
                    "No field named '" + pred.fieldName() + "' in layout"));
        }
    }
}
