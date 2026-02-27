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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/// A predicate node represents a single comparison operation against zero or more values.
///
/// Predicates can identify their field by positional index, by name, or both. The mode
/// is determined at construction time:
///
/// - Indexed mode: `field` is >= 0, `fieldName` is null
/// - Named mode: `fieldName` is non-null, `field` is -1
/// - Both may be set when constructed by a {@link PredicateContext} that resolves names to indices
public class PredicateNode implements BBWriter<PredicateNode>, PNode<PredicateNode> {
    /// the field index, or -1 if using named mode
    private final int field;
    /// the field name, or null if using indexed mode
    private final String fieldName;
    /// the operator type
    private final OpType op;
    /// the values to compare
    private final long[] v;

    /// Creates a predicate node with a positional field index.
    ///
    /// @param field the field offset (must be >= 0)
    /// @param op    the operator type
    /// @param v     the values to compare
    public PredicateNode(int field, OpType op, long... v) {
        this.field = field;
        this.fieldName = null;
        this.op = op;
        this.v = v;
    }

    /// Creates a predicate node with a named field.
    ///
    /// @param fieldName the field name
    /// @param op        the operator type
    /// @param v         the values to compare
    public PredicateNode(String fieldName, OpType op, long... v) {
        this.field = -1;
        this.fieldName = Objects.requireNonNull(fieldName, "fieldName must not be null");
        this.op = op;
        this.v = v;
    }

    /// Package-private constructor for decode use, allowing both field and fieldName.
    ///
    /// @param field     the field index, or -1 if not applicable
    /// @param fieldName the field name, or null if not applicable
    /// @param op        the operator type
    /// @param v         the values to compare
    PredicateNode(int field, String fieldName, OpType op, long[] v) {
        this.field = field;
        this.fieldName = fieldName;
        this.op = op;
        this.v = v;
    }

    /// Returns the field offset.
    /// @return the field offset, or -1 if using named mode
    public int field() {
        return field;
    }

    /// Returns the field name.
    /// @return the field name, or null if using indexed mode
    public String fieldName() {
        return fieldName;
    }

    /// Returns the operator type.
    /// @return the operator type
    public OpType op() {
        return op;
    }

    /// Returns the comparison values.
    /// @return the values to compare
    public long[] v() {
        return v;
    }

    /// Creates a predicate node from type tag and positional field.
    ///
    /// @param type the type of {@link ConjugateType}
    /// @param field the field offset
    /// @param op the operator type
    /// @param v the values to compare
    public PredicateNode(byte type, int field, OpType op, long... v) {
        this(field, op, v);
    }

    /// Creates a predicate node by decoding from a byte buffer (indexed format).
    ///
    /// @param b the byte buffer to decode the predicate node from
    /// @see #PredicateNode(byte, int, OpType, long...)
    public PredicateNode(ByteBuffer b) {
        this(
            b.get(),
            b.get(),
            OpType.values()[b.get()],
            readValues(b)
        );
    }

    private static long[] readValues(ByteBuffer b) {
        int len = b.getShort();
        long[] values = new long[len];
        for (int i = 0; i < values.length; i++) {
            values[i] = b.getLong();
        }
        return values;
    }

    /// Encodes this predicate node into a byte buffer.
    ///
    /// If {@link #fieldName()} is non-null, writes the named wire format:
    /// `[PRED:1][nameLen:2][nameBytes:N][op:1][vLen:2][v:8*n]`
    ///
    /// Otherwise writes the indexed wire format:
    /// `[PRED:1][field:1][op:1][vLen:2][v:8*n]`
    ///
    /// @param out the output buffer
    /// @return the output buffer, for method chaining
    @Override
    public ByteBuffer encode(ByteBuffer out) {
        out.put((byte) ConjugateType.PRED.ordinal());
        if (fieldName != null) {
            byte[] nameBytes = fieldName.getBytes(StandardCharsets.UTF_8);
            out.putShort((short) nameBytes.length);
            out.put(nameBytes);
        } else {
            out.put((byte) field);
        }
        out.put((byte) op.ordinal());
        out.putShort((short) v.length);
        for (long l : v) {
            out.putLong(l);
        }
        return out;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PredicateNode))
            return false;

        PredicateNode that = (PredicateNode) o;
        return field == that.field
            && Objects.equals(fieldName, that.fieldName)
            && Arrays.equals(v, that.v)
            && op == that.op;
    }

    @Override
    public int hashCode() {
        int result = field;
        result = 31 * result + Objects.hashCode(fieldName);
        result = 31 * result + Objects.hashCode(op);
        result = 31 * result + Arrays.hashCode(v);
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PredicateNode{");
        if (fieldName != null) {
            sb.append("fieldName='").append(fieldName).append('\'');
        } else {
            sb.append("field=").append(field);
        }
        sb.append(", op=").append(op);
        sb.append(", v=");
        if (v == null)
            sb.append("null");
        else {
            sb.append('[');
            for (int i = 0; i < v.length; ++i)
                sb.append(i == 0 ? "" : ", ").append(v[i]);
            sb.append(']');
        }
        sb.append('}');
        return sb.toString();
    }
}
