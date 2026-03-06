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
///
/// ## Comparand types
///
/// Comparands can be untyped ({@code long[]}) for the legacy/indexed wire format, or
/// typed ({@link Comparand}{@code []}) for the typed named wire format. If typed comparands
/// are present, they take precedence over the untyped values.
public class PredicateNode implements BBWriter<PredicateNode>, PNode<PredicateNode> {
    /// the field index, or -1 if using named mode
    private final int field;
    /// the field name, or null if using indexed mode
    private final String fieldName;
    /// the operator type
    private final OpType op;
    /// the untyped comparison values (i64-only, legacy format)
    private final long[] v;
    /// the typed comparison values, or null if using legacy format
    private final Comparand[] comparands;

    /// Creates a predicate node with a positional field index (legacy i64 comparands).
    ///
    /// @param field the field offset (must be >= 0)
    /// @param op    the operator type
    /// @param v     the values to compare
    public PredicateNode(int field, OpType op, long... v) {
        this.field = field;
        this.fieldName = null;
        this.op = op;
        this.v = v;
        this.comparands = null;
    }

    /// Creates a predicate node with a named field (legacy i64 comparands).
    ///
    /// @param fieldName the field name
    /// @param op        the operator type
    /// @param v         the values to compare
    public PredicateNode(String fieldName, OpType op, long... v) {
        this.field = -1;
        this.fieldName = Objects.requireNonNull(fieldName, "fieldName must not be null");
        this.op = op;
        this.v = v;
        this.comparands = null;
    }

    /// Creates a predicate node with a named field and typed comparands.
    ///
    /// @param fieldName  the field name
    /// @param op         the operator type
    /// @param comparands the typed comparand values
    public PredicateNode(String fieldName, OpType op, Comparand... comparands) {
        this.field = -1;
        this.fieldName = Objects.requireNonNull(fieldName, "fieldName must not be null");
        this.op = op;
        this.comparands = comparands;
        // populate v with i64 values from IntVal comparands for backward compatibility
        this.v = toLongArray(comparands);
    }

    /// Creates a predicate node with a positional field index and typed comparands.
    ///
    /// @param field      the field offset (must be >= 0)
    /// @param op         the operator type
    /// @param comparands the typed comparand values
    public PredicateNode(int field, OpType op, Comparand... comparands) {
        this.field = field;
        this.fieldName = null;
        this.op = op;
        this.comparands = comparands;
        this.v = toLongArray(comparands);
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
        this.comparands = null;
    }

    /// Package-private constructor for decode use with typed comparands.
    ///
    /// @param field      the field index, or -1 if not applicable
    /// @param fieldName  the field name, or null if not applicable
    /// @param op         the operator type
    /// @param comparands the typed comparand values
    PredicateNode(int field, String fieldName, OpType op, Comparand[] comparands) {
        this.field = field;
        this.fieldName = fieldName;
        this.op = op;
        this.comparands = comparands;
        this.v = toLongArray(comparands);
    }

    private static long[] toLongArray(Comparand[] comparands) {
        long[] result = new long[comparands.length];
        for (int i = 0; i < comparands.length; i++) {
            if (comparands[i] instanceof Comparand.IntVal) {
                result[i] = ((Comparand.IntVal) comparands[i]).value();
            }
        }
        return result;
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

    /// Returns the comparison values (legacy i64 format).
    /// @return the values to compare
    public long[] v() {
        return v;
    }

    /// Returns the typed comparands, or null if using legacy i64 format.
    /// @return the typed comparands, or null
    public Comparand[] comparands() {
        return comparands;
    }

    /// Returns whether this predicate uses typed comparands.
    /// @return true if typed comparands are present
    public boolean isTyped() {
        return comparands != null;
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

    /// Encodes this predicate node using the typed named wire format.
    ///
    /// This format uses the {@code 0xFF} version marker and per-comparand type tags,
    /// matching the vectordata-rs typed PNode format.
    ///
    /// Wire format:
    /// {@code [PRED:1][nameLen:2][nameBytes:N][op:1][comparandCount:2][tag:1 value:...]*}
    ///
    /// This method requires a non-null {@link #fieldName()} and typed comparands.
    /// If typed comparands are not set, the legacy {@code long[]} values are
    /// wrapped as {@link Comparand.IntVal}.
    ///
    /// @param out the output buffer
    /// @return the output buffer, for chaining
    public ByteBuffer encodeTyped(ByteBuffer out) {
        out.order(ByteOrder.LITTLE_ENDIAN);
        out.put((byte) ConjugateType.PRED.ordinal());
        String name = fieldName != null ? fieldName : ("F" + field);
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        out.putShort((short) nameBytes.length);
        out.put(nameBytes);
        out.put((byte) op.ordinal());

        Comparand[] comps = effectiveComparands();
        out.putShort((short) comps.length);
        for (Comparand c : comps) {
            c.encode(out);
        }
        return out;
    }

    /// Encodes this predicate node tree using the typed named framed format.
    ///
    /// Wire format: {@code [DIALECT=0x02][0xFF][typed tree body...]}
    ///
    /// @param out the output buffer
    /// @return the output buffer, for chaining
    public ByteBuffer encodeTypedFramed(ByteBuffer out) {
        out.order(ByteOrder.LITTLE_ENDIAN);
        out.put(PNode.DIALECT);
        out.put(PNode.TYPED_VERSION_MARKER);
        return encodeTyped(out);
    }

    /// Returns the effective comparands for encoding. If typed comparands are
    /// set, returns them; otherwise wraps the legacy {@code long[]} values
    /// as {@link Comparand.IntVal}.
    ///
    /// @return the comparands
    Comparand[] effectiveComparands() {
        if (comparands != null) {
            return comparands;
        }
        Comparand[] result = new Comparand[v.length];
        for (int i = 0; i < v.length; i++) {
            result[i] = new Comparand.IntVal(v[i]);
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PredicateNode))
            return false;

        PredicateNode that = (PredicateNode) o;
        if (field != that.field || !Objects.equals(fieldName, that.fieldName) || op != that.op)
            return false;

        // compare typed comparands if both have them
        if (comparands != null && that.comparands != null) {
            return Arrays.equals(comparands, that.comparands);
        }
        return Arrays.equals(v, that.v);
    }

    @Override
    public int hashCode() {
        int result = field;
        result = 31 * result + Objects.hashCode(fieldName);
        result = 31 * result + Objects.hashCode(op);
        if (comparands != null) {
            result = 31 * result + Arrays.hashCode(comparands);
        } else {
            result = 31 * result + Arrays.hashCode(v);
        }
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
        if (comparands != null) {
            sb.append(", comparands=").append(Arrays.toString(comparands));
        } else {
            sb.append(", v=");
            if (v == null) {
                sb.append("null");
            } else {
                sb.append('[');
                for (int i = 0; i < v.length; ++i)
                    sb.append(i == 0 ? "" : ", ").append(v[i]);
                sb.append(']');
            }
        }
        sb.append('}');
        return sb.toString();
    }
}
