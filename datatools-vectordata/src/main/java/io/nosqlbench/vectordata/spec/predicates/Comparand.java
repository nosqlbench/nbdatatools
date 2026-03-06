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

/// A typed comparand value for use in predicate expressions.
///
/// Comparands support six types, matching the vectordata-rs typed PNode wire format:
///
/// | Tag | Type   | Wire encoding              |
/// |-----|--------|----------------------------|
/// |  0  | Int    | {@code [i64 LE]}           |
/// |  1  | Float  | {@code [f64 LE]}           |
/// |  2  | Text   | {@code [len:u16 LE][utf8]} |
/// |  3  | Bool   | {@code [u8: 0/1]}          |
/// |  4  | Bytes  | {@code [len:u32 LE][data]} |
/// |  5  | Null   | (no value bytes)           |
public abstract class Comparand {

    /// Wire tag for 64-bit integer comparands
    public static final byte TAG_INT = 0;
    /// Wire tag for 64-bit float comparands
    public static final byte TAG_FLOAT = 1;
    /// Wire tag for UTF-8 text comparands
    public static final byte TAG_TEXT = 2;
    /// Wire tag for boolean comparands
    public static final byte TAG_BOOL = 3;
    /// Wire tag for raw byte array comparands
    public static final byte TAG_BYTES = 4;
    /// Wire tag for null comparands
    public static final byte TAG_NULL = 5;

    Comparand() {}

    /// Returns the wire tag for this comparand type.
    /// @return the tag byte
    public abstract byte tag();

    /// Encodes this comparand's tag and value into the buffer.
    /// @param buf the target buffer
    public abstract void encode(ByteBuffer buf);

    /// Returns the encoded size in bytes (including tag byte).
    /// @return the encoded size
    public abstract int encodedSize();

    /// Decodes a single typed comparand from the buffer.
    /// @param buf the buffer to read from
    /// @return the decoded comparand
    public static Comparand decode(ByteBuffer buf) {
        byte tag = buf.get();
        switch (tag) {
            case TAG_INT:
                return new IntVal(buf.getLong());
            case TAG_FLOAT:
                return new FloatVal(buf.getDouble());
            case TAG_TEXT: {
                int len = Short.toUnsignedInt(buf.getShort());
                byte[] bytes = new byte[len];
                buf.get(bytes);
                return new TextVal(new String(bytes, StandardCharsets.UTF_8));
            }
            case TAG_BOOL:
                return new BoolVal(buf.get() != 0);
            case TAG_BYTES: {
                int len = buf.getInt();
                byte[] bytes = new byte[len];
                buf.get(bytes);
                return new BytesVal(bytes);
            }
            case TAG_NULL:
                return NullVal.INSTANCE;
            default:
                throw new IllegalArgumentException("Unknown comparand tag: " + tag);
        }
    }

    /// A 64-bit integer comparand.
    public static final class IntVal extends Comparand {
        private final long value;

        /// Creates an integer comparand.
        /// @param value the integer value
        public IntVal(long value) { this.value = value; }

        /// Returns the integer value.
        /// @return the value
        public long value() { return value; }

        @Override public byte tag() { return TAG_INT; }
        @Override public void encode(ByteBuffer buf) { buf.put(TAG_INT); buf.putLong(value); }
        @Override public int encodedSize() { return 1 + 8; }

        @Override public boolean equals(Object o) {
            return o instanceof IntVal && ((IntVal) o).value == value;
        }
        @Override public int hashCode() { return Long.hashCode(value); }
        @Override public String toString() { return "IntVal(" + value + ")"; }
    }

    /// A 64-bit floating-point comparand.
    public static final class FloatVal extends Comparand {
        private final double value;

        /// Creates a float comparand.
        /// @param value the double value
        public FloatVal(double value) { this.value = value; }

        /// Returns the double value.
        /// @return the value
        public double value() { return value; }

        @Override public byte tag() { return TAG_FLOAT; }
        @Override public void encode(ByteBuffer buf) { buf.put(TAG_FLOAT); buf.putDouble(value); }
        @Override public int encodedSize() { return 1 + 8; }

        @Override public boolean equals(Object o) {
            return o instanceof FloatVal && Double.compare(((FloatVal) o).value, value) == 0;
        }
        @Override public int hashCode() { return Double.hashCode(value); }
        @Override public String toString() { return "FloatVal(" + value + ")"; }
    }

    /// A UTF-8 text comparand.
    public static final class TextVal extends Comparand {
        private final String value;

        /// Creates a text comparand.
        /// @param value the text value
        public TextVal(String value) { this.value = Objects.requireNonNull(value); }

        /// Returns the text value.
        /// @return the value
        public String value() { return value; }

        @Override public byte tag() { return TAG_TEXT; }
        @Override public void encode(ByteBuffer buf) {
            buf.put(TAG_TEXT);
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            buf.putShort((short) bytes.length);
            buf.put(bytes);
        }
        @Override public int encodedSize() {
            return 1 + 2 + value.getBytes(StandardCharsets.UTF_8).length;
        }

        @Override public boolean equals(Object o) {
            return o instanceof TextVal && ((TextVal) o).value.equals(value);
        }
        @Override public int hashCode() { return value.hashCode(); }
        @Override public String toString() { return "TextVal(\"" + value + "\")"; }
    }

    /// A boolean comparand.
    public static final class BoolVal extends Comparand {
        private final boolean value;

        /// Creates a boolean comparand.
        /// @param value the boolean value
        public BoolVal(boolean value) { this.value = value; }

        /// Returns the boolean value.
        /// @return the value
        public boolean value() { return value; }

        @Override public byte tag() { return TAG_BOOL; }
        @Override public void encode(ByteBuffer buf) { buf.put(TAG_BOOL); buf.put((byte) (value ? 1 : 0)); }
        @Override public int encodedSize() { return 1 + 1; }

        @Override public boolean equals(Object o) {
            return o instanceof BoolVal && ((BoolVal) o).value == value;
        }
        @Override public int hashCode() { return Boolean.hashCode(value); }
        @Override public String toString() { return "BoolVal(" + value + ")"; }
    }

    /// A raw byte array comparand.
    public static final class BytesVal extends Comparand {
        private final byte[] value;

        /// Creates a bytes comparand.
        /// @param value the byte array
        public BytesVal(byte[] value) { this.value = Objects.requireNonNull(value); }

        /// Returns the byte array value.
        /// @return the value
        public byte[] value() { return value; }

        @Override public byte tag() { return TAG_BYTES; }
        @Override public void encode(ByteBuffer buf) {
            buf.put(TAG_BYTES);
            buf.putInt(value.length);
            buf.put(value);
        }
        @Override public int encodedSize() { return 1 + 4 + value.length; }

        @Override public boolean equals(Object o) {
            return o instanceof BytesVal && Arrays.equals(value, ((BytesVal) o).value);
        }
        @Override public int hashCode() { return Arrays.hashCode(value); }
        @Override public String toString() { return "BytesVal(" + value.length + " bytes)"; }
    }

    /// A null comparand.
    public static final class NullVal extends Comparand {
        /// Shared singleton instance
        public static final NullVal INSTANCE = new NullVal();

        /// Creates a null comparand.
        public NullVal() {}

        @Override public byte tag() { return TAG_NULL; }
        @Override public void encode(ByteBuffer buf) { buf.put(TAG_NULL); }
        @Override public int encodedSize() { return 1; }

        @Override public boolean equals(Object o) { return o instanceof NullVal; }
        @Override public int hashCode() { return 0; }
        @Override public String toString() { return "NullVal"; }
    }
}
