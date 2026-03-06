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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/// A binary-encoded metadata node using PNode-style direct ByteBuffer I/O.
///
/// MNode is a high-performance binary metadata format. It uses
/// a compact, self-describing wire format with type-tagged values and
/// length-prefixed field names — no external schema is required to decode.
///
/// Internally, decoded data is stored in parallel arrays ({@code keys[]},
/// {@code types[]}, {@code values[]}) rather than a {@code Map}, avoiding
/// hash table allocation on the decode path.
///
/// ## Supported value types (29 tags)
///
/// | Tag | Name           | Java type      | Wire encoding                          |
/// |-----|----------------|----------------|----------------------------------------|
/// |  0  | STRING         | String         | {@code [len:4][utf8:N]}                |
/// |  1  | INT (long)     | Long           | {@code [val:8]} LE                     |
/// |  2  | FLOAT (double) | Double         | {@code [val:8]} LE                     |
/// |  3  | BOOL           | Boolean        | {@code [val:1]} 0/1                    |
/// |  4  | BYTES          | byte[]         | {@code [len:4][bytes:N]}               |
/// |  5  | NULL           | null           | (no value bytes)                       |
/// |  6  | ENUM_STR       | String         | {@code [len:4][utf8:N]}                |
/// |  7  | ENUM_ORD       | Integer        | {@code [ordinal:4]} LE                 |
/// |  8  | LIST           | List           | {@code [count:4][tag+value...]}        |
/// |  9  | NODE (map)     | MNode          | {@code [len:4][mnode_payload:N]}       |
/// | 10  | TEXT           | String         | {@code [len:4][utf8:N]}                |
/// | 11  | ASCII          | String         | {@code [len:4][bytes:N]}               |
/// | 12  | INT32          | Integer        | {@code [val:4]} LE                     |
/// | 13  | SHORT          | Short          | {@code [val:2]} LE                     |
/// | 14  | DECIMAL        | BigDecimal     | {@code [scale:4][len:4][bytes:N]}      |
/// | 15  | VARINT         | BigInteger     | {@code [len:4][bytes:N]}               |
/// | 16  | FLOAT32        | Float          | {@code [val:4]} LE                     |
/// | 17  | HALF           | Float (stored) | {@code [val:2]} LE                     |
/// | 18  | MILLIS         | Instant        | {@code [val:8]} LE                     |
/// | 19  | NANOS          | Instant        | {@code [seconds:8][nanoAdjust:4]} LE   |
/// | 20  | DATE           | LocalDate      | {@code [len:4][utf8:N]}                |
/// | 21  | TIME           | LocalTime      | {@code [len:4][utf8:N]}                |
/// | 22  | DATETIME       | Instant        | {@code [len:4][utf8:N]}                |
/// | 23  | UUIDV1         | UUID           | {@code [msb:8][lsb:8]}                 |
/// | 24  | UUIDV7         | UUID           | {@code [msb:8][lsb:8]}                 |
/// | 25  | ULID           | Ulid           | {@code [bytes:16]}                     |
/// | 26  | ARRAY          | typed array    | {@code [elemTag:1][count:4][values:N]} |
/// | 27  | SET            | Set            | {@code [count:4][tag+value...]}        |
/// | 28  | TYPED_MAP      | Map            | {@code [count:4][kTag+k+vTag+v...]}    |
///
/// ## Wire format
///
/// ```
/// [dialect:1 = 0x01][field_count:2]
/// per field: [nameLen:2][nameUtf8:N][typeTag:1][valueBytes...]
/// ```
///
/// All multi-byte integers use little-endian byte order.
///
/// The dialect leader byte ({@code 0x01}) identifies this as an MNode record,
/// enabling auto-detection when mixed with PNode ({@code 0x02}) records
/// in an ANode stream.
///
/// ## ByteBuffer framing
///
/// When embedded in a stream via {@link #encode(ByteBuffer)} /
/// {@link #fromBuffer(ByteBuffer)}, a 4-byte little-endian length
/// prefix is prepended: {@code [totalLen:4][dialect:1][payload...]}.
public final class MNode {

    /// Dialect leader byte identifying MNode records in mixed streams.
    public static final byte DIALECT = 0x01;

    // --- Type tags (0-9: original, 10-28: extended) ---
    static final byte TAG_TEXT          = 0;  // string (unvalidated)
    static final byte TAG_INT           = 1;  // long
    static final byte TAG_FLOAT         = 2;  // double
    static final byte TAG_BOOL          = 3;
    static final byte TAG_BYTES         = 4;
    static final byte TAG_NULL          = 5;
    static final byte TAG_ENUM_STR      = 6;
    static final byte TAG_ENUM_ORD      = 7;
    static final byte TAG_LIST          = 8;
    static final byte TAG_MAP           = 9;  // nested MNode (node)
    static final byte TAG_TEXT_VALIDATED = 10; // text (validated UTF-8)
    static final byte TAG_ASCII         = 11;
    static final byte TAG_INT32         = 12;
    static final byte TAG_SHORT         = 13;
    static final byte TAG_DECIMAL       = 14;
    static final byte TAG_VARINT        = 15;
    static final byte TAG_FLOAT32       = 16;
    static final byte TAG_HALF          = 17;
    static final byte TAG_MILLIS        = 18;
    static final byte TAG_NANOS         = 19;
    static final byte TAG_DATE          = 20;
    static final byte TAG_TIME          = 21;
    static final byte TAG_DATETIME      = 22;
    static final byte TAG_UUIDV1        = 23;
    static final byte TAG_UUIDV7        = 24;
    static final byte TAG_ULID          = 25;
    static final byte TAG_ARRAY         = 26;
    static final byte TAG_SET           = 27;
    static final byte TAG_TYPED_MAP     = 28;

    /// Human-readable tag names for error messages
    static final String[] TAG_NAMES = {
        "STRING", "LONG", "DOUBLE", "BOOL", "BYTES", "NULL",
        "ENUM_STR", "ENUM_ORD", "LIST", "NODE",
        "TEXT", "ASCII", "INT32", "SHORT", "DECIMAL", "VARINT",
        "FLOAT32", "HALF", "MILLIS", "NANOS", "DATE", "TIME", "DATETIME",
        "UUIDV1", "UUIDV7", "ULID", "ARRAY", "SET", "TYPED_MAP"
    };

    /// Primary CDDL names for each tag (lowercase, CDDL convention).
    private static final String[] CDDL_NAMES = {
        "string", "long", "double", "bool", "byte", "null",
        "enum_str", "enum_ord", "list", "node",
        "text", "ascii", "int", "short", "decimal", "varint",
        "float32", "half", "millis", "nanos", "date", "time", "datetime",
        "uuidv1", "uuidv7", "ulid", "array", "set", "map"
    };

    /// Map from CDDL type name (primary + aliases) to tag byte.
    static final Map<String, Byte> CDDL_NAME_TO_TAG;

    /// Map from tag byte to primary CDDL name.
    static final Map<Byte, String> TAG_TO_CDDL_NAME;

    static {
        Map<String, Byte> nameToTag = new HashMap<>();
        Map<Byte, String> tagToName = new HashMap<>();

        // primary names
        for (int i = 0; i < CDDL_NAMES.length; i++) {
            nameToTag.put(CDDL_NAMES[i], (byte) i);
            tagToName.put((byte) i, CDDL_NAMES[i]);
        }

        // aliases
        nameToTag.put("bigint", TAG_INT);
        nameToTag.put("smallint", TAG_SHORT);
        nameToTag.put("bytes", TAG_BYTES);
        nameToTag.put("blob", TAG_BYTES);
        nameToTag.put("binary", TAG_BYTES);
        nameToTag.put("epochmillis", TAG_MILLIS);
        nameToTag.put("epochnanos", TAG_NANOS);
        nameToTag.put("timestamp", TAG_DATETIME);
        nameToTag.put("timeuuid", TAG_UUIDV1);
        nameToTag.put("float", TAG_FLOAT32);
        nameToTag.put("float16", TAG_HALF);
        nameToTag.put("float64", TAG_FLOAT);
        nameToTag.put("boolean", TAG_BOOL);
        nameToTag.put("tstr", TAG_TEXT);

        CDDL_NAME_TO_TAG = Collections.unmodifiableMap(nameToTag);
        TAG_TO_CDDL_NAME = Collections.unmodifiableMap(tagToName);
    }

    /// Resolve a CDDL type name (primary or alias) to its tag byte.
    /// @param name the type name (case-sensitive)
    /// @return the tag byte
    /// @throws IllegalArgumentException if the name is not recognized
    public static byte resolveTag(String name) {
        Byte tag = CDDL_NAME_TO_TAG.get(name);
        if (tag == null) {
            throw new IllegalArgumentException("Unknown CDDL type name: " + name);
        }
        return tag;
    }

    /// Return the primary CDDL name for a tag byte.
    /// @param tag the tag byte
    /// @return the primary CDDL name
    /// @throws IllegalArgumentException if the tag is not recognized
    public static String cddlName(byte tag) {
        String name = TAG_TO_CDDL_NAME.get(tag);
        if (name == null) {
            throw new IllegalArgumentException("Unknown tag: " + tag);
        }
        return name;
    }

    private final String[] keys;
    private final byte[] types;
    private final Object[] values;

    /// Per-field enum definitions. Null when no ordinal enums are present.
    /// When non-null, {@code enumDefs[i]} is the allowed-values array for
    /// field {@code i}, or null if that field is not an ordinal enum.
    private final String[][] enumDefs;

    private MNode(String[] keys, byte[] types, Object[] values, String[][] enumDefs) {
        this.keys = keys;
        this.types = types;
        this.values = values;
        this.enumDefs = enumDefs;
    }

    // ==================== Marker classes ====================

    /// Package-private marker for self-describing enum values
    static final class EnumVal {
        final String value;
        EnumVal(String value) { this.value = value; }
    }

    /// Package-private marker for ordinal-encoded enum values
    static final class EnumOrd {
        final int ordinal;
        final String[] allowedValues;
        EnumOrd(int ordinal, String[] allowedValues) {
            this.ordinal = ordinal;
            this.allowedValues = allowedValues;
        }
    }

    /// Marker for validated UTF-8 text (tag 10)
    static final class TextValidated {
        final String value;
        TextValidated(String value) { this.value = value; }
    }

    /// Marker for ASCII-only text (tag 11)
    static final class AsciiVal {
        final String value;
        AsciiVal(String value) { this.value = value; }
    }

    /// Marker for 32-bit integer (tag 12)
    static final class Int32Val {
        final int value;
        Int32Val(int value) { this.value = value; }
    }

    /// Marker for 16-bit integer (tag 13)
    static final class Int16Val {
        final short value;
        Int16Val(short value) { this.value = value; }
    }

    /// Marker for 32-bit float (tag 16)
    static final class Float32Val {
        final float value;
        Float32Val(float value) { this.value = value; }
    }

    /// Marker for half-precision float (tag 17)
    static final class HalfVal {
        final float value;
        HalfVal(float value) { this.value = value; }
    }

    /// Marker for epoch-millis instant (tag 18)
    static final class MillisVal {
        final Instant value;
        MillisVal(Instant value) { this.value = value; }
    }

    /// Marker for epoch-nanos instant (tag 19)
    static final class NanosVal {
        final Instant value;
        NanosVal(Instant value) { this.value = value; }
    }

    /// Marker for ISO datetime instant (tag 22)
    static final class DateTimeVal {
        final Instant value;
        DateTimeVal(Instant value) { this.value = value; }
    }

    /// Marker for UUID v1 (tag 23)
    static final class UuidV1Val {
        final UUID value;
        UuidV1Val(UUID value) { this.value = value; }
    }

    /// Marker for UUID v7 (tag 24)
    static final class UuidV7Val {
        final UUID value;
        UuidV7Val(UUID value) { this.value = value; }
    }

    /// Wrapper for typed arrays (tag 26)
    static final class TypedArrayVal {
        final Object array;
        final byte elemTag;
        TypedArrayVal(Object array, byte elemTag) {
            this.array = array;
            this.elemTag = elemTag;
        }
    }

    /// Marker for typed-key map (tag 28)
    static final class TypedMapVal {
        final Map<Object, Object> map;
        TypedMapVal(Map<Object, Object> map) { this.map = map; }
    }

    // ==================== Public marker factories ====================

    /// Marker for a self-describing enum value in {@link #of(Object...)}.
    /// The string value is stored directly on the wire.
    /// @param value the enum string value
    /// @return a marker object to pass as a value in {@link #of(Object...)}
    public static Object enumVal(String value) {
        if (value == null) {
            throw new IllegalArgumentException("enumVal() value must not be null");
        }
        return new EnumVal(value);
    }

    /// Marker for an ordinal-encoded enum value in {@link #of(Object...)}.
    /// The ordinal is stored on the wire. Enum definitions must be attached
    /// via {@link #withEnumDef(String, String...)} before calling
    /// {@link #getEnum(String)}.
    /// @param ordinal the zero-based ordinal index
    /// @return a marker object to pass as a value in {@link #of(Object...)}
    public static Object enumOrd(int ordinal) {
        return new EnumOrd(ordinal, null);
    }

    /// Marker for an ordinal-encoded enum value with inline definitions.
    /// The ordinal is stored on the wire. The allowed values are attached
    /// to the MNode so {@link #getEnum(String)} can resolve immediately.
    /// @param ordinal the zero-based ordinal index
    /// @param allowedValues the complete list of allowed enum string values
    /// @return a marker object to pass as a value in {@link #of(Object...)}
    /// @throws IllegalArgumentException if ordinal is out of range
    public static Object enumOrd(int ordinal, String... allowedValues) {
        if (ordinal < 0 || ordinal >= allowedValues.length) {
            throw new IllegalArgumentException(
                "enumOrd ordinal " + ordinal + " out of range for " + allowedValues.length + " allowed values"
            );
        }
        return new EnumOrd(ordinal, allowedValues.clone());
    }

    /// Marker for validated UTF-8 text (tag 10). Validates the string is
    /// well-formed UTF-8.
    /// @param value the text value
    /// @return a marker object
    /// @throws IllegalArgumentException if the string is not well-formed UTF-8
    public static Object text(String value) {
        if (value == null) {
            throw new IllegalArgumentException("text() value must not be null");
        }
        validateUtf8(value);
        return new TextValidated(value);
    }

    /// Marker for ASCII-only text (tag 11). Validates every character is
    /// in the printable ASCII range (U+0020–U+007E).
    /// @param value the ASCII text value
    /// @return a marker object
    /// @throws IllegalArgumentException if the string contains non-printable-ASCII characters
    public static Object ascii(String value) {
        if (value == null) {
            throw new IllegalArgumentException("ascii() value must not be null");
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c < 0x20 || c > 0x7E) {
                throw new IllegalArgumentException(
                    "ascii() value contains non-printable-ASCII character at index " + i
                    + ": U+" + String.format("%04X", (int) c)
                );
            }
        }
        return new AsciiVal(value);
    }

    /// Marker for a 32-bit integer value (tag 12).
    /// @param value the int value
    /// @return a marker object
    public static Object int32(int value) {
        return new Int32Val(value);
    }

    /// Marker for a 16-bit integer value (tag 13).
    /// @param value the short value
    /// @return a marker object
    public static Object int16(short value) {
        return new Int16Val(value);
    }

    /// Marker for a 32-bit float value (tag 16).
    /// @param value the float value
    /// @return a marker object
    public static Object float32(float value) {
        return new Float32Val(value);
    }

    /// Marker for a half-precision float value (tag 17).
    /// The value is stored as a float but narrowed to binary16 on encode.
    /// @param value the float value (will be rounded to half precision)
    /// @return a marker object
    public static Object half(float value) {
        return new HalfVal(value);
    }

    /// Marker for an epoch-millis instant (tag 18).
    /// The Instant is stored as UTC milliseconds since the Unix epoch.
    /// Instant is inherently UTC — no timezone conversion is performed or needed.
    /// @param value the Instant (UTC)
    /// @return a marker object
    public static Object millis(Instant value) {
        if (value == null) {
            throw new IllegalArgumentException("millis() value must not be null");
        }
        return new MillisVal(value);
    }

    /// Marker for an epoch-nanos instant (tag 19).
    /// The Instant is stored as UTC seconds + nanosecond adjustment.
    /// Instant is inherently UTC — no timezone conversion is performed or needed.
    /// @param value the Instant (UTC)
    /// @return a marker object
    public static Object nanos(Instant value) {
        if (value == null) {
            throw new IllegalArgumentException("nanos() value must not be null");
        }
        return new NanosVal(value);
    }

    /// Marker for an ISO datetime instant (tag 22).
    /// The Instant is stored as a UTC ISO 8601 string (trailing 'Z').
    /// Instant is inherently UTC — no timezone conversion is performed or needed.
    /// @param value the Instant (UTC)
    /// @return a marker object
    public static Object datetime(Instant value) {
        if (value == null) {
            throw new IllegalArgumentException("datetime() value must not be null");
        }
        return new DateTimeVal(value);
    }

    /// Marker for a UUID v1 (tag 23). Validates version nibble.
    /// @param value the UUID (must be version 1)
    /// @return a marker object
    /// @throws IllegalArgumentException if the UUID is not version 1
    public static Object uuidV1(UUID value) {
        if (value == null) {
            throw new IllegalArgumentException("uuidV1() value must not be null");
        }
        if (value.version() != 1) {
            throw new IllegalArgumentException(
                "uuidV1() requires a version 1 UUID, got version " + value.version()
            );
        }
        return new UuidV1Val(value);
    }

    /// Marker for a UUID v7 (tag 24). Validates version nibble.
    /// @param value the UUID (must be version 7)
    /// @return a marker object
    /// @throws IllegalArgumentException if the UUID is not version 7
    public static Object uuidV7(UUID value) {
        if (value == null) {
            throw new IllegalArgumentException("uuidV7() value must not be null");
        }
        if (value.version() != 7) {
            throw new IllegalArgumentException(
                "uuidV7() requires a version 7 UUID, got version " + value.version()
            );
        }
        return new UuidV7Val(value);
    }

    /// Marker for a typed-key map (tag 28). Keys must be scalars or strings.
    /// @param map the map
    /// @return a marker object
    public static Object typedMap(Map<Object, Object> map) {
        if (map == null) {
            throw new IllegalArgumentException("typedMap() value must not be null");
        }
        return new TypedMapVal(map);
    }

    /// Marker for a typed long array (tag 26).
    /// @param arr the array
    /// @return a marker object
    public static Object array(long[] arr) {
        return new TypedArrayVal(arr.clone(), TAG_INT);
    }

    /// Marker for a typed int array (tag 26).
    /// @param arr the array
    /// @return a marker object
    public static Object array(int[] arr) {
        return new TypedArrayVal(arr.clone(), TAG_INT32);
    }

    /// Marker for a typed short array (tag 26).
    /// @param arr the array
    /// @return a marker object
    public static Object array(short[] arr) {
        return new TypedArrayVal(arr.clone(), TAG_SHORT);
    }

    /// Marker for a typed double array (tag 26).
    /// @param arr the array
    /// @return a marker object
    public static Object array(double[] arr) {
        return new TypedArrayVal(arr.clone(), TAG_FLOAT);
    }

    /// Marker for a typed float array (tag 26).
    /// @param arr the array
    /// @return a marker object
    public static Object array(float[] arr) {
        return new TypedArrayVal(arr.clone(), TAG_FLOAT32);
    }

    /// Marker for a typed boolean array (tag 26).
    /// @param arr the array
    /// @return a marker object
    public static Object array(boolean[] arr) {
        return new TypedArrayVal(arr.clone(), TAG_BOOL);
    }

    /// Marker for a typed Half array (tag 26). Each element is 2 bytes (binary16).
    /// @param arr the array
    /// @return a marker object
    public static Object array(Half[] arr) {
        return new TypedArrayVal(arr.clone(), TAG_HALF);
    }

    /// Marker for a typed Instant array encoded as epoch millis (tag 26).
    /// Each element is 8 bytes.
    /// @param arr the array
    /// @return a marker object
    public static Object arrayMillis(Instant[] arr) {
        return new TypedArrayVal(arr.clone(), TAG_MILLIS);
    }

    /// Marker for a typed Instant array encoded as epoch nanos (tag 26).
    /// Each element is 12 bytes (seconds:8 + nanoAdjust:4).
    /// @param arr the array
    /// @return a marker object
    public static Object arrayNanos(Instant[] arr) {
        return new TypedArrayVal(arr.clone(), TAG_NANOS);
    }

    /// Marker for a typed int array encoded as enum ordinals (tag 26).
    /// Each element is 4 bytes.
    /// @param arr the array
    /// @return a marker object
    public static Object arrayEnumOrd(int[] arr) {
        return new TypedArrayVal(arr.clone(), TAG_ENUM_ORD);
    }

    /// Marker for a typed UUID v1 array (tag 26). Validates all UUIDs are version 1.
    /// Each element is 16 bytes.
    /// @param arr the array
    /// @return a marker object
    /// @throws IllegalArgumentException if any UUID is not version 1
    public static Object arrayUuidV1(UUID[] arr) {
        for (int i = 0; i < arr.length; i++) {
            if (arr[i].version() != 1) {
                throw new IllegalArgumentException(
                    "arrayUuidV1() element [" + i + "] is not version 1, got version " + arr[i].version()
                );
            }
        }
        return new TypedArrayVal(arr.clone(), TAG_UUIDV1);
    }

    /// Marker for a typed UUID v7 array (tag 26). Validates all UUIDs are version 7.
    /// Each element is 16 bytes.
    /// @param arr the array
    /// @return a marker object
    /// @throws IllegalArgumentException if any UUID is not version 7
    public static Object arrayUuidV7(UUID[] arr) {
        for (int i = 0; i < arr.length; i++) {
            if (arr[i].version() != 7) {
                throw new IllegalArgumentException(
                    "arrayUuidV7() element [" + i + "] is not version 7, got version " + arr[i].version()
                );
            }
        }
        return new TypedArrayVal(arr.clone(), TAG_UUIDV7);
    }

    /// Marker for a typed Ulid array (tag 26). Each element is 16 bytes.
    /// @param arr the array
    /// @return a marker object
    public static Object array(Ulid[] arr) {
        return new TypedArrayVal(arr.clone(), TAG_ULID);
    }

    // ==================== Factory ====================

    /// Varargs factory for creating an MNode from key-value pairs.
    ///
    /// Keys must be Strings. Supported value types include all original types
    /// (String, Long, Integer, Double, Float, Boolean, byte[], null, List, MNode)
    /// plus marker types for the extended type system (text, ascii, int32, int16,
    /// float32, half, millis, nanos, datetime, uuidV1, uuidV7, typedMap, array)
    /// and directly-inferrable types (BigDecimal, BigInteger, LocalDate, LocalTime,
    /// Ulid, Set).
    ///
    /// @param kvs alternating key, value pairs
    /// @return a new MNode
    /// @throws IllegalArgumentException if an odd number of arguments is given,
    ///         a key is not a String, or a value type is unsupported
    public static MNode of(Object... kvs) {
        if (kvs.length % 2 != 0) {
            throw new IllegalArgumentException("MNode.of() requires an even number of arguments (key-value pairs)");
        }
        int count = kvs.length / 2;
        String[] keys = new String[count];
        byte[] types = new byte[count];
        Object[] values = new Object[count];
        String[][] enumDefs = null;
        for (int i = 0; i < count; i++) {
            Object k = kvs[i * 2];
            Object v = kvs[i * 2 + 1];
            if (!(k instanceof String)) {
                throw new IllegalArgumentException("MNode.of() keys must be Strings, got: " + k.getClass().getName());
            }
            keys[i] = (String) k;
            if (v == null) {
                types[i] = TAG_NULL;
                values[i] = null;
            // --- Marker types first (before their underlying Java types) ---
            } else if (v instanceof EnumVal) {
                types[i] = TAG_ENUM_STR;
                values[i] = ((EnumVal) v).value;
            } else if (v instanceof EnumOrd) {
                EnumOrd eo = (EnumOrd) v;
                types[i] = TAG_ENUM_ORD;
                values[i] = eo.ordinal;
                if (eo.allowedValues != null) {
                    if (enumDefs == null) {
                        enumDefs = new String[count][];
                    }
                    enumDefs[i] = eo.allowedValues;
                }
            } else if (v instanceof TextValidated) {
                types[i] = TAG_TEXT_VALIDATED;
                values[i] = ((TextValidated) v).value;
            } else if (v instanceof AsciiVal) {
                types[i] = TAG_ASCII;
                values[i] = ((AsciiVal) v).value;
            } else if (v instanceof Int32Val) {
                types[i] = TAG_INT32;
                values[i] = ((Int32Val) v).value;
            } else if (v instanceof Int16Val) {
                types[i] = TAG_SHORT;
                values[i] = ((Int16Val) v).value;
            } else if (v instanceof Float32Val) {
                types[i] = TAG_FLOAT32;
                values[i] = ((Float32Val) v).value;
            } else if (v instanceof HalfVal) {
                types[i] = TAG_HALF;
                values[i] = ((HalfVal) v).value; // stored as Float
            } else if (v instanceof MillisVal) {
                types[i] = TAG_MILLIS;
                values[i] = ((MillisVal) v).value;
            } else if (v instanceof NanosVal) {
                types[i] = TAG_NANOS;
                values[i] = ((NanosVal) v).value;
            } else if (v instanceof DateTimeVal) {
                types[i] = TAG_DATETIME;
                values[i] = ((DateTimeVal) v).value;
            } else if (v instanceof UuidV1Val) {
                types[i] = TAG_UUIDV1;
                values[i] = ((UuidV1Val) v).value;
            } else if (v instanceof UuidV7Val) {
                types[i] = TAG_UUIDV7;
                values[i] = ((UuidV7Val) v).value;
            } else if (v instanceof TypedArrayVal) {
                types[i] = TAG_ARRAY;
                values[i] = v; // keep the wrapper
            } else if (v instanceof TypedMapVal) {
                types[i] = TAG_TYPED_MAP;
                values[i] = ((TypedMapVal) v).map;
            // --- Directly-inferrable types ---
            } else if (v instanceof String) {
                types[i] = TAG_TEXT;
                values[i] = v;
            } else if (v instanceof Long) {
                types[i] = TAG_INT;
                values[i] = v;
            } else if (v instanceof Integer) {
                types[i] = TAG_INT;
                values[i] = ((Integer) v).longValue();
            } else if (v instanceof Double) {
                types[i] = TAG_FLOAT;
                values[i] = v;
            } else if (v instanceof Float) {
                types[i] = TAG_FLOAT;
                values[i] = ((Float) v).doubleValue();
            } else if (v instanceof Boolean) {
                types[i] = TAG_BOOL;
                values[i] = v;
            } else if (v instanceof byte[]) {
                types[i] = TAG_BYTES;
                values[i] = v;
            } else if (v instanceof BigDecimal) {
                types[i] = TAG_DECIMAL;
                values[i] = v;
            } else if (v instanceof BigInteger) {
                types[i] = TAG_VARINT;
                values[i] = v;
            } else if (v instanceof LocalDate) {
                types[i] = TAG_DATE;
                values[i] = v;
            } else if (v instanceof LocalTime) {
                types[i] = TAG_TIME;
                values[i] = v;
            } else if (v instanceof Ulid) {
                types[i] = TAG_ULID;
                values[i] = v;
            } else if (v instanceof Set) {
                types[i] = TAG_SET;
                values[i] = v;
            } else if (v instanceof List) {
                types[i] = TAG_LIST;
                values[i] = v;
            } else if (v instanceof MNode) {
                types[i] = TAG_MAP;
                values[i] = v;
            } else {
                throw new IllegalArgumentException(
                    "MNode.of() unsupported value type: " + v.getClass().getName()
                    + " for key '" + keys[i] + "'"
                );
            }
        }
        return new MNode(keys, types, values, enumDefs);
    }

    // ==================== Typed accessors ====================

    /// Get a String value by key.
    /// Widens across string (tag 0), text (tag 10), and ascii (tag 11).
    /// @param key the key
    /// @return the String value
    /// @throws IllegalArgumentException if the key is missing or the value is not a text type
    public String getString(String key) {
        int idx = requireIndex(key);
        if (isTextTag(types[idx])) {
            return (String) values[idx];
        }
        throw typeMismatch(key, "String (STRING, TEXT, or ASCII)", values[idx], types[idx]);
    }

    /// Get a long value by key.
    /// Widens within integer family: accepts LONG (tag 1), INT32 (tag 12),
    /// SHORT (tag 13).
    /// @param key the key
    /// @return the long value
    /// @throws IllegalArgumentException if the key is missing or the field is
    ///         not an integer type
    public long getLong(String key) {
        int idx = requireIndex(key);
        byte tag = types[idx];
        if (tag == TAG_INT) {
            return (Long) values[idx];
        }
        if (tag == TAG_INT32) {
            return ((Integer) values[idx]).longValue();
        }
        if (tag == TAG_SHORT) {
            return ((Short) values[idx]).longValue();
        }
        throw typeMismatch(key, "integer (LONG, INT32, or SHORT)", values[idx], tag);
    }

    /// Get an int value by key.
    /// Widens within integer family: accepts INT32 (tag 12), SHORT (tag 13).
    /// @param key the key
    /// @return the int value
    /// @throws IllegalArgumentException if the key is missing or the field is
    ///         not an int32 or short type
    public int getInt(String key) {
        int idx = requireIndex(key);
        byte tag = types[idx];
        if (tag == TAG_INT32) {
            return (Integer) values[idx];
        }
        if (tag == TAG_SHORT) {
            return ((Short) values[idx]).intValue();
        }
        throw typeMismatch(key, "integer (INT32 or SHORT)", values[idx], tag);
    }

    /// Get a short value by key.
    /// Only accepts SHORT (tag 13) — no widening.
    /// @param key the key
    /// @return the short value
    /// @throws IllegalArgumentException if the key is missing or the field is not SHORT
    public short getShort(String key) {
        int idx = requireIndex(key);
        if (types[idx] == TAG_SHORT) {
            return (Short) values[idx];
        }
        throw typeMismatch(key, "SHORT", values[idx], types[idx]);
    }

    /// Get a double value by key.
    /// Widens within float family: accepts DOUBLE (tag 2), FLOAT32 (tag 16),
    /// HALF (tag 17).
    /// @param key the key
    /// @return the double value
    /// @throws IllegalArgumentException if the key is missing or the field is
    ///         not a float type
    public double getDouble(String key) {
        int idx = requireIndex(key);
        byte tag = types[idx];
        if (tag == TAG_FLOAT) {
            return (Double) values[idx];
        }
        if (tag == TAG_FLOAT32) {
            return ((Float) values[idx]).doubleValue();
        }
        if (tag == TAG_HALF) {
            return ((Float) values[idx]).doubleValue();
        }
        throw typeMismatch(key, "float (DOUBLE, FLOAT32, or HALF)", values[idx], tag);
    }

    /// Get a float value by key.
    /// Widens within float family: accepts FLOAT32 (tag 16), HALF (tag 17).
    /// @param key the key
    /// @return the float value
    /// @throws IllegalArgumentException if the key is missing or the field is
    ///         not a float32 or half type
    public float getFloat(String key) {
        int idx = requireIndex(key);
        byte tag = types[idx];
        if (tag == TAG_FLOAT32) {
            return (Float) values[idx];
        }
        if (tag == TAG_HALF) {
            return (Float) values[idx];
        }
        throw typeMismatch(key, "float (FLOAT32 or HALF)", values[idx], tag);
    }

    /// Get a BigDecimal value by key.
    /// @param key the key
    /// @return the BigDecimal value
    /// @throws IllegalArgumentException if the key is missing or the field is not DECIMAL
    public BigDecimal getDecimal(String key) {
        int idx = requireIndex(key);
        if (types[idx] == TAG_DECIMAL) {
            return (BigDecimal) values[idx];
        }
        throw typeMismatch(key, "DECIMAL", values[idx], types[idx]);
    }

    /// Get a BigInteger value by key.
    /// @param key the key
    /// @return the BigInteger value
    /// @throws IllegalArgumentException if the key is missing or the field is not VARINT
    public BigInteger getVarint(String key) {
        int idx = requireIndex(key);
        if (types[idx] == TAG_VARINT) {
            return (BigInteger) values[idx];
        }
        throw typeMismatch(key, "VARINT", values[idx], types[idx]);
    }

    /// Get a boolean value by key.
    /// @param key the key
    /// @return the boolean value
    /// @throws IllegalArgumentException if the key is missing or the value is not a Boolean
    public boolean getBoolean(String key) {
        int idx = requireIndex(key);
        if (types[idx] == TAG_BOOL) {
            return (Boolean) values[idx];
        }
        throw typeMismatch(key, "Boolean", values[idx], types[idx]);
    }

    /// Get a byte array value by key.
    /// @param key the key
    /// @return the byte array
    /// @throws IllegalArgumentException if the key is missing or the value is not a byte[]
    public byte[] getBytes(String key) {
        int idx = requireIndex(key);
        if (types[idx] == TAG_BYTES) {
            return (byte[]) values[idx];
        }
        throw typeMismatch(key, "byte[]", values[idx], types[idx]);
    }

    /// Get an enum value by key as its string representation.
    ///
    /// Works for both self-describing ({@code TAG_ENUM_STR}) and
    /// ordinal-encoded ({@code TAG_ENUM_ORD}) enum fields.
    /// @param key the key
    /// @return the enum string value
    /// @throws IllegalArgumentException if the key is missing or the field is not an enum type
    /// @throws IllegalStateException if the field is ordinal-encoded with no attached definition
    public String getEnum(String key) {
        int idx = requireIndex(key);
        byte tag = types[idx];
        if (tag == TAG_ENUM_STR) {
            return (String) values[idx];
        }
        if (tag == TAG_ENUM_ORD) {
            int ordinal = (Integer) values[idx];
            if (enumDefs == null || enumDefs[idx] == null) {
                throw new IllegalStateException(
                    "Enum field '" + key + "' is ordinal-encoded (ordinal=" + ordinal
                    + ") but no enum definition is attached. Use withEnumDef() to provide one."
                );
            }
            String[] allowed = enumDefs[idx];
            if (ordinal < 0 || ordinal >= allowed.length) {
                throw new IllegalStateException(
                    "Enum field '" + key + "' ordinal " + ordinal
                    + " out of range for " + allowed.length + " allowed values"
                );
            }
            return allowed[ordinal];
        }
        throw typeMismatch(key, "Enum (ENUM_STR or ENUM_ORD)", values[idx], tag);
    }

    /// Get the raw ordinal of an ordinal-encoded enum field.
    /// @param key the key
    /// @return the ordinal value
    /// @throws IllegalArgumentException if the key is missing or not an ordinal enum
    public int getEnumOrdinal(String key) {
        int idx = requireIndex(key);
        if (types[idx] == TAG_ENUM_ORD) {
            return (Integer) values[idx];
        }
        throw typeMismatch(key, "Enum (ENUM_ORD)", values[idx], types[idx]);
    }

    /// Get a List value by key.
    /// @param key the key
    /// @return the list (unmodifiable)
    /// @throws IllegalArgumentException if the key is missing or the value is not a List
    @SuppressWarnings("unchecked")
    public List<Object> getList(String key) {
        int idx = requireIndex(key);
        if (types[idx] == TAG_LIST) {
            return (List<Object>) values[idx];
        }
        throw typeMismatch(key, "List", values[idx], types[idx]);
    }

    /// Get a nested MNode value by key.
    /// @param key the key
    /// @return the nested MNode
    /// @throws IllegalArgumentException if the key is missing or the value is not a nested MNode
    public MNode getNode(String key) {
        int idx = requireIndex(key);
        if (types[idx] == TAG_MAP) {
            return (MNode) values[idx];
        }
        throw typeMismatch(key, "MNode (MAP)", values[idx], types[idx]);
    }

    /// Get an epoch-millis Instant value by key.
    /// @param key the key
    /// @return the Instant
    /// @throws IllegalArgumentException if the key is missing or the field is not MILLIS
    public Instant getMillis(String key) {
        int idx = requireIndex(key);
        if (types[idx] == TAG_MILLIS) {
            return (Instant) values[idx];
        }
        throw typeMismatch(key, "MILLIS", values[idx], types[idx]);
    }

    /// Get an epoch-nanos Instant value by key.
    /// @param key the key
    /// @return the Instant
    /// @throws IllegalArgumentException if the key is missing or the field is not NANOS
    public Instant getNanos(String key) {
        int idx = requireIndex(key);
        if (types[idx] == TAG_NANOS) {
            return (Instant) values[idx];
        }
        throw typeMismatch(key, "NANOS", values[idx], types[idx]);
    }

    /// Get a LocalDate value by key.
    /// @param key the key
    /// @return the LocalDate
    /// @throws IllegalArgumentException if the key is missing or the field is not DATE
    public LocalDate getDate(String key) {
        int idx = requireIndex(key);
        if (types[idx] == TAG_DATE) {
            return (LocalDate) values[idx];
        }
        throw typeMismatch(key, "DATE", values[idx], types[idx]);
    }

    /// Get a LocalTime value by key.
    /// @param key the key
    /// @return the LocalTime
    /// @throws IllegalArgumentException if the key is missing or the field is not TIME
    public LocalTime getTime(String key) {
        int idx = requireIndex(key);
        if (types[idx] == TAG_TIME) {
            return (LocalTime) values[idx];
        }
        throw typeMismatch(key, "TIME", values[idx], types[idx]);
    }

    /// Get an ISO datetime Instant value by key.
    /// @param key the key
    /// @return the Instant
    /// @throws IllegalArgumentException if the key is missing or the field is not DATETIME
    public Instant getDateTime(String key) {
        int idx = requireIndex(key);
        if (types[idx] == TAG_DATETIME) {
            return (Instant) values[idx];
        }
        throw typeMismatch(key, "DATETIME", values[idx], types[idx]);
    }

    /// Get an ISO datetime value adjusted to the given time zone.
    /// @param key the key
    /// @param zone the time zone
    /// @return the ZonedDateTime
    /// @throws IllegalArgumentException if the key is missing or the field is not DATETIME
    public ZonedDateTime getDateTime(String key, ZoneId zone) {
        return getDateTime(key).atZone(zone);
    }

    /// Get a UUID v1 value by key.
    /// @param key the key
    /// @return the UUID
    /// @throws IllegalArgumentException if the key is missing or the field is not UUIDV1
    public UUID getUuidV1(String key) {
        int idx = requireIndex(key);
        if (types[idx] == TAG_UUIDV1) {
            return (UUID) values[idx];
        }
        throw typeMismatch(key, "UUIDV1", values[idx], types[idx]);
    }

    /// Get a UUID v7 value by key.
    /// @param key the key
    /// @return the UUID
    /// @throws IllegalArgumentException if the key is missing or the field is not UUIDV7
    public UUID getUuidV7(String key) {
        int idx = requireIndex(key);
        if (types[idx] == TAG_UUIDV7) {
            return (UUID) values[idx];
        }
        throw typeMismatch(key, "UUIDV7", values[idx], types[idx]);
    }

    /// Get a Ulid value by key.
    /// @param key the key
    /// @return the Ulid
    /// @throws IllegalArgumentException if the key is missing or the field is not ULID
    public Ulid getUlid(String key) {
        int idx = requireIndex(key);
        if (types[idx] == TAG_ULID) {
            return (Ulid) values[idx];
        }
        throw typeMismatch(key, "ULID", values[idx], types[idx]);
    }

    /// Get a typed array value by key, cast to the requested type.
    /// @param key the key
    /// @param type the expected array class (e.g. {@code float[].class})
    /// @param <T> the array type
    /// @return the typed array
    /// @throws IllegalArgumentException if the key is missing, not an ARRAY, or wrong element type
    @SuppressWarnings("unchecked")
    public <T> T getArray(String key, Class<T> type) {
        int idx = requireIndex(key);
        if (types[idx] != TAG_ARRAY) {
            throw typeMismatch(key, "ARRAY", values[idx], types[idx]);
        }
        TypedArrayVal tav = (TypedArrayVal) values[idx];
        if (!type.isInstance(tav.array)) {
            throw new IllegalArgumentException(
                "MNode key '" + key + "' array element type is " + tav.array.getClass().getComponentType().getName()
                + ", requested " + type.getComponentType().getName()
            );
        }
        return (T) tav.array;
    }

    /// Get the element type class of a typed array field.
    /// @param key the key
    /// @return the element type class (e.g. {@code float.class})
    /// @throws IllegalArgumentException if the key is missing or not an ARRAY
    public Class<?> getArrayElementType(String key) {
        int idx = requireIndex(key);
        if (types[idx] != TAG_ARRAY) {
            throw typeMismatch(key, "ARRAY", values[idx], types[idx]);
        }
        return ((TypedArrayVal) values[idx]).array.getClass().getComponentType();
    }

    /// Get a Set value by key.
    /// @param key the key
    /// @return the set (unmodifiable)
    /// @throws IllegalArgumentException if the key is missing or the value is not a Set
    @SuppressWarnings("unchecked")
    public Set<Object> getSet(String key) {
        int idx = requireIndex(key);
        if (types[idx] == TAG_SET) {
            return (Set<Object>) values[idx];
        }
        throw typeMismatch(key, "SET", values[idx], types[idx]);
    }

    /// Get a typed-key map value by key.
    /// @param key the key
    /// @return the map (unmodifiable)
    /// @throws IllegalArgumentException if the key is missing or the value is not a TYPED_MAP
    @SuppressWarnings("unchecked")
    public Map<Object, Object> getTypedMap(String key) {
        int idx = requireIndex(key);
        if (types[idx] == TAG_TYPED_MAP) {
            return (Map<Object, Object>) values[idx];
        }
        throw typeMismatch(key, "TYPED_MAP", values[idx], types[idx]);
    }

    /// Attach an enum definition to an ordinal-encoded enum field.
    /// Returns a new MNode sharing all data with this one except for the
    /// added definition. This is a cheap operation.
    /// @param key the field name
    /// @param allowedValues the complete list of allowed enum string values
    /// @return a new MNode with the definition attached
    /// @throws IllegalArgumentException if the key is missing or is not an
    ///         ordinal-encoded enum field
    public MNode withEnumDef(String key, String... allowedValues) {
        int idx = indexOf(key);
        if (idx < 0) {
            throw new IllegalArgumentException("MNode missing key: " + key);
        }
        if (types[idx] != TAG_ENUM_ORD) {
            throw new IllegalArgumentException(
                "withEnumDef() requires an ENUM_ORD field, but '" + key + "' has tag " + types[idx]
            );
        }
        String[][] newDefs = enumDefs != null ? enumDefs.clone() : new String[keys.length][];
        newDefs[idx] = allowedValues.clone();
        return new MNode(keys, types, values, newDefs);
    }

    // ==================== Optional (find*) variants ====================

    /// Find a String value by key.
    /// Widens across string/text/ascii tags.
    /// @param key the key
    /// @return an Optional containing the value, or empty if the key is absent or not a text type
    public Optional<String> findString(String key) {
        int idx = indexOf(key);
        if (idx >= 0 && values[idx] != null && isTextTag(types[idx])) {
            return Optional.of((String) values[idx]);
        }
        return Optional.empty();
    }

    /// Find a Long value by key.
    /// Checks for LONG, INT32, or SHORT tags.
    /// @param key the key
    /// @return an Optional containing the value, or empty if the key is absent or not an integer type
    public Optional<Long> findLong(String key) {
        int idx = indexOf(key);
        if (idx >= 0 && values[idx] != null) {
            byte tag = types[idx];
            if (tag == TAG_INT || tag == TAG_INT32 || tag == TAG_SHORT) {
                return Optional.of(getLong(key));
            }
        }
        return Optional.empty();
    }

    /// Find a Double value by key.
    /// Checks for DOUBLE, FLOAT32, or HALF tags.
    /// @param key the key
    /// @return an Optional containing the value, or empty if the key is absent or not a float type
    public Optional<Double> findDouble(String key) {
        int idx = indexOf(key);
        if (idx >= 0 && values[idx] != null) {
            byte tag = types[idx];
            if (tag == TAG_FLOAT || tag == TAG_FLOAT32 || tag == TAG_HALF) {
                return Optional.of(getDouble(key));
            }
        }
        return Optional.empty();
    }

    /// Find a Boolean value by key
    /// @param key the key
    /// @return an Optional containing the value, or empty if the key is absent or not BOOL
    public Optional<Boolean> findBoolean(String key) {
        return hasTyped(key, TAG_BOOL) ? Optional.of(getBoolean(key)) : Optional.empty();
    }

    /// Find a byte array value by key
    /// @param key the key
    /// @return an Optional containing the value, or empty if the key is absent or not BYTES
    public Optional<byte[]> findBytes(String key) {
        return hasTyped(key, TAG_BYTES) ? Optional.of(getBytes(key)) : Optional.empty();
    }

    /// Find an enum value by key
    /// @param key the key
    /// @return an Optional containing the string value, or empty if the key
    ///         is absent or not an enum type
    public Optional<String> findEnum(String key) {
        int idx = indexOf(key);
        if (idx < 0 || values[idx] == null) return Optional.empty();
        byte tag = types[idx];
        if (tag == TAG_ENUM_STR || tag == TAG_ENUM_ORD) {
            return Optional.of(getEnum(key));
        }
        return Optional.empty();
    }

    /// Find a List value by key
    /// @param key the key
    /// @return an Optional containing the list, or empty if the key is absent or not LIST
    public Optional<List<Object>> findList(String key) {
        return hasTyped(key, TAG_LIST) ? Optional.of(getList(key)) : Optional.empty();
    }

    /// Find a nested MNode value by key
    /// @param key the key
    /// @return an Optional containing the MNode, or empty if the key is absent or not MAP
    public Optional<MNode> findNode(String key) {
        return hasTyped(key, TAG_MAP) ? Optional.of(getNode(key)) : Optional.empty();
    }

    /// Find a BigDecimal value by key
    /// @param key the key
    /// @return an Optional containing the value, or empty if absent or not DECIMAL
    public Optional<BigDecimal> findDecimal(String key) {
        return hasTyped(key, TAG_DECIMAL) ? Optional.of(getDecimal(key)) : Optional.empty();
    }

    /// Find a BigInteger value by key
    /// @param key the key
    /// @return an Optional containing the value, or empty if absent or not VARINT
    public Optional<BigInteger> findVarint(String key) {
        return hasTyped(key, TAG_VARINT) ? Optional.of(getVarint(key)) : Optional.empty();
    }

    /// Find an epoch-millis Instant by key
    /// @param key the key
    /// @return an Optional containing the value, or empty if absent or not MILLIS
    public Optional<Instant> findMillis(String key) {
        return hasTyped(key, TAG_MILLIS) ? Optional.of(getMillis(key)) : Optional.empty();
    }

    /// Find an epoch-nanos Instant by key
    /// @param key the key
    /// @return an Optional containing the value, or empty if absent or not NANOS
    public Optional<Instant> findNanos(String key) {
        return hasTyped(key, TAG_NANOS) ? Optional.of(getNanos(key)) : Optional.empty();
    }

    /// Find a LocalDate by key
    /// @param key the key
    /// @return an Optional containing the value, or empty if absent or not DATE
    public Optional<LocalDate> findDate(String key) {
        return hasTyped(key, TAG_DATE) ? Optional.of(getDate(key)) : Optional.empty();
    }

    /// Find a LocalTime by key
    /// @param key the key
    /// @return an Optional containing the value, or empty if absent or not TIME
    public Optional<LocalTime> findTime(String key) {
        return hasTyped(key, TAG_TIME) ? Optional.of(getTime(key)) : Optional.empty();
    }

    /// Find a datetime Instant by key
    /// @param key the key
    /// @return an Optional containing the value, or empty if absent or not DATETIME
    public Optional<Instant> findDateTime(String key) {
        return hasTyped(key, TAG_DATETIME) ? Optional.of(getDateTime(key)) : Optional.empty();
    }

    /// Find a UUID v1 by key
    /// @param key the key
    /// @return an Optional containing the value, or empty if absent or not UUIDV1
    public Optional<UUID> findUuidV1(String key) {
        return hasTyped(key, TAG_UUIDV1) ? Optional.of(getUuidV1(key)) : Optional.empty();
    }

    /// Find a UUID v7 by key
    /// @param key the key
    /// @return an Optional containing the value, or empty if absent or not UUIDV7
    public Optional<UUID> findUuidV7(String key) {
        return hasTyped(key, TAG_UUIDV7) ? Optional.of(getUuidV7(key)) : Optional.empty();
    }

    /// Find a Ulid by key
    /// @param key the key
    /// @return an Optional containing the value, or empty if absent or not ULID
    public Optional<Ulid> findUlid(String key) {
        return hasTyped(key, TAG_ULID) ? Optional.of(getUlid(key)) : Optional.empty();
    }

    /// Find a Set by key
    /// @param key the key
    /// @return an Optional containing the set, or empty if absent or not SET
    public Optional<Set<Object>> findSet(String key) {
        return hasTyped(key, TAG_SET) ? Optional.of(getSet(key)) : Optional.empty();
    }

    /// Find a typed-key map by key
    /// @param key the key
    /// @return an Optional containing the map, or empty if absent or not TYPED_MAP
    public Optional<Map<Object, Object>> findTypedMap(String key) {
        return hasTyped(key, TAG_TYPED_MAP) ? Optional.of(getTypedMap(key)) : Optional.empty();
    }

    /// Find an int value by key.
    /// Widens within integer family: accepts INT32 (tag 12) and SHORT (tag 13).
    /// @param key the key
    /// @return an Optional containing the value, or empty if the key is absent or not an int/short type
    public Optional<Integer> findInt(String key) {
        int idx = indexOf(key);
        if (idx >= 0 && values[idx] != null) {
            byte tag = types[idx];
            if (tag == TAG_INT32 || tag == TAG_SHORT) {
                return Optional.of(getInt(key));
            }
        }
        return Optional.empty();
    }

    /// Find a short value by key.
    /// Only accepts SHORT (tag 13) — no widening.
    /// @param key the key
    /// @return an Optional containing the value, or empty if the key is absent or not SHORT
    public Optional<Short> findShort(String key) {
        return hasTyped(key, TAG_SHORT) ? Optional.of(getShort(key)) : Optional.empty();
    }

    /// Find a float value by key.
    /// Widens within float family: accepts FLOAT32 (tag 16) and HALF (tag 17).
    /// @param key the key
    /// @return an Optional containing the value, or empty if the key is absent or not a float32/half type
    public Optional<Float> findFloat(String key) {
        int idx = indexOf(key);
        if (idx >= 0 && values[idx] != null) {
            byte tag = types[idx];
            if (tag == TAG_FLOAT32 || tag == TAG_HALF) {
                return Optional.of(getFloat(key));
            }
        }
        return Optional.empty();
    }

    /// Find an enum ordinal value by key.
    /// Only accepts ENUM_ORD (tag 7).
    /// @param key the key
    /// @return an Optional containing the ordinal, or empty if absent or not ENUM_ORD
    public Optional<Integer> findEnumOrdinal(String key) {
        return hasTyped(key, TAG_ENUM_ORD) ? Optional.of(getEnumOrdinal(key)) : Optional.empty();
    }

    /// Find a typed array value by key, cast to the requested type.
    /// @param key the key
    /// @param type the expected array class (e.g. {@code float[].class})
    /// @param <T> the array type
    /// @return an Optional containing the typed array, or empty if absent or not ARRAY or wrong type
    @SuppressWarnings("unchecked")
    public <T> Optional<T> findArray(String key, Class<T> type) {
        int idx = indexOf(key);
        if (idx >= 0 && values[idx] != null && types[idx] == TAG_ARRAY) {
            TypedArrayVal tav = (TypedArrayVal) values[idx];
            if (type.isInstance(tav.array)) {
                return Optional.of((T) tav.array);
            }
        }
        return Optional.empty();
    }

    /// Find the element type class of a typed array field.
    /// @param key the key
    /// @return an Optional containing the element type class, or empty if absent or not ARRAY
    public Optional<Class<?>> findArrayElementType(String key) {
        int idx = indexOf(key);
        if (idx >= 0 && values[idx] != null && types[idx] == TAG_ARRAY) {
            return Optional.of(((TypedArrayVal) values[idx]).array.getClass().getComponentType());
        }
        return Optional.empty();
    }

    // ==================== Raw access ====================

    /// Return an unmodifiable view of the underlying data as a map.
    /// This allocates a new Map — prefer typed accessors for hot paths.
    /// @return the metadata map
    public Map<String, Object> toMap() {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < keys.length; i++) {
            map.put(keys[i], values[i]);
        }
        return Collections.unmodifiableMap(map);
    }

    /// Get a raw value by key
    /// @param key the key
    /// @return the value, or null if absent
    public Object get(String key) {
        int idx = indexOf(key);
        return idx >= 0 ? values[idx] : null;
    }

    /// Check whether a key is present and its value is not null
    /// @param key the key
    /// @return true if the key exists and its value is not null
    public boolean has(String key) {
        int idx = indexOf(key);
        return idx >= 0 && values[idx] != null;
    }

    /// Return the number of entries in this node
    /// @return the entry count
    public int size() {
        return keys.length;
    }

    /// Return the field names. Package-private for codec support.
    /// @return defensive copy of the keys array
    String[] keys() {
        return keys.clone();
    }

    /// Return the type tags. Package-private for codec support.
    /// @return defensive copy of the types array
    byte[] types() {
        return types.clone();
    }

    /// Return the values. Package-private for codec support.
    /// @return defensive copy of the values array
    Object[] values() {
        return values.clone();
    }

    /// Return the enum definitions. Package-private for codec support.
    /// @return the enum definitions array, or null if none
    String[][] enumDefs() {
        return enumDefs != null ? enumDefs.clone() : null;
    }

    // ==================== Binary codec ====================

    /// Encode this MNode to a byte array with dialect leader (no length prefix).
    ///
    /// Wire format:
    /// {@code [dialect:1][field_count:2][per-field: nameLen:2, nameUtf8:N, typeTag:1, valueBytes...]}
    /// @return the encoded bytes
    public byte[] toBytes() {
        int size = 1 + 2; // dialect + field count
        byte[][] nameBytes = new byte[keys.length][];
        for (int i = 0; i < keys.length; i++) {
            nameBytes[i] = keys[i].getBytes(StandardCharsets.UTF_8);
            size += 2 + nameBytes[i].length + 1; // nameLen + name + typeTag
            size += valueSizeOf(types[i], values[i]);
        }

        ByteBuffer buf = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        buf.put(DIALECT);
        buf.putShort((short) keys.length);
        for (int i = 0; i < keys.length; i++) {
            buf.putShort((short) nameBytes[i].length);
            buf.put(nameBytes[i]);
            buf.put(types[i]);
            encodeValue(buf, types[i], values[i]);
        }
        return buf.array();
    }

    /// Decode an MNode from raw bytes with dialect leader.
    ///
    /// The first byte must be {@code 0x01} (the MNode dialect leader); it is
    /// consumed before decoding the payload.
    ///
    /// @param bytes the encoded bytes (with dialect leader)
    /// @return the decoded MNode
    /// @throws IllegalArgumentException if the first byte is not the dialect leader
    public static MNode fromBytes(byte[] bytes) {
        if (bytes.length == 0 || bytes[0] != DIALECT) {
            throw new IllegalArgumentException(
                "Expected MNode dialect leader 0x01, got 0x"
                + (bytes.length > 0 ? Integer.toHexString(bytes[0] & 0xFF) : "empty"));
        }
        ByteBuffer buf = ByteBuffer.wrap(bytes, 1, bytes.length - 1).order(ByteOrder.LITTLE_ENDIAN);
        return decodePayload(buf);
    }

    /// Encode this MNode into a length-prefixed ByteBuffer with dialect leader.
    /// Wire format: {@code [totalLen:4][dialect:1][payload...]}.
    /// @param out the buffer to write into
    /// @return the same buffer, for chaining
    public ByteBuffer encode(ByteBuffer out) {
        byte[] payload = toBytes();
        out.order(ByteOrder.LITTLE_ENDIAN);
        out.putInt(payload.length);
        out.put(payload);
        return out;
    }

    /// Decode an MNode from a length-prefixed ByteBuffer.
    ///
    /// Wire format: {@code [totalLen:4][dialect:1][payload...]}.
    /// @param buf the buffer to read from
    /// @return the decoded MNode
    /// @throws IllegalArgumentException if the dialect leader is missing
    public static MNode fromBuffer(ByteBuffer buf) {
        buf.order(ByteOrder.LITTLE_ENDIAN);
        int length = buf.getInt();
        int startPos = buf.position();
        byte leader = buf.get();
        if (leader != DIALECT) {
            throw new IllegalArgumentException(
                "Expected MNode dialect leader 0x01, got 0x" + Integer.toHexString(leader & 0xFF));
        }
        MNode node = decodePayload(buf);
        buf.position(startPos + length);
        return node;
    }

    // ==================== Encoding internals ====================

    private static int valueSizeOf(byte typeTag, Object value) {
        switch (typeTag) {
            case TAG_TEXT:
            case TAG_ENUM_STR:
            case TAG_TEXT_VALIDATED:
            case TAG_ASCII:
                return 4 + ((String) value).getBytes(StandardCharsets.UTF_8).length;
            case TAG_INT:
                return 8;
            case TAG_FLOAT:
                return 8;
            case TAG_BOOL:
                return 1;
            case TAG_BYTES:
                return 4 + ((byte[]) value).length;
            case TAG_NULL:
                return 0;
            case TAG_ENUM_ORD:
                return 4;
            case TAG_INT32:
                return 4;
            case TAG_SHORT:
                return 2;
            case TAG_DECIMAL: {
                BigDecimal bd = (BigDecimal) value;
                return 4 + 4 + bd.unscaledValue().toByteArray().length; // scale + len + bytes
            }
            case TAG_VARINT: {
                BigInteger bi = (BigInteger) value;
                return 4 + bi.toByteArray().length; // len + bytes
            }
            case TAG_FLOAT32:
                return 4;
            case TAG_HALF:
                return 2;
            case TAG_MILLIS:
                return 8;
            case TAG_NANOS:
                return 12; // seconds(8) + nanoAdjust(4)
            case TAG_DATE:
            case TAG_TIME:
            case TAG_DATETIME: {
                String isoStr = temporalToString(typeTag, value);
                return 4 + isoStr.getBytes(StandardCharsets.UTF_8).length;
            }
            case TAG_UUIDV1:
            case TAG_UUIDV7:
                return 16;
            case TAG_ULID:
                return 16;
            case TAG_LIST: {
                List<?> list = (List<?>) value;
                int s = 4; // element count
                for (Object elem : list) {
                    byte elemTag = inferTag(elem);
                    s += 1 + valueSizeOf(elemTag, normalizeValue(elemTag, elem));
                }
                return s;
            }
            case TAG_MAP:
                return 4 + ((MNode) value).toBytes().length;
            case TAG_ARRAY: {
                TypedArrayVal tav = (TypedArrayVal) value;
                int elemSize = fixedSizeOf(tav.elemTag);
                int elemCount = arrayLength(tav.array);
                return 1 + 4 + elemCount * elemSize; // elemTag + count + data
            }
            case TAG_SET: {
                Set<?> set = (Set<?>) value;
                int s = 4; // count
                for (Object elem : set) {
                    byte elemTag = inferTag(elem);
                    s += 1 + valueSizeOf(elemTag, normalizeValue(elemTag, elem));
                }
                return s;
            }
            case TAG_TYPED_MAP: {
                Map<?, ?> map = (Map<?, ?>) value;
                int s = 4; // count
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    byte keyTag = inferTag(entry.getKey());
                    byte valTag = inferTag(entry.getValue());
                    s += 1 + valueSizeOf(keyTag, normalizeValue(keyTag, entry.getKey()));
                    s += 1 + valueSizeOf(valTag, normalizeValue(valTag, entry.getValue()));
                }
                return s;
            }
            default:
                throw new IllegalStateException("Unknown type tag: " + typeTag);
        }
    }

    private static void encodeValue(ByteBuffer buf, byte typeTag, Object value) {
        switch (typeTag) {
            case TAG_TEXT:
            case TAG_ENUM_STR:
            case TAG_TEXT_VALIDATED:
            case TAG_ASCII: {
                byte[] textBytes = ((String) value).getBytes(StandardCharsets.UTF_8);
                buf.putInt(textBytes.length);
                buf.put(textBytes);
                break;
            }
            case TAG_INT:
                buf.putLong((Long) value);
                break;
            case TAG_FLOAT:
                buf.putDouble((Double) value);
                break;
            case TAG_BOOL:
                buf.put((byte) (Boolean.TRUE.equals(value) ? 1 : 0));
                break;
            case TAG_BYTES: {
                byte[] blob = (byte[]) value;
                buf.putInt(blob.length);
                buf.put(blob);
                break;
            }
            case TAG_NULL:
                break;
            case TAG_ENUM_ORD:
                buf.putInt((Integer) value);
                break;
            case TAG_INT32:
                buf.putInt((Integer) value);
                break;
            case TAG_SHORT:
                buf.putShort((Short) value);
                break;
            case TAG_DECIMAL: {
                BigDecimal bd = (BigDecimal) value;
                buf.putInt(bd.scale());
                byte[] unscaled = bd.unscaledValue().toByteArray();
                buf.putInt(unscaled.length);
                buf.put(unscaled);
                break;
            }
            case TAG_VARINT: {
                byte[] biBytes = ((BigInteger) value).toByteArray();
                buf.putInt(biBytes.length);
                buf.put(biBytes);
                break;
            }
            case TAG_FLOAT32:
                buf.putFloat((Float) value);
                break;
            case TAG_HALF:
                buf.putShort(floatToHalf((Float) value));
                break;
            case TAG_MILLIS:
                buf.putLong(((Instant) value).toEpochMilli());
                break;
            case TAG_NANOS: {
                Instant inst = (Instant) value;
                buf.putLong(inst.getEpochSecond());
                buf.putInt(inst.getNano());
                break;
            }
            case TAG_DATE:
            case TAG_TIME:
            case TAG_DATETIME: {
                byte[] isoBytes = temporalToString(typeTag, value).getBytes(StandardCharsets.UTF_8);
                buf.putInt(isoBytes.length);
                buf.put(isoBytes);
                break;
            }
            case TAG_UUIDV1:
            case TAG_UUIDV7: {
                UUID uuid = (UUID) value;
                buf.putLong(uuid.getMostSignificantBits());
                buf.putLong(uuid.getLeastSignificantBits());
                break;
            }
            case TAG_ULID: {
                buf.put(((Ulid) value).toBytes());
                break;
            }
            case TAG_LIST: {
                List<?> list = (List<?>) value;
                buf.putInt(list.size());
                for (Object elem : list) {
                    byte elemTag = inferTag(elem);
                    Object normalized = normalizeValue(elemTag, elem);
                    buf.put(elemTag);
                    encodeValue(buf, elemTag, normalized);
                }
                break;
            }
            case TAG_MAP: {
                byte[] payload = ((MNode) value).toBytes();
                buf.putInt(payload.length);
                buf.put(payload);
                break;
            }
            case TAG_ARRAY: {
                TypedArrayVal tav = (TypedArrayVal) value;
                buf.put(tav.elemTag);
                encodeTypedArray(buf, tav);
                break;
            }
            case TAG_SET: {
                Set<?> set = (Set<?>) value;
                List<Object> sorted = new ArrayList<>(set);
                sorted.sort(MNode::compareScalars);
                buf.putInt(sorted.size());
                for (Object elem : sorted) {
                    byte elemTag = inferTag(elem);
                    Object normalized = normalizeValue(elemTag, elem);
                    buf.put(elemTag);
                    encodeValue(buf, elemTag, normalized);
                }
                break;
            }
            case TAG_TYPED_MAP: {
                Map<?, ?> map = (Map<?, ?>) value;
                List<? extends Map.Entry<?, ?>> sortedEntries = new ArrayList<>(map.entrySet());
                sortedEntries.sort((e1, e2) -> compareScalars(e1.getKey(), e2.getKey()));
                buf.putInt(sortedEntries.size());
                for (Map.Entry<?, ?> entry : sortedEntries) {
                    byte keyTag = inferTag(entry.getKey());
                    Object normalizedKey = normalizeValue(keyTag, entry.getKey());
                    buf.put(keyTag);
                    encodeValue(buf, keyTag, normalizedKey);
                    byte valTag = inferTag(entry.getValue());
                    Object normalizedVal = normalizeValue(valTag, entry.getValue());
                    buf.put(valTag);
                    encodeValue(buf, valTag, normalizedVal);
                }
                break;
            }
            default:
                throw new IllegalStateException("Unknown type tag: " + typeTag);
        }
    }

    private static MNode decodePayload(ByteBuffer buf) {
        int fieldCount = Short.toUnsignedInt(buf.getShort());
        String[] keys = new String[fieldCount];
        byte[] types = new byte[fieldCount];
        Object[] values = new Object[fieldCount];

        for (int i = 0; i < fieldCount; i++) {
            int nameLen = Short.toUnsignedInt(buf.getShort());
            byte[] nameBytes = new byte[nameLen];
            buf.get(nameBytes);
            keys[i] = new String(nameBytes, StandardCharsets.UTF_8);

            byte typeTag = buf.get();
            types[i] = typeTag;
            values[i] = decodeValue(buf, typeTag);
        }
        return new MNode(keys, types, values, null);
    }

    private static Object decodeValue(ByteBuffer buf, byte typeTag) {
        switch (typeTag) {
            case TAG_TEXT:
            case TAG_ENUM_STR:
            case TAG_TEXT_VALIDATED:
            case TAG_ASCII: {
                int len = buf.getInt();
                byte[] textBytes = new byte[len];
                buf.get(textBytes);
                return new String(textBytes, StandardCharsets.UTF_8);
            }
            case TAG_INT:
                return buf.getLong();
            case TAG_FLOAT:
                return buf.getDouble();
            case TAG_BOOL:
                return buf.get() != 0;
            case TAG_BYTES: {
                int len = buf.getInt();
                byte[] blob = new byte[len];
                buf.get(blob);
                return blob;
            }
            case TAG_NULL:
                return null;
            case TAG_ENUM_ORD:
                return buf.getInt();
            case TAG_INT32:
                return buf.getInt();
            case TAG_SHORT:
                return buf.getShort();
            case TAG_DECIMAL: {
                int scale = buf.getInt();
                int len = buf.getInt();
                byte[] unscaled = new byte[len];
                buf.get(unscaled);
                return new BigDecimal(new BigInteger(unscaled), scale);
            }
            case TAG_VARINT: {
                int len = buf.getInt();
                byte[] biBytes = new byte[len];
                buf.get(biBytes);
                return new BigInteger(biBytes);
            }
            case TAG_FLOAT32:
                return buf.getFloat();
            case TAG_HALF:
                return halfToFloat(buf.getShort());
            case TAG_MILLIS:
                return Instant.ofEpochMilli(buf.getLong());
            case TAG_NANOS: {
                long seconds = buf.getLong();
                int nanoAdj = buf.getInt();
                return Instant.ofEpochSecond(seconds, nanoAdj);
            }
            case TAG_DATE: {
                int len = buf.getInt();
                byte[] isoBytes = new byte[len];
                buf.get(isoBytes);
                return LocalDate.parse(new String(isoBytes, StandardCharsets.UTF_8));
            }
            case TAG_TIME: {
                int len = buf.getInt();
                byte[] isoBytes = new byte[len];
                buf.get(isoBytes);
                return LocalTime.parse(new String(isoBytes, StandardCharsets.UTF_8));
            }
            case TAG_DATETIME: {
                int len = buf.getInt();
                byte[] isoBytes = new byte[len];
                buf.get(isoBytes);
                return Instant.parse(new String(isoBytes, StandardCharsets.UTF_8));
            }
            case TAG_UUIDV1:
            case TAG_UUIDV7:
                return new UUID(buf.getLong(), buf.getLong());
            case TAG_ULID: {
                byte[] ulidBytes = new byte[16];
                buf.get(ulidBytes);
                return Ulid.of(ulidBytes);
            }
            case TAG_LIST: {
                int count = buf.getInt();
                List<Object> list = new ArrayList<>(count);
                for (int j = 0; j < count; j++) {
                    byte elemTag = buf.get();
                    list.add(decodeValue(buf, elemTag));
                }
                return Collections.unmodifiableList(list);
            }
            case TAG_MAP: {
                int payloadLen = buf.getInt();
                int startPos = buf.position();
                // consume dialect leader if present
                if (buf.remaining() > 0 && buf.get(buf.position()) == DIALECT) {
                    buf.get();
                }
                MNode nested = decodePayload(buf);
                buf.position(startPos + payloadLen);
                return nested;
            }
            case TAG_ARRAY:
                return decodeTypedArray(buf);
            case TAG_SET: {
                int count = buf.getInt();
                LinkedHashSet<Object> set = new LinkedHashSet<>(count);
                for (int j = 0; j < count; j++) {
                    byte elemTag = buf.get();
                    Object elem = decodeValue(buf, elemTag);
                    if (!set.add(elem)) {
                        throw new IllegalArgumentException("Duplicate element in SET: " + elem);
                    }
                }
                return Collections.unmodifiableSet(set);
            }
            case TAG_TYPED_MAP: {
                int count = buf.getInt();
                LinkedHashMap<Object, Object> map = new LinkedHashMap<>(count);
                for (int j = 0; j < count; j++) {
                    byte keyTag = buf.get();
                    Object key = decodeValue(buf, keyTag);
                    byte valTag = buf.get();
                    Object val = decodeValue(buf, valTag);
                    map.put(key, val);
                }
                return Collections.unmodifiableMap(map);
            }
            default:
                throw new IllegalStateException("Unknown type tag: " + typeTag);
        }
    }

    // ==================== Typed array encode/decode ====================

    private static void encodeTypedArray(ByteBuffer buf, TypedArrayVal tav) {
        Object arr = tav.array;
        if (arr instanceof long[]) {
            long[] a = (long[]) arr;
            buf.putInt(a.length);
            for (long v : a) buf.putLong(v);
        } else if (arr instanceof int[]) {
            int[] a = (int[]) arr;
            buf.putInt(a.length);
            for (int v : a) buf.putInt(v);
        } else if (arr instanceof short[]) {
            short[] a = (short[]) arr;
            buf.putInt(a.length);
            for (short v : a) buf.putShort(v);
        } else if (arr instanceof double[]) {
            double[] a = (double[]) arr;
            buf.putInt(a.length);
            for (double v : a) buf.putDouble(v);
        } else if (arr instanceof float[]) {
            float[] a = (float[]) arr;
            buf.putInt(a.length);
            for (float v : a) buf.putFloat(v);
        } else if (arr instanceof boolean[]) {
            boolean[] a = (boolean[]) arr;
            buf.putInt(a.length);
            for (boolean v : a) buf.put((byte) (v ? 1 : 0));
        } else if (arr instanceof Half[]) {
            Half[] a = (Half[]) arr;
            buf.putInt(a.length);
            for (Half v : a) buf.putShort(v.toBits());
        } else if (arr instanceof Instant[]) {
            Instant[] a = (Instant[]) arr;
            buf.putInt(a.length);
            if (tav.elemTag == TAG_MILLIS) {
                for (Instant v : a) buf.putLong(v.toEpochMilli());
            } else { // TAG_NANOS
                for (Instant v : a) {
                    buf.putLong(v.getEpochSecond());
                    buf.putInt(v.getNano());
                }
            }
        } else if (arr instanceof UUID[]) {
            UUID[] a = (UUID[]) arr;
            buf.putInt(a.length);
            for (UUID v : a) {
                buf.putLong(v.getMostSignificantBits());
                buf.putLong(v.getLeastSignificantBits());
            }
        } else if (arr instanceof Ulid[]) {
            Ulid[] a = (Ulid[]) arr;
            buf.putInt(a.length);
            for (Ulid v : a) buf.put(v.toBytes());
        } else {
            throw new IllegalStateException("Unsupported typed array: " + arr.getClass().getName());
        }
    }

    private static TypedArrayVal decodeTypedArray(ByteBuffer buf) {
        byte elemTag = buf.get();
        int count = buf.getInt();
        switch (elemTag) {
            case TAG_INT: {
                long[] a = new long[count];
                for (int i = 0; i < count; i++) a[i] = buf.getLong();
                return new TypedArrayVal(a, elemTag);
            }
            case TAG_INT32: {
                int[] a = new int[count];
                for (int i = 0; i < count; i++) a[i] = buf.getInt();
                return new TypedArrayVal(a, elemTag);
            }
            case TAG_SHORT: {
                short[] a = new short[count];
                for (int i = 0; i < count; i++) a[i] = buf.getShort();
                return new TypedArrayVal(a, elemTag);
            }
            case TAG_FLOAT: {
                double[] a = new double[count];
                for (int i = 0; i < count; i++) a[i] = buf.getDouble();
                return new TypedArrayVal(a, elemTag);
            }
            case TAG_FLOAT32: {
                float[] a = new float[count];
                for (int i = 0; i < count; i++) a[i] = buf.getFloat();
                return new TypedArrayVal(a, elemTag);
            }
            case TAG_BOOL: {
                boolean[] a = new boolean[count];
                for (int i = 0; i < count; i++) a[i] = buf.get() != 0;
                return new TypedArrayVal(a, elemTag);
            }
            case TAG_HALF: {
                Half[] a = new Half[count];
                for (int i = 0; i < count; i++) a[i] = Half.fromBits(buf.getShort());
                return new TypedArrayVal(a, elemTag);
            }
            case TAG_ENUM_ORD: {
                int[] a = new int[count];
                for (int i = 0; i < count; i++) a[i] = buf.getInt();
                return new TypedArrayVal(a, elemTag);
            }
            case TAG_MILLIS: {
                Instant[] a = new Instant[count];
                for (int i = 0; i < count; i++) a[i] = Instant.ofEpochMilli(buf.getLong());
                return new TypedArrayVal(a, elemTag);
            }
            case TAG_NANOS: {
                Instant[] a = new Instant[count];
                for (int i = 0; i < count; i++) {
                    long seconds = buf.getLong();
                    int nanoAdj = buf.getInt();
                    a[i] = Instant.ofEpochSecond(seconds, nanoAdj);
                }
                return new TypedArrayVal(a, elemTag);
            }
            case TAG_UUIDV1:
            case TAG_UUIDV7: {
                UUID[] a = new UUID[count];
                for (int i = 0; i < count; i++) a[i] = new UUID(buf.getLong(), buf.getLong());
                return new TypedArrayVal(a, elemTag);
            }
            case TAG_ULID: {
                Ulid[] a = new Ulid[count];
                for (int i = 0; i < count; i++) {
                    byte[] ulidBytes = new byte[16];
                    buf.get(ulidBytes);
                    a[i] = Ulid.of(ulidBytes);
                }
                return new TypedArrayVal(a, elemTag);
            }
            default:
                throw new IllegalStateException("Unsupported typed array element tag: " + elemTag);
        }
    }

    // ==================== Half-precision IEEE 754 conversion ====================

    /// Convert a float to IEEE 754 binary16 (half-precision) bits.
    /// Pure-Java bit manipulation — works on Java 11+.
    static short floatToHalf(float value) {
        int fbits = Float.floatToIntBits(value);
        int sign = (fbits >>> 16) & 0x8000;
        int exp = ((fbits >>> 23) & 0xFF) - 127;
        int mantissa = fbits & 0x007FFFFF;

        if (exp > 15) {
            // Overflow → ±Infinity (or NaN if mantissa != 0)
            if (exp == 128 && mantissa != 0) {
                return (short) (sign | 0x7C00 | (mantissa >>> 13)); // NaN
            }
            return (short) (sign | 0x7C00); // ±Inf
        }
        if (exp > -15) {
            // Normal half
            int roundBit = 1 << 12;
            int raw = sign | ((exp + 15) << 10) | (mantissa >>> 13);
            // Round to nearest, ties to even
            if ((mantissa & roundBit) != 0) {
                if ((mantissa & (roundBit - 1)) != 0 || (raw & 1) != 0) {
                    raw++;
                }
            }
            return (short) raw;
        }
        if (exp >= -24) {
            // Subnormal half
            mantissa |= 0x00800000; // add implicit leading 1
            int shift = -1 - exp; // shift amount: 14 for exp=-15, up to 24 for exp=-24
            int roundBit = 1 << (shift - 1 + 13);
            int raw = sign | (mantissa >>> (shift + 13));
            if (shift + 13 < 32) {
                int remainder = mantissa & ((1 << (shift + 13)) - 1);
                if ((remainder & roundBit) != 0) {
                    if ((remainder & (roundBit - 1)) != 0 || (raw & 1) != 0) {
                        raw++;
                    }
                }
            }
            return (short) raw;
        }
        // Too small → ±0
        return (short) sign;
    }

    /// Convert IEEE 754 binary16 bits to a float.
    /// Pure-Java bit manipulation — works on Java 11+.
    static float halfToFloat(short bits) {
        int h = bits & 0xFFFF;
        int sign = (h & 0x8000) << 16;
        int exp = (h >>> 10) & 0x1F;
        int mantissa = h & 0x03FF;

        if (exp == 0) {
            if (mantissa == 0) {
                // ±0
                return Float.intBitsToFloat(sign);
            }
            // Subnormal → normalize
            while ((mantissa & 0x0400) == 0) {
                mantissa <<= 1;
                exp--;
            }
            exp++;
            mantissa &= 0x03FF;
            return Float.intBitsToFloat(sign | ((exp + 112) << 23) | (mantissa << 13));
        }
        if (exp == 31) {
            // Inf or NaN
            return Float.intBitsToFloat(sign | 0x7F800000 | (mantissa << 13));
        }
        // Normal
        return Float.intBitsToFloat(sign | ((exp + 112) << 23) | (mantissa << 13));
    }

    // ==================== Helper methods ====================

    /// Check if a tag is a text-family tag (STRING, TEXT_VALIDATED, ASCII)
    private static boolean isTextTag(byte tag) {
        return tag == TAG_TEXT || tag == TAG_TEXT_VALIDATED || tag == TAG_ASCII;
    }

    /// Return the fixed byte size for an array element tag
    private static int fixedSizeOf(byte elemTag) {
        switch (elemTag) {
            case TAG_BOOL:  return 1;
            case TAG_SHORT: return 2;
            case TAG_HALF:  return 2;
            case TAG_INT32: return 4;
            case TAG_ENUM_ORD: return 4;
            case TAG_FLOAT32: return 4;
            case TAG_INT:   return 8;
            case TAG_FLOAT: return 8;
            case TAG_MILLIS: return 8;
            case TAG_NANOS: return 12;
            case TAG_UUIDV1: return 16;
            case TAG_UUIDV7: return 16;
            case TAG_ULID:  return 16;
            default:
                throw new IllegalArgumentException("Element tag " + elemTag + " is not a fixed-size type");
        }
    }

    /// Get the length of a typed array object
    private static int arrayLength(Object arr) {
        if (arr instanceof long[]) return ((long[]) arr).length;
        if (arr instanceof int[]) return ((int[]) arr).length;
        if (arr instanceof short[]) return ((short[]) arr).length;
        if (arr instanceof double[]) return ((double[]) arr).length;
        if (arr instanceof float[]) return ((float[]) arr).length;
        if (arr instanceof boolean[]) return ((boolean[]) arr).length;
        if (arr instanceof Object[]) return ((Object[]) arr).length;
        throw new IllegalStateException("Not a supported typed array: " + arr.getClass().getName());
    }

    /// Convert a temporal value to its ISO string representation
    private static String temporalToString(byte typeTag, Object value) {
        switch (typeTag) {
            case TAG_DATE:     return ((LocalDate) value).toString();
            case TAG_TIME:     return ((LocalTime) value).toString();
            case TAG_DATETIME: return ((Instant) value).toString();
            default:
                throw new IllegalStateException("Not a temporal tag: " + typeTag);
        }
    }

    /// Validate that a string encodes to well-formed UTF-8.
    /// Java Strings are inherently valid Unicode (UTF-16), so encoding to
    /// UTF-8 and decoding back with strict error reporting catches any
    /// issues with unpaired surrogates.
    private static void validateUtf8(String value) {
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT);
        try {
            byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
            decoder.decode(java.nio.ByteBuffer.wrap(utf8));
        } catch (Exception e) {
            throw new IllegalArgumentException("text() value is not well-formed UTF-8: " + e.getMessage(), e);
        }
    }

    /// Compare two scalar values for deterministic ordering.
    /// Sorts by type tag first, then by natural ordering within the same tag.
    @SuppressWarnings({"unchecked", "rawtypes"})
    static int compareScalars(Object a, Object b) {
        byte tagA = inferTag(a);
        byte tagB = inferTag(b);
        if (tagA != tagB) return Byte.compare(tagA, tagB);
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;
        if (a instanceof Comparable) {
            return ((Comparable) a).compareTo(b);
        }
        return 0;
    }

    /// Infer the type tag for a list/set/map element from its Java type
    static byte inferTag(Object value) {
        if (value == null) return TAG_NULL;
        if (value instanceof String) return TAG_TEXT;
        if (value instanceof Long) return TAG_INT;
        if (value instanceof Integer) return TAG_INT;
        if (value instanceof Double) return TAG_FLOAT;
        if (value instanceof Float) return TAG_FLOAT;
        if (value instanceof Boolean) return TAG_BOOL;
        if (value instanceof byte[]) return TAG_BYTES;
        if (value instanceof Short) return TAG_SHORT;
        if (value instanceof BigDecimal) return TAG_DECIMAL;
        if (value instanceof BigInteger) return TAG_VARINT;
        if (value instanceof LocalDate) return TAG_DATE;
        if (value instanceof LocalTime) return TAG_TIME;
        if (value instanceof Ulid) return TAG_ULID;
        if (value instanceof List) return TAG_LIST;
        if (value instanceof MNode) return TAG_MAP;
        throw new IllegalArgumentException("Unsupported list element type: " + value.getClass().getName());
    }

    /// Normalize a value for its inferred tag (e.g. Integer to Long)
    static Object normalizeValue(byte tag, Object value) {
        if (tag == TAG_INT && value instanceof Integer) {
            return ((Integer) value).longValue();
        }
        if (tag == TAG_FLOAT && value instanceof Float) {
            return ((Float) value).doubleValue();
        }
        return value;
    }

    // ==================== Lookup internals ====================

    private int indexOf(String key) {
        for (int i = 0; i < keys.length; i++) {
            if (keys[i].equals(key)) {
                return i;
            }
        }
        return -1;
    }

    private int requireIndex(String key) {
        int idx = indexOf(key);
        if (idx < 0) {
            throw new IllegalArgumentException("MNode missing key: " + key);
        }
        if (values[idx] == null) {
            throw new IllegalArgumentException("MNode value is null for key: " + key);
        }
        return idx;
    }

    private boolean hasTyped(String key, byte expectedTag) {
        int idx = indexOf(key);
        return idx >= 0 && values[idx] != null && types[idx] == expectedTag;
    }

    private static IllegalArgumentException typeMismatch(String key, String expected, Object actual, byte actualTag) {
        String actualDesc = actual != null ? actual.getClass().getName() : "null";
        String tagName = (actualTag >= 0 && actualTag < TAG_NAMES.length) ? TAG_NAMES[actualTag] : String.valueOf(actualTag);
        return new IllegalArgumentException(
            "MNode key '" + key + "' expected " + expected + " but was " + actualDesc + " (tag=" + tagName + ")"
        );
    }

    // ==================== Object overrides ====================

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MNode)) return false;
        MNode that = (MNode) o;
        if (keys.length != that.keys.length) return false;
        for (int i = 0; i < keys.length; i++) {
            if (!keys[i].equals(that.keys[i])) return false;
            if (types[i] != that.types[i]) return false;
            if (!valuesEqual(types[i], values[i], that.values[i])) return false;
        }
        return true;
    }

    private static boolean valuesEqual(byte tag, Object a, Object b) {
        if (a == b) return true;
        if (a == null || b == null) return false;
        if (tag == TAG_BYTES) {
            return Arrays.equals((byte[]) a, (byte[]) b);
        }
        if (tag == TAG_ARRAY) {
            TypedArrayVal ta = (TypedArrayVal) a;
            TypedArrayVal tb = (TypedArrayVal) b;
            if (ta.elemTag != tb.elemTag) return false;
            return typedArrayEquals(ta.array, tb.array);
        }
        return a.equals(b);
    }

    private static boolean typedArrayEquals(Object a, Object b) {
        if (a instanceof long[]) return Arrays.equals((long[]) a, (long[]) b);
        if (a instanceof int[]) return Arrays.equals((int[]) a, (int[]) b);
        if (a instanceof short[]) return Arrays.equals((short[]) a, (short[]) b);
        if (a instanceof double[]) return Arrays.equals((double[]) a, (double[]) b);
        if (a instanceof float[]) return Arrays.equals((float[]) a, (float[]) b);
        if (a instanceof boolean[]) return Arrays.equals((boolean[]) a, (boolean[]) b);
        if (a instanceof Object[]) return Arrays.deepEquals((Object[]) a, (Object[]) b);
        return a.equals(b);
    }

    @Override
    public int hashCode() {
        int h = Arrays.hashCode(keys);
        h = 31 * h + Arrays.hashCode(types);
        for (int i = 0; i < values.length; i++) {
            h = 31 * h + valueHashCode(types[i], values[i]);
        }
        return h;
    }

    private static int valueHashCode(byte tag, Object value) {
        if (value == null) return 0;
        if (tag == TAG_BYTES) return Arrays.hashCode((byte[]) value);
        if (tag == TAG_ARRAY) {
            TypedArrayVal tav = (TypedArrayVal) value;
            return typedArrayHashCode(tav.array);
        }
        return value.hashCode();
    }

    private static int typedArrayHashCode(Object arr) {
        if (arr instanceof long[]) return Arrays.hashCode((long[]) arr);
        if (arr instanceof int[]) return Arrays.hashCode((int[]) arr);
        if (arr instanceof short[]) return Arrays.hashCode((short[]) arr);
        if (arr instanceof double[]) return Arrays.hashCode((double[]) arr);
        if (arr instanceof float[]) return Arrays.hashCode((float[]) arr);
        if (arr instanceof boolean[]) return Arrays.hashCode((boolean[]) arr);
        if (arr instanceof Object[]) return Arrays.deepHashCode((Object[]) arr);
        return arr.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("MNode{");
        for (int i = 0; i < keys.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(keys[i]).append('=');
            byte tag = types[i];
            switch (tag) {
                case TAG_BYTES:
                    sb.append("byte[").append(((byte[]) values[i]).length).append(']');
                    break;
                case TAG_ENUM_STR:
                    sb.append("enum(").append(values[i]).append(')');
                    break;
                case TAG_ENUM_ORD:
                    sb.append("enum#").append(values[i]);
                    if (enumDefs != null && enumDefs[i] != null) {
                        int ord = (Integer) values[i];
                        if (ord >= 0 && ord < enumDefs[i].length) {
                            sb.append('(').append(enumDefs[i][ord]).append(')');
                        }
                    }
                    break;
                case TAG_TEXT_VALIDATED:
                    sb.append("text(").append(values[i]).append(')');
                    break;
                case TAG_ASCII:
                    sb.append("ascii(").append(values[i]).append(')');
                    break;
                case TAG_HALF:
                    sb.append("half(").append(values[i]).append(')');
                    break;
                case TAG_FLOAT32:
                    sb.append("float32(").append(values[i]).append(')');
                    break;
                case TAG_INT32:
                    sb.append("int32(").append(values[i]).append(')');
                    break;
                case TAG_SHORT:
                    sb.append("short(").append(values[i]).append(')');
                    break;
                case TAG_MILLIS:
                    sb.append("millis(").append(values[i]).append(')');
                    break;
                case TAG_NANOS:
                    sb.append("nanos(").append(values[i]).append(')');
                    break;
                case TAG_DATETIME:
                    sb.append("datetime(").append(values[i]).append(')');
                    break;
                case TAG_UUIDV1:
                    sb.append("uuidv1(").append(values[i]).append(')');
                    break;
                case TAG_UUIDV7:
                    sb.append("uuidv7(").append(values[i]).append(')');
                    break;
                case TAG_ULID:
                    sb.append("ulid(").append(values[i]).append(')');
                    break;
                case TAG_ARRAY: {
                    TypedArrayVal tav = (TypedArrayVal) values[i];
                    sb.append("array(").append(tav.array.getClass().getComponentType().getSimpleName());
                    sb.append('[').append(arrayLength(tav.array)).append("])");
                    break;
                }
                case TAG_SET:
                    sb.append("set(").append(values[i]).append(')');
                    break;
                case TAG_TYPED_MAP:
                    sb.append("map(").append(values[i]).append(')');
                    break;
                default:
                    sb.append(values[i]);
                    break;
            }
        }
        sb.append('}');
        return sb.toString();
    }

    // ==================== Fingerprinting ====================

    /// Creates a structural fingerprint of this MNode by replacing all values
    /// with type-appropriate defaults. Two MNodes with the same fingerprint
    /// have identical schema (same field names and types in the same order).
    ///
    /// @return an MNode with the same keys and types but default zero values
    public MNode fingerprint() {
        Object[] defaults = new Object[values.length];
        for (int i = 0; i < types.length; i++) {
            defaults[i] = defaultValue(types[i]);
        }
        return new MNode(keys.clone(), types.clone(), defaults, null);
    }

    /// Returns whether this MNode has the same structural schema as another:
    /// same field names and types in the same order.
    ///
    /// @param other the other MNode
    /// @return true if the schemas match
    public boolean isCongruent(MNode other) {
        return Arrays.equals(keys, other.keys) && Arrays.equals(types, other.types);
    }

    private static Object defaultValue(byte tag) {
        switch (tag) {
            case TAG_TEXT:
            case TAG_ENUM_STR:
            case TAG_TEXT_VALIDATED:
            case TAG_ASCII:
                return "";
            case TAG_INT:
                return 0L;
            case TAG_FLOAT:
                return 0.0;
            case TAG_BOOL:
                return Boolean.FALSE;
            case TAG_BYTES:
                return new byte[0];
            case TAG_NULL:
                return null;
            case TAG_ENUM_ORD:
                return 0;
            case TAG_INT32:
                return 0;
            case TAG_SHORT:
                return (short) 0;
            case TAG_DECIMAL:
                return BigDecimal.ZERO;
            case TAG_VARINT:
                return BigInteger.ZERO;
            case TAG_FLOAT32:
                return 0.0f;
            case TAG_HALF:
                return 0.0f;
            case TAG_MILLIS:
            case TAG_NANOS:
            case TAG_DATETIME:
                return Instant.EPOCH;
            case TAG_DATE:
                return LocalDate.EPOCH;
            case TAG_TIME:
                return LocalTime.MIDNIGHT;
            case TAG_UUIDV1:
            case TAG_UUIDV7:
                return new UUID(0L, 0L);
            case TAG_ULID:
                return Ulid.of(new byte[16]);
            case TAG_LIST:
                return Collections.emptyList();
            case TAG_MAP:
                return MNode.of();
            case TAG_ARRAY:
                return new TypedArrayVal(new long[0], TAG_INT);
            case TAG_SET:
                return Collections.emptySet();
            case TAG_TYPED_MAP:
                return Collections.emptyMap();
            default:
                throw new IllegalStateException("Unknown type tag: " + tag);
        }
    }
}
