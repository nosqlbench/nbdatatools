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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/// Stateless codec for formatting and parsing MNode values in CDDL text notation.
///
/// ## Format output example
///
/// ```
/// {
///   name: tstr = "glove-100",
///   dims: int = 128,
///   config: node = { engine: tstr = "hnsw" }
/// }
/// ```
///
/// ## Parser grammar
///
/// ```
/// node       := '{' field (',' field)* ','? '}'
/// field      := IDENT ':' type_spec '=' value
/// type_spec  := IDENT | '[' IDENT ']'
/// value      := string_lit | number_lit | bool_lit | null_lit
///             | list_lit | set_lit | node_lit | bytes_lit
/// ```
///
/// Type names resolve via {@link MNode#resolveTag(String)} — accepts all aliases.
public final class MNodeCddlCodec {

    private MNodeCddlCodec() {}

    // ==================== Formatting ====================

    /// Format an MNode as CDDL text notation.
    /// @param node the node to format
    /// @return the CDDL text representation
    public static String format(MNode node) {
        StringBuilder sb = new StringBuilder();
        formatNode(sb, node, 0);
        return sb.toString();
    }

    private static void formatNode(StringBuilder sb, MNode node, int indent) {
        String[] keys = node.keys();
        byte[] types = node.types();
        Object[] values = node.values();

        sb.append("{\n");
        for (int i = 0; i < keys.length; i++) {
            appendIndent(sb, indent + 1);
            sb.append(keys[i]).append(": ");
            appendTypeAndValue(sb, types[i], values[i], indent + 1);
            if (i < keys.length - 1) {
                sb.append(',');
            }
            sb.append('\n');
        }
        appendIndent(sb, indent);
        sb.append('}');
    }

    private static void appendTypeAndValue(StringBuilder sb, byte tag, Object value, int indent) {
        if (tag == MNode.TAG_ARRAY) {
            MNode.TypedArrayVal tav = (MNode.TypedArrayVal) value;
            sb.append('[').append(MNode.cddlName(tav.elemTag)).append("] = ");
            formatTypedArray(sb, tav);
        } else if (tag == MNode.TAG_MAP) {
            sb.append("node = ");
            formatNode(sb, (MNode) value, indent);
        } else {
            sb.append(MNode.cddlName(tag)).append(" = ");
            formatValue(sb, tag, value, indent);
        }
    }

    @SuppressWarnings("unchecked")
    private static void formatValue(StringBuilder sb, byte tag, Object value, int indent) {
        if (value == null) {
            sb.append("null");
            return;
        }
        switch (tag) {
            case MNode.TAG_TEXT:
            case MNode.TAG_TEXT_VALIDATED:
            case MNode.TAG_ASCII:
            case MNode.TAG_ENUM_STR:
                sb.append('"').append(escapeString((String) value)).append('"');
                break;
            case MNode.TAG_INT:
                sb.append(value);
                break;
            case MNode.TAG_INT32:
                sb.append(value);
                break;
            case MNode.TAG_SHORT:
                sb.append(value);
                break;
            case MNode.TAG_ENUM_ORD:
                sb.append(value);
                break;
            case MNode.TAG_FLOAT:
                sb.append(value);
                break;
            case MNode.TAG_FLOAT32:
                sb.append(value).append('f');
                break;
            case MNode.TAG_HALF:
                sb.append(value).append('h');
                break;
            case MNode.TAG_BOOL:
                sb.append(value);
                break;
            case MNode.TAG_BYTES:
                sb.append("h'");
                for (byte b : (byte[]) value) {
                    sb.append(String.format("%02x", b & 0xFF));
                }
                sb.append('\'');
                break;
            case MNode.TAG_NULL:
                sb.append("null");
                break;
            case MNode.TAG_DECIMAL:
                sb.append("decimal(\"").append(value).append("\")");
                break;
            case MNode.TAG_VARINT:
                sb.append("varint(\"").append(value).append("\")");
                break;
            case MNode.TAG_MILLIS:
                sb.append("millis(\"").append(value).append("\")");
                break;
            case MNode.TAG_NANOS:
                sb.append("nanos(\"").append(value).append("\")");
                break;
            case MNode.TAG_DATE:
            case MNode.TAG_TIME:
            case MNode.TAG_DATETIME:
                sb.append('"').append(value).append('"');
                break;
            case MNode.TAG_UUIDV1:
            case MNode.TAG_UUIDV7:
                sb.append('"').append(value).append('"');
                break;
            case MNode.TAG_ULID:
                sb.append('"').append(value).append('"');
                break;
            case MNode.TAG_LIST: {
                List<?> list = (List<?>) value;
                sb.append('[');
                for (int i = 0; i < list.size(); i++) {
                    if (i > 0) sb.append(", ");
                    Object elem = list.get(i);
                    byte elemTag = MNode.inferTag(elem);
                    Object normalized = MNode.normalizeValue(elemTag, elem);
                    formatValue(sb, elemTag, normalized, indent);
                }
                sb.append(']');
                break;
            }
            case MNode.TAG_SET: {
                Set<?> set = (Set<?>) value;
                sb.append("set [");
                boolean first = true;
                for (Object elem : set) {
                    if (!first) sb.append(", ");
                    first = false;
                    byte elemTag = MNode.inferTag(elem);
                    Object normalized = MNode.normalizeValue(elemTag, elem);
                    formatValue(sb, elemTag, normalized, indent);
                }
                sb.append(']');
                break;
            }
            case MNode.TAG_MAP: {
                formatNode(sb, (MNode) value, indent);
                break;
            }
            case MNode.TAG_TYPED_MAP: {
                Map<?, ?> map = (Map<?, ?>) value;
                sb.append("{ ");
                boolean first = true;
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    if (!first) sb.append(", ");
                    first = false;
                    byte kTag = MNode.inferTag(entry.getKey());
                    Object kNorm = MNode.normalizeValue(kTag, entry.getKey());
                    formatValue(sb, kTag, kNorm, indent);
                    sb.append(" => ");
                    byte vTag = MNode.inferTag(entry.getValue());
                    Object vNorm = MNode.normalizeValue(vTag, entry.getValue());
                    formatValue(sb, vTag, vNorm, indent);
                }
                sb.append(" }");
                break;
            }
            default:
                sb.append(value);
        }
    }

    private static void formatTypedArray(StringBuilder sb, MNode.TypedArrayVal tav) {
        Object arr = tav.array;
        sb.append('[');
        if (arr instanceof long[]) {
            long[] a = (long[]) arr;
            for (int i = 0; i < a.length; i++) { if (i > 0) sb.append(", "); sb.append(a[i]); }
        } else if (arr instanceof int[]) {
            int[] a = (int[]) arr;
            for (int i = 0; i < a.length; i++) { if (i > 0) sb.append(", "); sb.append(a[i]); }
        } else if (arr instanceof short[]) {
            short[] a = (short[]) arr;
            for (int i = 0; i < a.length; i++) { if (i > 0) sb.append(", "); sb.append(a[i]); }
        } else if (arr instanceof double[]) {
            double[] a = (double[]) arr;
            for (int i = 0; i < a.length; i++) { if (i > 0) sb.append(", "); sb.append(a[i]); }
        } else if (arr instanceof float[]) {
            float[] a = (float[]) arr;
            for (int i = 0; i < a.length; i++) { if (i > 0) sb.append(", "); sb.append(a[i]).append('f'); }
        } else if (arr instanceof boolean[]) {
            boolean[] a = (boolean[]) arr;
            for (int i = 0; i < a.length; i++) { if (i > 0) sb.append(", "); sb.append(a[i]); }
        } else if (arr instanceof Half[]) {
            Half[] a = (Half[]) arr;
            for (int i = 0; i < a.length; i++) { if (i > 0) sb.append(", "); sb.append(a[i].toFloat()).append('h'); }
        } else if (arr instanceof Instant[]) {
            Instant[] a = (Instant[]) arr;
            String wrapper = tav.elemTag == MNode.TAG_MILLIS ? "millis" : "nanos";
            for (int i = 0; i < a.length; i++) {
                if (i > 0) sb.append(", ");
                sb.append(wrapper).append("(\"").append(a[i]).append("\")");
            }
        } else if (arr instanceof UUID[]) {
            UUID[] a = (UUID[]) arr;
            for (int i = 0; i < a.length; i++) { if (i > 0) sb.append(", "); sb.append('"').append(a[i]).append('"'); }
        } else if (arr instanceof Ulid[]) {
            Ulid[] a = (Ulid[]) arr;
            for (int i = 0; i < a.length; i++) { if (i > 0) sb.append(", "); sb.append('"').append(a[i]).append('"'); }
        }
        sb.append(']');
    }

    private static String escapeString(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':  sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:   sb.append(c);
            }
        }
        return sb.toString();
    }

    private static void appendIndent(StringBuilder sb, int level) {
        for (int i = 0; i < level; i++) sb.append("  ");
    }

    // ==================== Parsing ====================

    /// Parse a CDDL text representation into an MNode.
    /// @param text the CDDL text
    /// @return the parsed MNode
    /// @throws IllegalArgumentException if the text cannot be parsed
    public static MNode parse(String text) {
        Parser p = new Parser(text.trim());
        MNode result = p.parseNode();
        p.skipWhitespace();
        if (p.pos < p.input.length()) {
            throw new IllegalArgumentException("Unexpected content after closing '}' at position " + p.pos);
        }
        return result;
    }

    private static final class Parser {
        final String input;
        int pos;

        Parser(String input) {
            this.input = input;
            this.pos = 0;
        }

        MNode parseNode() {
            expect('{');
            skipWhitespace();
            List<Object> kvs = new ArrayList<>();
            while (pos < input.length() && peek() != '}') {
                String key = parseIdent();
                skipWhitespace();
                expect(':');
                skipWhitespace();
                String typeName = parseTypeSpec();
                boolean isArrayType = typeName.startsWith("[") && typeName.endsWith("]");
                skipWhitespace();
                expect('=');
                skipWhitespace();

                byte tag;
                if (isArrayType) {
                    String elemTypeName = typeName.substring(1, typeName.length() - 1);
                    byte elemTag = MNode.resolveTag(elemTypeName);
                    Object arrayVal = parseTypedArrayValue(elemTag);
                    kvs.add(key);
                    kvs.add(arrayVal);
                } else if (typeName.equals("node")) {
                    MNode nested = parseNode();
                    kvs.add(key);
                    kvs.add(nested);
                } else {
                    tag = MNode.resolveTag(typeName);
                    Object value = parseValue(tag);
                    kvs.add(key);
                    kvs.add(wrapValue(tag, value));
                }
                skipWhitespace();
                if (pos < input.length() && peek() == ',') {
                    advance();
                    skipWhitespace();
                }
            }
            expect('}');
            return MNode.of(kvs.toArray());
        }

        /// Wrap a parsed raw value into the appropriate MNode marker
        private Object wrapValue(byte tag, Object value) {
            switch (tag) {
                case MNode.TAG_TEXT:
                case MNode.TAG_TEXT_VALIDATED:
                    return MNode.text((String) value);
                case MNode.TAG_ASCII:
                    return MNode.ascii((String) value);
                case MNode.TAG_INT:
                    if (value instanceof Long) return value;
                    return ((Number) value).longValue();
                case MNode.TAG_INT32:
                    return MNode.int32(((Number) value).intValue());
                case MNode.TAG_SHORT:
                    return MNode.int16(((Number) value).shortValue());
                case MNode.TAG_FLOAT:
                    if (value instanceof Double) return value;
                    return ((Number) value).doubleValue();
                case MNode.TAG_FLOAT32:
                    return MNode.float32(((Number) value).floatValue());
                case MNode.TAG_HALF:
                    return MNode.half(((Number) value).floatValue());
                case MNode.TAG_BOOL:
                    return value;
                case MNode.TAG_BYTES:
                    return value;
                case MNode.TAG_NULL:
                    return null;
                case MNode.TAG_ENUM_STR:
                    return MNode.enumVal((String) value);
                case MNode.TAG_ENUM_ORD:
                    return MNode.enumOrd(((Number) value).intValue());
                case MNode.TAG_DECIMAL:
                    return value; // BigDecimal
                case MNode.TAG_VARINT:
                    return value; // BigInteger
                case MNode.TAG_MILLIS:
                    return MNode.millis((Instant) value);
                case MNode.TAG_NANOS:
                    return MNode.nanos((Instant) value);
                case MNode.TAG_DATE:
                    return value; // LocalDate
                case MNode.TAG_TIME:
                    return value; // LocalTime
                case MNode.TAG_DATETIME:
                    return MNode.datetime((Instant) value);
                case MNode.TAG_UUIDV1:
                    return MNode.uuidV1((UUID) value);
                case MNode.TAG_UUIDV7:
                    return MNode.uuidV7((UUID) value);
                case MNode.TAG_ULID:
                    return value; // Ulid
                case MNode.TAG_LIST:
                    return value; // List
                case MNode.TAG_SET:
                    return value; // Set
                case MNode.TAG_TYPED_MAP:
                    return MNode.typedMap((Map<Object, Object>) value);
                default:
                    return value;
            }
        }

        @SuppressWarnings("unchecked")
        private Object parseValue(byte tag) {
            switch (tag) {
                case MNode.TAG_TEXT:
                case MNode.TAG_TEXT_VALIDATED:
                case MNode.TAG_ASCII:
                case MNode.TAG_ENUM_STR:
                    return parseStringLiteral();
                case MNode.TAG_INT:
                case MNode.TAG_INT32:
                case MNode.TAG_SHORT:
                case MNode.TAG_ENUM_ORD:
                    return parseNumberLiteral();
                case MNode.TAG_FLOAT:
                    return parseNumberLiteral();
                case MNode.TAG_FLOAT32:
                    return parseFloatSuffixed('f');
                case MNode.TAG_HALF:
                    return parseFloatSuffixed('h');
                case MNode.TAG_BOOL:
                    return parseBoolLiteral();
                case MNode.TAG_BYTES:
                    return parseBytesLiteral();
                case MNode.TAG_NULL:
                    return parseNullLiteral();
                case MNode.TAG_DECIMAL:
                    return parseWrappedString("decimal", s -> new BigDecimal(s));
                case MNode.TAG_VARINT:
                    return parseWrappedString("varint", s -> new BigInteger(s));
                case MNode.TAG_MILLIS:
                    return parseWrappedString("millis", Instant::parse);
                case MNode.TAG_NANOS:
                    return parseWrappedString("nanos", Instant::parse);
                case MNode.TAG_DATE:
                    return parseStringLiteralAs(LocalDate::parse);
                case MNode.TAG_TIME:
                    return parseStringLiteralAs(LocalTime::parse);
                case MNode.TAG_DATETIME:
                    return parseStringLiteralAs(Instant::parse);
                case MNode.TAG_UUIDV1:
                case MNode.TAG_UUIDV7:
                    return parseStringLiteralAs(UUID::fromString);
                case MNode.TAG_ULID:
                    return parseStringLiteralAs(Ulid::of);
                case MNode.TAG_LIST:
                    return parseListLiteral();
                case MNode.TAG_SET:
                    return parseSetLiteral();
                case MNode.TAG_TYPED_MAP:
                    return parseTypedMapLiteral();
                default:
                    throw new IllegalArgumentException("Cannot parse value for tag: " + tag);
            }
        }

        private Object parseTypedArrayValue(byte elemTag) {
            expect('[');
            skipWhitespace();
            List<Object> elements = new ArrayList<>();
            while (pos < input.length() && peek() != ']') {
                elements.add(parseValue(elemTag));
                skipWhitespace();
                if (pos < input.length() && peek() == ',') {
                    advance();
                    skipWhitespace();
                }
            }
            expect(']');
            return toTypedArrayMarker(elemTag, elements);
        }

        private Object toTypedArrayMarker(byte elemTag, List<Object> elements) {
            switch (elemTag) {
                case MNode.TAG_INT: {
                    long[] arr = new long[elements.size()];
                    for (int i = 0; i < arr.length; i++) arr[i] = ((Number) elements.get(i)).longValue();
                    return MNode.array(arr);
                }
                case MNode.TAG_INT32: {
                    int[] arr = new int[elements.size()];
                    for (int i = 0; i < arr.length; i++) arr[i] = ((Number) elements.get(i)).intValue();
                    return MNode.array(arr);
                }
                case MNode.TAG_SHORT: {
                    short[] arr = new short[elements.size()];
                    for (int i = 0; i < arr.length; i++) arr[i] = ((Number) elements.get(i)).shortValue();
                    return MNode.array(arr);
                }
                case MNode.TAG_FLOAT: {
                    double[] arr = new double[elements.size()];
                    for (int i = 0; i < arr.length; i++) arr[i] = ((Number) elements.get(i)).doubleValue();
                    return MNode.array(arr);
                }
                case MNode.TAG_FLOAT32: {
                    float[] arr = new float[elements.size()];
                    for (int i = 0; i < arr.length; i++) arr[i] = ((Number) elements.get(i)).floatValue();
                    return MNode.array(arr);
                }
                case MNode.TAG_BOOL: {
                    boolean[] arr = new boolean[elements.size()];
                    for (int i = 0; i < arr.length; i++) arr[i] = (Boolean) elements.get(i);
                    return MNode.array(arr);
                }
                case MNode.TAG_HALF: {
                    Half[] arr = new Half[elements.size()];
                    for (int i = 0; i < arr.length; i++) arr[i] = Half.of(((Number) elements.get(i)).floatValue());
                    return MNode.array(arr);
                }
                case MNode.TAG_MILLIS: {
                    Instant[] arr = new Instant[elements.size()];
                    for (int i = 0; i < arr.length; i++) arr[i] = (Instant) elements.get(i);
                    return MNode.arrayMillis(arr);
                }
                case MNode.TAG_NANOS: {
                    Instant[] arr = new Instant[elements.size()];
                    for (int i = 0; i < arr.length; i++) arr[i] = (Instant) elements.get(i);
                    return MNode.arrayNanos(arr);
                }
                case MNode.TAG_ENUM_ORD: {
                    int[] arr = new int[elements.size()];
                    for (int i = 0; i < arr.length; i++) arr[i] = ((Number) elements.get(i)).intValue();
                    return MNode.arrayEnumOrd(arr);
                }
                case MNode.TAG_UUIDV1: {
                    UUID[] arr = new UUID[elements.size()];
                    for (int i = 0; i < arr.length; i++) arr[i] = (UUID) elements.get(i);
                    return MNode.arrayUuidV1(arr);
                }
                case MNode.TAG_UUIDV7: {
                    UUID[] arr = new UUID[elements.size()];
                    for (int i = 0; i < arr.length; i++) arr[i] = (UUID) elements.get(i);
                    return MNode.arrayUuidV7(arr);
                }
                case MNode.TAG_ULID: {
                    Ulid[] arr = new Ulid[elements.size()];
                    for (int i = 0; i < arr.length; i++) arr[i] = (Ulid) elements.get(i);
                    return MNode.array(arr);
                }
                default:
                    throw new IllegalArgumentException("Unsupported typed array element tag: " + elemTag);
            }
        }

        private String parseStringLiteral() {
            expect('"');
            StringBuilder sb = new StringBuilder();
            while (pos < input.length() && peek() != '"') {
                char c = advance();
                if (c == '\\') {
                    char esc = advance();
                    switch (esc) {
                        case '"':  sb.append('"'); break;
                        case '\\': sb.append('\\'); break;
                        case 'n':  sb.append('\n'); break;
                        case 'r':  sb.append('\r'); break;
                        case 't':  sb.append('\t'); break;
                        default:   sb.append('\\').append(esc);
                    }
                } else {
                    sb.append(c);
                }
            }
            expect('"');
            return sb.toString();
        }

        private <T> T parseStringLiteralAs(java.util.function.Function<String, T> converter) {
            String s = parseStringLiteral();
            return converter.apply(s);
        }

        private <T> T parseWrappedString(String prefix, java.util.function.Function<String, T> converter) {
            expectWord(prefix);
            expect('(');
            String s = parseStringLiteral();
            expect(')');
            return converter.apply(s);
        }

        private Number parseNumberLiteral() {
            int start = pos;
            if (pos < input.length() && (peek() == '-' || peek() == '+')) advance();
            while (pos < input.length() && (Character.isDigit(peek()) || peek() == '.')) advance();
            // check for E notation
            if (pos < input.length() && (peek() == 'e' || peek() == 'E')) {
                advance();
                if (pos < input.length() && (peek() == '-' || peek() == '+')) advance();
                while (pos < input.length() && Character.isDigit(peek())) advance();
            }
            String numStr = input.substring(start, pos);
            if (numStr.contains(".") || numStr.contains("e") || numStr.contains("E")) {
                return Double.parseDouble(numStr);
            }
            long val = Long.parseLong(numStr);
            if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) {
                return (int) val;
            }
            return val;
        }

        private Number parseFloatSuffixed(char suffix) {
            int start = pos;
            if (pos < input.length() && (peek() == '-' || peek() == '+')) advance();
            while (pos < input.length() && (Character.isDigit(peek()) || peek() == '.')) advance();
            if (pos < input.length() && (peek() == 'e' || peek() == 'E')) {
                advance();
                if (pos < input.length() && (peek() == '-' || peek() == '+')) advance();
                while (pos < input.length() && Character.isDigit(peek())) advance();
            }
            // consume the suffix
            if (pos < input.length() && Character.toLowerCase(peek()) == Character.toLowerCase(suffix)) {
                advance();
            }
            String numStr = input.substring(start, pos);
            // strip suffix if present at end
            if (numStr.length() > 0 && Character.toLowerCase(numStr.charAt(numStr.length() - 1)) == Character.toLowerCase(suffix)) {
                numStr = numStr.substring(0, numStr.length() - 1);
            }
            return Float.parseFloat(numStr);
        }

        private boolean parseBoolLiteral() {
            if (matchWord("true")) return true;
            if (matchWord("false")) return false;
            throw parseError("Expected 'true' or 'false'");
        }

        private Object parseNullLiteral() {
            expectWord("null");
            return null;
        }

        private byte[] parseBytesLiteral() {
            expect('h');
            expect('\'');
            StringBuilder hex = new StringBuilder();
            while (pos < input.length() && peek() != '\'') {
                char c = advance();
                if (!Character.isWhitespace(c)) hex.append(c);
            }
            expect('\'');
            String hexStr = hex.toString();
            if (hexStr.length() % 2 != 0) {
                throw parseError("Hex string must have even length");
            }
            byte[] bytes = new byte[hexStr.length() / 2];
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = (byte) Integer.parseInt(hexStr.substring(i * 2, i * 2 + 2), 16);
            }
            return bytes;
        }

        private List<Object> parseListLiteral() {
            expect('[');
            skipWhitespace();
            List<Object> list = new ArrayList<>();
            while (pos < input.length() && peek() != ']') {
                list.add(parseAnyValue());
                skipWhitespace();
                if (pos < input.length() && peek() == ',') {
                    advance();
                    skipWhitespace();
                }
            }
            expect(']');
            return list;
        }

        private Set<Object> parseSetLiteral() {
            expectWord("set");
            skipWhitespace();
            expect('[');
            skipWhitespace();
            Set<Object> set = new LinkedHashSet<>();
            while (pos < input.length() && peek() != ']') {
                set.add(parseAnyValue());
                skipWhitespace();
                if (pos < input.length() && peek() == ',') {
                    advance();
                    skipWhitespace();
                }
            }
            expect(']');
            return set;
        }

        @SuppressWarnings("unchecked")
        private Map<Object, Object> parseTypedMapLiteral() {
            expect('{');
            skipWhitespace();
            Map<Object, Object> map = new LinkedHashMap<>();
            while (pos < input.length() && peek() != '}') {
                Object key = parseAnyValue();
                skipWhitespace();
                expect('=');
                expect('>');
                skipWhitespace();
                Object val = parseAnyValue();
                map.put(key, val);
                skipWhitespace();
                if (pos < input.length() && peek() == ',') {
                    advance();
                    skipWhitespace();
                }
            }
            expect('}');
            return map;
        }

        /// Parse a value without a type hint — infers from syntax
        private Object parseAnyValue() {
            skipWhitespace();
            char c = peek();
            if (c == '"') {
                return parseStringLiteral();
            } else if (c == 't' && lookAhead("true")) {
                return parseBoolLiteral();
            } else if (c == 'f' && lookAhead("false")) {
                return parseBoolLiteral();
            } else if (c == 'n' && lookAhead("null")) {
                parseNullLiteral();
                return null;
            } else if (c == '[') {
                return parseListLiteral();
            } else if (c == '{') {
                return parseTypedMapLiteral();
            } else if (c == '-' || c == '+' || Character.isDigit(c)) {
                Number n = parseNumberLiteral();
                // check for float suffix
                if (pos < input.length() && peek() == 'f') {
                    advance();
                    return n.floatValue();
                }
                if (pos < input.length() && peek() == 'h') {
                    advance();
                    return n.floatValue();
                }
                if (n instanceof Integer) return ((Integer) n).longValue();
                return n;
            }
            throw parseError("Unexpected character: '" + c + "'");
        }

        private String parseIdent() {
            int start = pos;
            while (pos < input.length() && (Character.isLetterOrDigit(peek()) || peek() == '_' || peek() == '-')) {
                advance();
            }
            if (pos == start) throw parseError("Expected identifier");
            return input.substring(start, pos);
        }

        private String parseTypeSpec() {
            if (pos < input.length() && peek() == '[') {
                advance();
                skipWhitespace();
                String inner = parseIdent();
                skipWhitespace();
                expect(']');
                return "[" + inner + "]";
            }
            return parseIdent();
        }

        // ---- low-level ----

        void skipWhitespace() {
            while (pos < input.length() && Character.isWhitespace(input.charAt(pos))) pos++;
        }

        char peek() {
            return input.charAt(pos);
        }

        char advance() {
            return input.charAt(pos++);
        }

        void expect(char c) {
            skipWhitespace();
            if (pos >= input.length() || peek() != c) {
                throw parseError("Expected '" + c + "'");
            }
            advance();
        }

        void expectWord(String word) {
            for (int i = 0; i < word.length(); i++) {
                if (pos >= input.length() || peek() != word.charAt(i)) {
                    throw parseError("Expected '" + word + "'");
                }
                advance();
            }
        }

        boolean matchWord(String word) {
            if (pos + word.length() > input.length()) return false;
            for (int i = 0; i < word.length(); i++) {
                if (input.charAt(pos + i) != word.charAt(i)) return false;
            }
            // ensure word boundary
            if (pos + word.length() < input.length() && Character.isLetterOrDigit(input.charAt(pos + word.length()))) {
                return false;
            }
            pos += word.length();
            return true;
        }

        boolean lookAhead(String word) {
            if (pos + word.length() > input.length()) return false;
            for (int i = 0; i < word.length(); i++) {
                if (input.charAt(pos + i) != word.charAt(i)) return false;
            }
            // ensure word boundary
            return pos + word.length() >= input.length() || !Character.isLetterOrDigit(input.charAt(pos + word.length()));
        }

        IllegalArgumentException parseError(String msg) {
            int contextStart = Math.max(0, pos - 20);
            int contextEnd = Math.min(input.length(), pos + 20);
            String context = input.substring(contextStart, contextEnd);
            return new IllegalArgumentException(msg + " at position " + pos + " near: ..." + context + "...");
        }
    }
}
