package io.nosqlbench.vectordata.discovery.metadata;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Codec for encoding and decoding metadata records against a
/// {@link MetadataLayout}.
///
/// ## Wire format per field
///
/// ```
/// [fieldIndex:2][fieldType:1][valueBytes...]
/// ```
///
/// Value encoding by type:
/// - **TEXT**: `[len:4][utf8:N]`
/// - **INT**: `[val:8]` (little-endian long)
/// - **FLOAT**: `[val:8]` (little-endian double)
/// - **BOOL**: `[val:1]` (0 = false, 1 = true)
/// - **ENUM**: `[ordinal:4]` (little-endian int, index into enum values list)
public final class MetadataRecordCodec {

    private MetadataRecordCodec() {}

    /// Encodes a metadata record as a byte array.
    ///
    /// Keys in the record map must match field names in the layout.
    /// Missing fields are omitted from the output.
    ///
    /// @param layout the metadata layout describing the schema
    /// @param record the field values keyed by name
    /// @return the encoded bytes
    /// @throws IllegalArgumentException if a record key is not in the layout
    ///                                  or a value type does not match
    public static byte[] encode(MetadataLayout layout, Map<String, Object> record) {
        List<FieldDescriptor> fields = layout.getFields();

        // First pass: compute size
        int size = 0;
        for (Map.Entry<String, Object> entry : record.entrySet()) {
            String name = entry.getKey();
            Object value = entry.getValue();
            if (value == null) continue;

            int fieldIdx = findFieldIndex(layout, name);
            FieldDescriptor fd = fields.get(fieldIdx);
            size += 2 + 1; // fieldIndex + fieldType
            size += valueSize(fd, value);
        }

        ByteBuffer buf = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);

        for (Map.Entry<String, Object> entry : record.entrySet()) {
            String name = entry.getKey();
            Object value = entry.getValue();
            if (value == null) continue;

            int fieldIdx = findFieldIndex(layout, name);
            FieldDescriptor fd = fields.get(fieldIdx);

            buf.putShort((short) fieldIdx);
            buf.put(fd.type().wireTag());
            encodeValue(buf, fd, value);
        }

        return buf.array();
    }

    /// Decodes a metadata record from a buffer.
    ///
    /// @param layout the metadata layout describing the schema
    /// @param buf    the buffer to read from (read from current position to limit)
    /// @return the decoded record keyed by field name
    public static Map<String, Object> decode(MetadataLayout layout, ByteBuffer buf) {
        buf.order(ByteOrder.LITTLE_ENDIAN);
        Map<String, Object> record = new LinkedHashMap<>();

        while (buf.hasRemaining()) {
            int fieldIdx = Short.toUnsignedInt(buf.getShort());
            byte typeTag = buf.get();
            FieldType ft = FieldType.fromWireTag(typeTag);
            FieldDescriptor fd = layout.getField(fieldIdx);

            Object value = decodeValue(buf, fd, ft);
            record.put(fd.name(), value);
        }

        return record;
    }

    private static int findFieldIndex(MetadataLayout layout, String name) {
        List<FieldDescriptor> fields = layout.getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).name().equals(name)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Field not in layout: " + name);
    }

    private static int valueSize(FieldDescriptor fd, Object value) {
        switch (fd.type()) {
            case TEXT:
                return 4 + ((String) value).getBytes(StandardCharsets.UTF_8).length;
            case INT:
                return 8;
            case FLOAT:
                return 8;
            case BOOL:
                return 1;
            case ENUM:
                return 4;
            default:
                throw new IllegalArgumentException("Unknown field type: " + fd.type());
        }
    }

    private static void encodeValue(ByteBuffer buf, FieldDescriptor fd, Object value) {
        switch (fd.type()) {
            case TEXT: {
                byte[] textBytes = ((String) value).getBytes(StandardCharsets.UTF_8);
                buf.putInt(textBytes.length);
                buf.put(textBytes);
                break;
            }
            case INT: {
                buf.putLong(((Number) value).longValue());
                break;
            }
            case FLOAT: {
                buf.putDouble(((Number) value).doubleValue());
                break;
            }
            case BOOL: {
                buf.put((byte) (Boolean.TRUE.equals(value) ? 1 : 0));
                break;
            }
            case ENUM: {
                String enumVal = (String) value;
                int ordinal = fd.enumValues().indexOf(enumVal);
                if (ordinal < 0) {
                    throw new IllegalArgumentException(
                        String.format("Enum value '%s' not in allowed values %s for field '%s'",
                            enumVal, fd.enumValues(), fd.name()));
                }
                buf.putInt(ordinal);
                break;
            }
        }
    }

    private static Object decodeValue(ByteBuffer buf, FieldDescriptor fd, FieldType ft) {
        switch (ft) {
            case TEXT: {
                int len = buf.getInt();
                byte[] textBytes = new byte[len];
                buf.get(textBytes);
                return new String(textBytes, StandardCharsets.UTF_8);
            }
            case INT: {
                return buf.getLong();
            }
            case FLOAT: {
                return buf.getDouble();
            }
            case BOOL: {
                return buf.get() != 0;
            }
            case ENUM: {
                int ordinal = buf.getInt();
                return fd.enumValues().get(ordinal);
            }
            default:
                throw new IllegalArgumentException("Unknown field type: " + ft);
        }
    }
}
