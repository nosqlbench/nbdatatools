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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/// Describes a single field in a metadata record.
///
/// Each descriptor carries the field name, its {@link FieldType}, and for
/// {@link FieldType#ENUM ENUM} fields, the list of allowed string values.
///
/// ## Wire format
///
/// ```
/// [type:1][nameLen:2][nameBytes:N]
/// ```
///
/// For ENUM fields, the name bytes are followed by:
///
/// ```
/// [enumCount:2][len0:2][bytes0:M]...[lenK:2][bytesK:M]
/// ```
public final class FieldDescriptor {

    private final String name;
    private final FieldType type;
    private final List<String> enumValues;

    /// Creates a field descriptor.
    ///
    /// @param name       the field name
    /// @param type       the field type
    /// @param enumValues the allowed enum values (empty for non-ENUM fields)
    public FieldDescriptor(String name, FieldType type, List<String> enumValues) {
        this.name = name;
        this.type = type;
        this.enumValues = List.copyOf(enumValues);
    }

    /// Creates a non-ENUM field descriptor.
    ///
    /// @param name the field name
    /// @param type the field type (must not be {@link FieldType#ENUM})
    public FieldDescriptor(String name, FieldType type) {
        this(name, type, List.of());
    }

    /// Returns the field name.
    ///
    /// @return the field name
    public String name() {
        return name;
    }

    /// Returns the field type.
    ///
    /// @return the field type
    public FieldType type() {
        return type;
    }

    /// Returns the allowed enum values.
    ///
    /// @return the enum values, or an empty list for non-ENUM fields
    public List<String> enumValues() {
        return enumValues;
    }

    /// Encodes this descriptor into a byte array.
    ///
    /// @return the encoded bytes
    public byte[] encode() {
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        int size = 1 + 2 + nameBytes.length;
        if (type == FieldType.ENUM) {
            size += 2; // enumCount
            for (String ev : enumValues) {
                size += 2 + ev.getBytes(StandardCharsets.UTF_8).length;
            }
        }
        ByteBuffer buf = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        buf.put(type.wireTag());
        buf.putShort((short) nameBytes.length);
        buf.put(nameBytes);
        if (type == FieldType.ENUM) {
            buf.putShort((short) enumValues.size());
            for (String ev : enumValues) {
                byte[] evBytes = ev.getBytes(StandardCharsets.UTF_8);
                buf.putShort((short) evBytes.length);
                buf.put(evBytes);
            }
        }
        return buf.array();
    }

    /// Decodes a {@link FieldDescriptor} from the current position of a buffer.
    ///
    /// The buffer position is advanced past the consumed bytes.
    ///
    /// @param buf the buffer to read from
    /// @return the decoded field descriptor
    public static FieldDescriptor fromBuffer(ByteBuffer buf) {
        buf.order(ByteOrder.LITTLE_ENDIAN);
        FieldType ft = FieldType.fromWireTag(buf.get());
        int nameLen = Short.toUnsignedInt(buf.getShort());
        byte[] nameBytes = new byte[nameLen];
        buf.get(nameBytes);
        String name = new String(nameBytes, StandardCharsets.UTF_8);

        List<String> enumVals;
        if (ft == FieldType.ENUM) {
            int enumCount = Short.toUnsignedInt(buf.getShort());
            enumVals = new ArrayList<>(enumCount);
            for (int i = 0; i < enumCount; i++) {
                int evLen = Short.toUnsignedInt(buf.getShort());
                byte[] evBytes = new byte[evLen];
                buf.get(evBytes);
                enumVals.add(new String(evBytes, StandardCharsets.UTF_8));
            }
            enumVals = Collections.unmodifiableList(enumVals);
        } else {
            enumVals = List.of();
        }
        return new FieldDescriptor(name, ft, enumVals);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FieldDescriptor)) return false;
        FieldDescriptor that = (FieldDescriptor) o;
        return Objects.equals(name, that.name) && type == that.type
               && Objects.equals(enumValues, that.enumValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, enumValues);
    }

    @Override
    public String toString() {
        return String.format("FieldDescriptor{name='%s', type=%s, enumValues=%s}", name, type, enumValues);
    }
}
