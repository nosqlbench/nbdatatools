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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/// Concrete implementation of {@link MetadataLayout} backed by an
/// ordered list of {@link FieldDescriptor} instances.
///
/// Supports binary serialization via {@link #encode()} and
/// {@link #fromBuffer(ByteBuffer)} for storage in slabtastic or
/// SQLite backends.
///
/// ## Wire format
///
/// ```
/// [fieldCount:2][field0:...][field1:...]...
/// ```
///
/// Each field is encoded by {@link FieldDescriptor#encode()}.
public class MetadataLayoutImpl implements MetadataLayout {

    private final List<FieldDescriptor> fields;
    private final Map<String, Integer> nameIndex;

    /// Creates a layout from an ordered list of field descriptors.
    ///
    /// @param fields the field descriptors
    public MetadataLayoutImpl(List<FieldDescriptor> fields) {
        this.fields = List.copyOf(fields);
        Map<String, Integer> idx = new LinkedHashMap<>();
        for (int i = 0; i < this.fields.size(); i++) {
            idx.put(this.fields.get(i).name(), i);
        }
        this.nameIndex = Collections.unmodifiableMap(idx);
    }

    @Override
    public List<FieldDescriptor> getFields() {
        return fields;
    }

    @Override
    public int getFieldCount() {
        return fields.size();
    }

    @Override
    public FieldDescriptor getField(int index) {
        return fields.get(index);
    }

    @Override
    public Optional<FieldDescriptor> getFieldByName(String name) {
        Integer idx = nameIndex.get(name);
        return idx != null ? Optional.of(fields.get(idx)) : Optional.empty();
    }

    /// Returns the zero-based index for a field name.
    ///
    /// @param name the field name
    /// @return the field index, or -1 if not found
    public int indexOfField(String name) {
        Integer idx = nameIndex.get(name);
        return idx != null ? idx : -1;
    }

    /// Encodes this layout into a byte array.
    ///
    /// @return the encoded bytes
    public byte[] encode() {
        List<byte[]> encodedFields = new ArrayList<>(fields.size());
        int totalSize = 2; // fieldCount
        for (FieldDescriptor fd : fields) {
            byte[] encoded = fd.encode();
            encodedFields.add(encoded);
            totalSize += encoded.length;
        }
        ByteBuffer buf = ByteBuffer.allocate(totalSize).order(ByteOrder.LITTLE_ENDIAN);
        buf.putShort((short) fields.size());
        for (byte[] encoded : encodedFields) {
            buf.put(encoded);
        }
        return buf.array();
    }

    /// Decodes a {@link MetadataLayoutImpl} from the current position
    /// of a buffer.
    ///
    /// @param buf the buffer to read from
    /// @return the decoded layout
    public static MetadataLayoutImpl fromBuffer(ByteBuffer buf) {
        buf.order(ByteOrder.LITTLE_ENDIAN);
        int fieldCount = Short.toUnsignedInt(buf.getShort());
        List<FieldDescriptor> fields = new ArrayList<>(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            fields.add(FieldDescriptor.fromBuffer(buf));
        }
        return new MetadataLayoutImpl(fields);
    }
}
