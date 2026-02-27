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

/// Field types for metadata records in predicate test data.
///
/// Each type has a wire tag used for binary serialization in
/// {@link FieldDescriptor} and {@link MetadataRecordCodec}.
public enum FieldType {

    /// UTF-8 text field
    TEXT((byte) 0),
    /// 64-bit signed integer field
    INT((byte) 1),
    /// 64-bit IEEE 754 floating-point field
    FLOAT((byte) 2),
    /// Boolean field (1 byte: 0 = false, 1 = true)
    BOOL((byte) 3),
    /// Enumerated value field with a fixed set of string values
    ENUM((byte) 4);

    private final byte wireTag;

    FieldType(byte wireTag) {
        this.wireTag = wireTag;
    }

    /// Returns the wire tag byte used for binary serialization.
    ///
    /// @return the wire tag
    public byte wireTag() {
        return wireTag;
    }

    /// Resolves a {@link FieldType} from its wire tag byte.
    ///
    /// @param tag the wire tag
    /// @return the corresponding field type
    /// @throws IllegalArgumentException if the tag is not recognized
    public static FieldType fromWireTag(byte tag) {
        for (FieldType ft : values()) {
            if (ft.wireTag == tag) {
                return ft;
            }
        }
        throw new IllegalArgumentException("Unknown FieldType wire tag: " + tag);
    }
}
