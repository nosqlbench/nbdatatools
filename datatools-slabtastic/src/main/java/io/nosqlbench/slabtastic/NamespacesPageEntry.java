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

package io.nosqlbench.slabtastic;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/// An entry in the namespaces page, mapping a namespace to its pages-page
/// offset within the file.
///
/// Binary layout (little-endian):
/// ```
/// [namespace_index:1][name_length:1][name_bytes:N][pages_page_offset:8]
/// ```
///
/// The namespace index identifies this namespace in page footers. The name
/// is a human-readable UTF-8 label (up to {@link SlabConstants#NAMESPACE_MAX_NAME_LENGTH}
/// bytes). The pages-page offset points to the pages page for this namespace
/// within the file.
///
/// @param namespaceIndex  the namespace index (1-127, 0 is reserved)
/// @param name            the human-readable namespace name
/// @param pagesPageOffset the byte offset of this namespace's pages page
public record NamespacesPageEntry(
    byte namespaceIndex,
    String name,
    long pagesPageOffset
) implements SlabConstants {

    /// Reads a {@link NamespacesPageEntry} from the given buffer at the
    /// specified offset.
    ///
    /// @param buf    the buffer to read from
    /// @param offset the absolute byte offset within {@code buf}
    /// @return a new {@link NamespacesPageEntry}
    public static NamespacesPageEntry readFrom(ByteBuffer buf, int offset) {
        ByteBuffer le = buf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        byte nsIdx = le.get(offset);
        int nameLen = le.get(offset + 1) & 0xFF;
        byte[] nameBytes = new byte[nameLen];
        for (int i = 0; i < nameLen; i++) {
            nameBytes[i] = le.get(offset + 2 + i);
        }
        String name = new String(nameBytes, StandardCharsets.UTF_8);
        long ppOffset = le.getLong(offset + 2 + nameLen);
        return new NamespacesPageEntry(nsIdx, name, ppOffset);
    }

    /// Writes this entry at the given position in the buffer.
    ///
    /// @param buf    the buffer to write into
    /// @param offset the absolute byte offset within {@code buf}
    public void writeTo(ByteBuffer buf, int offset) {
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        ByteBuffer le = buf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        le.put(offset, namespaceIndex);
        le.put(offset + 1, (byte) nameBytes.length);
        for (int i = 0; i < nameBytes.length; i++) {
            le.put(offset + 2 + i, nameBytes[i]);
        }
        le.putLong(offset + 2 + nameBytes.length, pagesPageOffset);
    }

    /// Returns the total serialized size of this entry in bytes.
    ///
    /// @return 1 (index) + 1 (name length) + name bytes + 8 (offset)
    public int serializedSize() {
        return 1 + 1 + name.getBytes(StandardCharsets.UTF_8).length + 8;
    }

    /// Validates that this entry has a valid namespace index and name length.
    ///
    /// @throws IllegalStateException if any field is invalid
    public void validate() {
        if (namespaceIndex == NAMESPACE_INVALID) {
            throw new IllegalStateException("Invalid namespace index: 0 (reserved)");
        }
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        if (nameBytes.length > NAMESPACE_MAX_NAME_LENGTH) {
            throw new IllegalStateException(
                "Namespace name too long: " + nameBytes.length
                    + " bytes (max " + NAMESPACE_MAX_NAME_LENGTH + ")");
        }
    }
}
