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

/// An 8-byte page header for the slabtastic format.
///
/// The first 4 bytes of every page have the UTF-8 encoding of "SLAB". This
/// identifies both the file and every page within it. The next 4 bytes are
/// the page length, which serves as a forward reference to the page footer
/// (or to determine when a page footer would be fully written by streaming
/// writes). This initial 8 bytes should always be considered when doing
/// data layout.
///
/// Binary layout (little-endian):
/// ```
/// [magic:4][page_size:4]
/// ```
///
/// The magic value is {@link SlabConstants#MAGIC} (`0x42414C53` LE, spelling
/// "SLAB" in file order). The page size is the total page size in bytes
/// including header, records, offsets, footer, and any alignment padding.
///
/// The page size in the header and footer must always be equal. The
/// `slab check` CLI command uses this to traverse forward and backward to
/// verify record sizes without necessarily reading the pages page.
///
/// Concurrent readers streaming a slabtastic file must not assume atomic
/// writes, and should use `[magic][size]` to determine when a page is
/// valid for reading based on the incremental file size.
///
/// @param pageSize the total page size in bytes
public record PageHeader(int pageSize) implements SlabConstants {

    /// Reads a {@link PageHeader} from 8 bytes at the given absolute position.
    ///
    /// @param buf    the buffer to read from
    /// @param offset the absolute byte offset within {@code buf}
    /// @return a new {@link PageHeader}
    /// @throws IllegalStateException if the magic bytes are incorrect
    public static PageHeader readFrom(ByteBuffer buf, int offset) {
        ByteBuffer le = buf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        int magic = le.getInt(offset);
        if (magic != MAGIC) {
            throw new IllegalStateException(
                "Bad magic: expected 0x%08X but got 0x%08X".formatted(MAGIC, magic));
        }
        int size = le.getInt(offset + 4);
        return new PageHeader(size);
    }

    /// Writes this header as 8 bytes at the given absolute position.
    ///
    /// @param buf    the buffer to write into
    /// @param offset the absolute byte offset within {@code buf}
    public void writeTo(ByteBuffer buf, int offset) {
        ByteBuffer le = buf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        le.putInt(offset, MAGIC);
        le.putInt(offset + 4, pageSize);
    }
}
