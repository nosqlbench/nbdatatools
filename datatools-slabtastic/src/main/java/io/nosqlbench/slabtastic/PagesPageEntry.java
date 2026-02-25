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

/// A 16-byte entry in the pages page, mapping a starting ordinal to its
/// file offset.
///
/// Binary layout (little-endian):
/// ```
/// [start_ordinal:8][file_offset:8]
/// ```
///
/// Even though these records are fixed-size, the layout of the pages page
/// does not diverge from the layout of other pages — the offsets are encoded
/// duplicitously in format v1 (array-based indexing could suffice given the
/// uniform record size, but consistency is preferred).
///
/// Pages in the pages page are not required to be monotonically structured
/// with respect to their file offsets (aligned with the monotonic structure
/// of their starting ordinals). Pages may actually be out of order, should
/// some append-only revisions be made to existing pages.
///
/// Pages not referenced in the pages page are considered logically deleted
/// and must not be used. Any reader behavior that reads unreferenced pages
/// (outside of file maintenance) is undefined and should be considered a
/// bug.
///
/// Entries are ordered by {@code startOrdinal} to support O(log2 n)
/// binary search for ordinal lookup.
///
/// @param startOrdinal the first ordinal in the referenced data page
/// @param fileOffset   the byte offset of the referenced page within the file
public record PagesPageEntry(long startOrdinal, long fileOffset)
    implements Comparable<PagesPageEntry> {

    /// Reads a {@link PagesPageEntry} from 16 bytes at the given position.
    ///
    /// @param buf    the buffer to read from
    /// @param offset the absolute byte offset within {@code buf}
    /// @return a new {@link PagesPageEntry}
    public static PagesPageEntry readFrom(ByteBuffer buf, int offset) {
        ByteBuffer le = buf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        long ordinal = le.getLong(offset);
        long fileOff = le.getLong(offset + 8);
        return new PagesPageEntry(ordinal, fileOff);
    }

    /// Writes this entry as 16 bytes at the given position.
    ///
    /// @param buf    the buffer to write into
    /// @param offset the absolute byte offset within {@code buf}
    public void writeTo(ByteBuffer buf, int offset) {
        ByteBuffer le = buf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        le.putLong(offset, startOrdinal);
        le.putLong(offset + 8, fileOffset);
    }

    @Override
    public int compareTo(PagesPageEntry other) {
        return Long.compare(this.startOrdinal, other.startOrdinal);
    }
}
