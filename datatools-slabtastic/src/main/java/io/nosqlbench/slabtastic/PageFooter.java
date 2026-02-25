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

/// A 16-byte page footer for the slabtastic format.
///
/// Binary layout (all little-endian):
/// ```
/// [start_ordinal:5][record_count:3][page_size:4][page_type:1][namespace_index:1][footer_length:2]
/// ```
///
/// The start ordinal is a 5-byte signed twos-complement integer supporting
/// the range [−2^39, 2^39 − 1]. The record count is a 3-byte unsigned
/// integer supporting up to 2^24 − 1 records per page.
///
/// The namespace index (byte 13) identifies which namespace this page belongs
/// to. This byte was formerly called "version" in the v1 format. Existing
/// files with version=1 are transparently read as namespace_index=1 (the
/// default namespace). Namespace index 0 is reserved and invalid. Page types
/// subsume the role previously played by version — all format evolution is
/// expressed through page types.
///
/// Footers are required to always be at least 16 bytes, and are padded out
/// to the nearest 16 bytes in length. Checksums are deferred to a future
/// version.
///
/// You can always read the last 16 bytes of a page to know where to find
/// the start of the footer, the start of the array-structured offset data,
/// and the start of the page. You can always read the last 16 bytes of the
/// file to do the same for the pages page.
///
/// The beginning of the record offsets starts before the footer. The first
/// element location is determined by the number of records: from the end of
/// the page, back up the footer length, then `-(4 * (record_count + 1))`.
/// All record offsets are encoded as array-structured int offsets from the
/// beginning of the page, which must take account of the page header (8
/// bytes).
///
/// The footer format is page-specific, since files may later contain pages
/// with an updated format. Namespace index 0 is invalid. Readers must verify
/// that the namespace index is non-zero before reading the page.
///
/// Page type values: 0 = invalid, 1 = pages page, 2 = data page,
/// 3 = namespaces page.
///
/// @param startOrdinal  the first ordinal in this page
/// @param recordCount   the number of records in this page
/// @param pageSize      the total page size in bytes (must be a multiple of 512)
/// @param pageType      the page type (see {@link SlabConstants})
/// @param namespaceIndex the namespace index for this page (see {@link SlabConstants})
/// @param footerLength  the footer length in bytes (always 16 for v1)
public record PageFooter(
    long startOrdinal,
    int recordCount,
    int pageSize,
    byte pageType,
    byte namespaceIndex,
    short footerLength
) implements SlabConstants {

    /// Reads a {@link PageFooter} from 16 bytes at the given absolute position.
    ///
    /// @param buf    the buffer to read from (byte order is ignored; bytes are
    ///               read individually)
    /// @param offset the absolute byte offset within {@code buf}
    /// @return a new {@link PageFooter}
    public static PageFooter readFrom(ByteBuffer buf, int offset) {
        // 5-byte signed ordinal (LE) — MSB not masked for sign extension
        long ordinal = (buf.get(offset) & 0xFFL)
            | ((buf.get(offset + 1) & 0xFFL) << 8)
            | ((buf.get(offset + 2) & 0xFFL) << 16)
            | ((buf.get(offset + 3) & 0xFFL) << 24)
            | ((long) buf.get(offset + 4) << 32);

        // 3-byte unsigned record count (LE)
        int recCount = (buf.get(offset + 5) & 0xFF)
            | ((buf.get(offset + 6) & 0xFF) << 8)
            | ((buf.get(offset + 7) & 0xFF) << 16);

        // Remaining fields via a LE-ordered duplicate
        ByteBuffer le = buf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        int pageSz = le.getInt(offset + 8);
        byte type = le.get(offset + 12);
        byte nsIdx = le.get(offset + 13);
        short footLen = le.getShort(offset + 14);

        return new PageFooter(ordinal, recCount, pageSz, type, nsIdx, footLen);
    }

    /// Writes this footer as 16 bytes at the given absolute position.
    ///
    /// @param buf    the buffer to write into
    /// @param offset the absolute byte offset within {@code buf}
    public void writeTo(ByteBuffer buf, int offset) {
        // 5-byte signed ordinal (LE)
        buf.put(offset, (byte) startOrdinal);
        buf.put(offset + 1, (byte) (startOrdinal >> 8));
        buf.put(offset + 2, (byte) (startOrdinal >> 16));
        buf.put(offset + 3, (byte) (startOrdinal >> 24));
        buf.put(offset + 4, (byte) (startOrdinal >> 32));

        // 3-byte unsigned record count (LE)
        buf.put(offset + 5, (byte) recordCount);
        buf.put(offset + 6, (byte) (recordCount >> 8));
        buf.put(offset + 7, (byte) (recordCount >> 16));

        // Remaining fields via LE-ordered duplicate
        ByteBuffer le = buf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        le.putInt(offset + 8, pageSize);
        le.put(offset + 12, pageType);
        le.put(offset + 13, namespaceIndex);
        le.putShort(offset + 14, footerLength);
    }

    /// Validates that this footer has a valid namespace index, a valid page
    /// type, an acceptable page size, and a correct footer length.
    ///
    /// @throws IllegalStateException if any field is invalid
    public void validate() {
        if (namespaceIndex == NAMESPACE_INVALID) {
            throw new IllegalStateException("Invalid namespace index: 0 (reserved)");
        }
        if (pageType == PAGE_TYPE_INVALID) {
            throw new IllegalStateException("Invalid page type: 0");
        }
        if (pageType != PAGE_TYPE_PAGES_PAGE && pageType != PAGE_TYPE_DATA
            && pageType != PAGE_TYPE_NAMESPACES_PAGE) {
            throw new IllegalStateException("Unrecognized page type: " + pageType);
        }
        if (pageSize < MIN_PAGE_SIZE) {
            throw new IllegalStateException(
                "Page size " + pageSize + " is below minimum " + MIN_PAGE_SIZE);
        }
        if (pageSize % PAGE_ALIGNMENT != 0) {
            throw new IllegalStateException(
                "Page size " + pageSize + " is not a multiple of " + PAGE_ALIGNMENT);
        }
        if (footerLength != FOOTER_V1_SIZE) {
            throw new IllegalStateException(
                "Footer length " + footerLength + " does not match v1 size " + FOOTER_V1_SIZE);
        }
    }

    /// Returns the namespace index.
    ///
    /// @deprecated Use {@link #namespaceIndex()} instead. This method exists
    ///             for source compatibility during the version→namespace transition.
    /// @return the namespace index (formerly called version)
    @Deprecated
    public byte version() {
        return namespaceIndex;
    }
}
