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
import java.util.ArrayList;
import java.util.List;

/// A complete slabtastic page that can be built from records or parsed from
/// an existing buffer.
///
/// Page layout (footer anchored at the end, all little-endian):
/// ```
/// [header:8][records...][gap][offsets:(N+1)*4][footer:16]
/// ```
///
/// Records are simply packed data with no known structure. The offsets fully
/// define the beginning and end of each record — there is one more offset
/// than records, to make indexing math simple for every record.
///
/// The total page size is always a multiple of {@link SlabConstants#PAGE_ALIGNMENT}
/// (512 bytes), with a minimum of 512 and a maximum of 2^32 bytes. The
/// footer occupies the last 16 bytes. The fence-post offset array
/// (N + 1 entries for N records) sits immediately before the footer. All
/// record offsets are encoded as array-structured int offsets from the
/// beginning of the page, which must take account of the 8-byte header.
/// Any remaining space between the end of the records and the start of the
/// offsets is zero-filled padding.
///
/// The page size in the header and footer must always be equal. The
/// `slab check` CLI command uses this agreement to traverse forward and
/// backward to verify record sizes without necessarily reading the pages
/// page.
public class SlabPage implements SlabConstants {

    private final long startOrdinal;
    private final byte pageType;
    private final List<byte[]> records;
    private final int pageSize;
    private final byte namespaceIndex;

    /// Builds a page from a list of records with a specified namespace index.
    ///
    /// @param startOrdinal  the first ordinal for this page
    /// @param pageType      the page type ({@link SlabConstants#PAGE_TYPE_DATA}
    ///                      or {@link SlabConstants#PAGE_TYPE_PAGES_PAGE})
    /// @param records       the record payloads
    /// @param namespaceIndex the namespace index for this page
    public SlabPage(long startOrdinal, byte pageType, List<byte[]> records, byte namespaceIndex) {
        this.startOrdinal = startOrdinal;
        this.pageType = pageType;
        this.records = List.copyOf(records);
        this.namespaceIndex = namespaceIndex;

        int totalRecordBytes = 0;
        for (byte[] r : records) {
            totalRecordBytes += r.length;
        }
        int minSize = HEADER_SIZE + totalRecordBytes
            + (records.size() + 1) * OFFSET_ENTRY_SIZE + FOOTER_V1_SIZE;
        this.pageSize = roundUp(minSize, PAGE_ALIGNMENT);
    }

    /// Builds a page from a list of records using the default namespace.
    ///
    /// @param startOrdinal the first ordinal for this page
    /// @param pageType     the page type ({@link SlabConstants#PAGE_TYPE_DATA}
    ///                     or {@link SlabConstants#PAGE_TYPE_PAGES_PAGE})
    /// @param records      the record payloads
    public SlabPage(long startOrdinal, byte pageType, List<byte[]> records) {
        this(startOrdinal, pageType, records, NAMESPACE_DEFAULT);
    }

    private SlabPage(long startOrdinal, byte pageType, List<byte[]> records, int pageSize,
                     byte namespaceIndex) {
        this.startOrdinal = startOrdinal;
        this.pageType = pageType;
        this.records = List.copyOf(records);
        this.pageSize = pageSize;
        this.namespaceIndex = namespaceIndex;
    }

    /// Parses a page from the given buffer. The buffer must contain at least
    /// 16 bytes (footer) and the header magic must be valid.
    ///
    /// @param buf a buffer positioned at the start of the page; the buffer's
    ///            byte order is ignored (bytes are read with explicit LE order)
    /// @return the parsed page
    /// @throws IllegalStateException if the page is structurally invalid
    public static SlabPage parseFrom(ByteBuffer buf) {
        PageHeader header = PageHeader.readFrom(buf, 0);
        int pageSz = header.pageSize();

        PageFooter footer = PageFooter.readFrom(buf, pageSz - FOOTER_V1_SIZE);
        footer.validate();

        if (header.pageSize() != footer.pageSize()) {
            throw new IllegalStateException(
                "Header page size %d != footer page size %d"
                    .formatted(header.pageSize(), footer.pageSize()));
        }

        int recCount = footer.recordCount();
        int offsetsStart = pageSz - FOOTER_V1_SIZE - (recCount + 1) * OFFSET_ENTRY_SIZE;

        ByteBuffer le = buf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        int[] offsets = new int[recCount + 1];
        for (int i = 0; i <= recCount; i++) {
            offsets[i] = le.getInt(offsetsStart + i * OFFSET_ENTRY_SIZE);
        }

        List<byte[]> records = new ArrayList<>(recCount);
        for (int i = 0; i < recCount; i++) {
            int start = offsets[i];
            int end = offsets[i + 1];
            byte[] data = new byte[end - start];
            buf.duplicate().position(start).get(data);
            records.add(data);
        }

        return new SlabPage(footer.startOrdinal(), footer.pageType(), records, pageSz,
            footer.namespaceIndex());
    }

    /// Serializes this page to a new little-endian {@link ByteBuffer}.
    ///
    /// @return a buffer of exactly {@link #serializedSize()} bytes
    public ByteBuffer toByteBuffer() {
        ByteBuffer buf = ByteBuffer.allocate(pageSize).order(ByteOrder.LITTLE_ENDIAN);

        // Header
        new PageHeader(pageSize).writeTo(buf, 0);

        // Records
        int pos = HEADER_SIZE;
        int[] offsets = new int[records.size() + 1];
        offsets[0] = HEADER_SIZE;
        for (int i = 0; i < records.size(); i++) {
            byte[] rec = records.get(i);
            buf.position(pos);
            buf.put(rec);
            pos += rec.length;
            offsets[i + 1] = pos;
        }

        // Offsets (fence-post), placed right before the footer
        int offsetsStart = pageSize - FOOTER_V1_SIZE - offsets.length * OFFSET_ENTRY_SIZE;
        for (int i = 0; i < offsets.length; i++) {
            buf.putInt(offsetsStart + i * OFFSET_ENTRY_SIZE, offsets[i]);
        }

        // Footer
        PageFooter footer = new PageFooter(
            startOrdinal, records.size(), pageSize,
            pageType, namespaceIndex, (short) FOOTER_V1_SIZE
        );
        footer.writeTo(buf, pageSize - FOOTER_V1_SIZE);

        buf.rewind();
        return buf;
    }

    /// Returns a read-only slice of the buffer backing the record at the
    /// given local index.
    ///
    /// @param localIndex the zero-based index within this page
    /// @return a {@link ByteBuffer} view over the record bytes
    /// @throws IndexOutOfBoundsException if the index is out of range
    public ByteBuffer getRecord(int localIndex) {
        if (localIndex < 0 || localIndex >= records.size()) {
            throw new IndexOutOfBoundsException(
                "Local index %d out of range [0, %d)".formatted(localIndex, records.size()));
        }
        return ByteBuffer.wrap(records.get(localIndex)).asReadOnlyBuffer();
    }

    /// Returns the total serialized size of this page in bytes (always a
    /// multiple of 512).
    public int serializedSize() {
        return pageSize;
    }

    /// Returns the starting ordinal for this page.
    public long startOrdinal() {
        return startOrdinal;
    }

    /// Returns the number of records in this page.
    public int recordCount() {
        return records.size();
    }

    /// Returns the page type.
    public byte pageType() {
        return pageType;
    }

    /// Returns the namespace index for this page.
    public byte namespaceIndex() {
        return namespaceIndex;
    }

    /// Returns a {@link PageFooter} matching this page's metadata.
    public PageFooter footer() {
        return new PageFooter(
            startOrdinal, records.size(), pageSize,
            pageType, namespaceIndex, (short) FOOTER_V1_SIZE
        );
    }

    private static int roundUp(int value, int alignment) {
        return ((value + alignment - 1) / alignment) * alignment;
    }
}
