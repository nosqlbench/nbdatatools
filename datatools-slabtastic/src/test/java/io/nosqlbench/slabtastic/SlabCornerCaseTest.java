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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

/// Corner-case tests that exercise boundary conditions, malformed files,
/// and contract violations not covered by the adversarial tests.
class SlabCornerCaseTest implements SlabConstants {

    @TempDir
    Path tempDir;

    // ── Page structure: header/footer mismatch in parseFrom ─────────

    @Test
    void parseFromRejectsHeaderFooterPageSizeMismatch() {
        // Build a valid 1024-byte page, then overwrite the header with a
        // different page size while keeping the footer untouched.
        SlabPage page = new SlabPage(0L, PAGE_TYPE_DATA, List.of("data".getBytes()));
        ByteBuffer buf = page.toByteBuffer();
        int realSize = page.serializedSize();

        // Corrupt header to claim a different page size
        ByteBuffer le = buf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        le.putInt(4, realSize * 2); // header says 2x the actual size

        assertThatThrownBy(() -> SlabPage.parseFrom(buf))
            .isInstanceOf(Exception.class);
    }

    // ── Page structure: exact 512-byte page, no gap ─────────────────

    @Test
    void pageWithNoGapBetweenRecordsAndOffsets() {
        // Calculate a payload that leaves zero gap:
        // pageSize=512, header=8, footer=16, 2 offsets=8 → payload=480
        int payloadSize = MIN_PAGE_SIZE - HEADER_SIZE - 2 * OFFSET_ENTRY_SIZE - FOOTER_V1_SIZE;
        byte[] payload = new byte[payloadSize];
        for (int i = 0; i < payload.length; i++) payload[i] = (byte) (i ^ 0xAB);

        SlabPage page = new SlabPage(0L, PAGE_TYPE_DATA, List.of(payload));
        assertThat(page.serializedSize()).isEqualTo(MIN_PAGE_SIZE);

        ByteBuffer buf = page.toByteBuffer();

        // Verify the offsets start immediately after the records:
        // records end at HEADER_SIZE + payloadSize = 8 + 480 = 488
        // offsets start at 512 - 16 - 2*4 = 488 → no gap
        ByteBuffer le = buf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        int offsetsStart = MIN_PAGE_SIZE - FOOTER_V1_SIZE - 2 * OFFSET_ENTRY_SIZE;
        assertThat(offsetsStart).isEqualTo(HEADER_SIZE + payloadSize);

        SlabPage parsed = SlabPage.parseFrom(buf);
        ByteBuffer rec = parsed.getRecord(0);
        byte[] got = new byte[rec.remaining()];
        rec.get(got);
        assertThat(got).isEqualTo(payload);
    }

    // ── Flush threshold: records landing exactly on page boundary ────

    @Test
    void recordsExactlyFillingPreferredPageSizeNoFlush() throws IOException {
        // Two records that together exactly fill a 512-byte page:
        // header(8) + rec0 + rec1 + 3*offset(12) + footer(16) = 512
        // rec0 + rec1 = 512 - 8 - 12 - 16 = 476
        int totalPayload = MIN_PAGE_SIZE - HEADER_SIZE - 3 * OFFSET_ENTRY_SIZE - FOOTER_V1_SIZE;
        int rec0Size = totalPayload / 2;
        int rec1Size = totalPayload - rec0Size;

        Path file = tempDir.resolve("exactfill.slab");
        try (SlabWriter writer = new SlabWriter(file, MIN_PAGE_SIZE)) {
            writer.write(0, new byte[rec0Size]);
            writer.write(1, new byte[rec1Size]);
        }
        try (SlabReader reader = new SlabReader(file)) {
            // Both records should be on the same page
            assertThat(reader.pageCount()).isEqualTo(1);
            assertThat(reader.recordCount()).isEqualTo(2);
        }
    }

    @Test
    void oneByteOverPreferredPageSizeTriggersFlush() throws IOException {
        // Two records that together need 1 byte more than a 512-byte page
        int totalPayload = MIN_PAGE_SIZE - HEADER_SIZE - 3 * OFFSET_ENTRY_SIZE - FOOTER_V1_SIZE + 1;
        int rec0Size = totalPayload / 2;
        int rec1Size = totalPayload - rec0Size;

        Path file = tempDir.resolve("overby1.slab");
        try (SlabWriter writer = new SlabWriter(file, MIN_PAGE_SIZE)) {
            writer.write(0, new byte[rec0Size]);
            writer.write(1, new byte[rec1Size]);
        }
        try (SlabReader reader = new SlabReader(file)) {
            // Should have flushed into 2 data pages
            assertThat(reader.pageCount()).isEqualTo(2);
            assertThat(reader.recordCount()).isEqualTo(2);
        }
    }

    // ── Writer ordinal contract: descending ─────────────────────────

    @Test
    void writerRejectsDescendingOrdinals() throws IOException {
        Path file = tempDir.resolve("descending.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(10, "ten".getBytes());
            assertThatThrownBy(() -> writer.write(5, "five".getBytes()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not strictly ascending");
        }
    }

    @Test
    void writerRejectsDescendingAcrossPages() throws IOException {
        Path file = tempDir.resolve("desc_cross.slab");
        try (SlabWriter writer = new SlabWriter(file, 512)) {
            writer.write(100, "a".getBytes());
            // Gap triggers page flush, but ordinal 50 is still descending
            assertThatThrownBy(() -> writer.write(50, "b".getBytes()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not strictly ascending");
        }
    }

    // ── Writer ordinal contract: duplicates ─────────────────────────

    @Test
    void writerRejectsDuplicateOrdinalOnSamePage() throws IOException {
        Path file = tempDir.resolve("dup_same.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(0, "first".getBytes());
            assertThatThrownBy(() -> writer.write(0, "second".getBytes()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not strictly ascending");
        }
    }

    @Test
    void writerRejectsDuplicateOrdinalAcrossPages() throws IOException {
        Path file = tempDir.resolve("dup_cross.slab");
        try (SlabWriter writer = new SlabWriter(file, 512)) {
            writer.write(0, new byte[400]); // fills page
            writer.write(1, new byte[10]);  // new page
            assertThatThrownBy(() -> writer.write(1, "dup".getBytes()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not strictly ascending");
        }
    }

    // ── Writer ordinal contract: out of range ───────────────────────

    @Test
    void writerRejectsOrdinalAboveMax() throws IOException {
        Path file = tempDir.resolve("over_max.slab");
        try (SlabWriter writer = new SlabWriter(file, 512)) {
            assertThatThrownBy(() -> writer.write(MAX_ORDINAL + 1, "x".getBytes()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("outside the 5-byte signed range");
        }
    }

    @Test
    void writerRejectsOrdinalBelowMin() throws IOException {
        Path file = tempDir.resolve("under_min.slab");
        try (SlabWriter writer = new SlabWriter(file, 512)) {
            assertThatThrownBy(() -> writer.write(MIN_ORDINAL - 1, "x".getBytes()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("outside the 5-byte signed range");
        }
    }

    @Test
    void writerAcceptsExactBoundaryOrdinals() throws IOException {
        Path file = tempDir.resolve("exact_bounds.slab");
        try (SlabWriter writer = new SlabWriter(file, 512)) {
            writer.write(MIN_ORDINAL, "min".getBytes());
            writer.write(MAX_ORDINAL, "max".getBytes());
        }
        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.get(MIN_ORDINAL)).isPresent();
            assertThat(reader.get(MAX_ORDINAL)).isPresent();
        }
    }

    // ── Writer: pages page with zero entries ────────────────────────

    @Test
    void writerWithNoRecordsProducesValidEmptyFile() throws IOException {
        Path file = tempDir.resolve("empty_writer.slab");
        try (SlabWriter writer = new SlabWriter(file, 512)) {
            // write nothing
        }
        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.pageCount()).isZero();
            assertThat(reader.recordCount()).isZero();
            assertThat(reader.get(0)).isEmpty();
            assertThat(reader.get(Long.MIN_VALUE)).isEmpty();
            assertThat(reader.get(Long.MAX_VALUE)).isEmpty();
        }
    }

    // ── Reader: pages page entry pointing past EOF ──────────────────

    @Test
    void readerRejectsPagesPageEntryBeyondEOF() throws IOException {
        Path file = tempDir.resolve("entry_past_eof.slab");

        // Build a pages page with one entry pointing to offset 999999
        PagesPageEntry badEntry = new PagesPageEntry(0L, 999999L);
        ByteBuffer entryBuf = ByteBuffer.allocate(PAGES_PAGE_RECORD_SIZE);
        badEntry.writeTo(entryBuf, 0);

        SlabPage pagesPage = new SlabPage(0L, PAGE_TYPE_PAGES_PAGE, List.of(entryBuf.array()));
        ByteBuffer buf = pagesPage.toByteBuffer();

        try (FileChannel ch = FileChannel.open(file,
            StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            buf.rewind();
            ch.write(buf);
        }

        // Reader construction tries to read header at offset 999999 which is past EOF
        assertThatThrownBy(() -> new SlabReader(file))
            .isInstanceOf(Exception.class);
    }

    // ── Reader: pages page footer pageSize exceeds file size ────────

    @Test
    void readerRejectsPagesPageSizeLargerThanFile() throws IOException {
        Path file = tempDir.resolve("pagespage_too_big.slab");

        // Write a 512-byte buffer where the footer claims pageSize=1024
        ByteBuffer buf = ByteBuffer.allocate(512).order(ByteOrder.LITTLE_ENDIAN);
        new PageHeader(1024).writeTo(buf, 0);
        new PageFooter(0L, 0, 1024, PAGE_TYPE_PAGES_PAGE, NAMESPACE_DEFAULT, (short) FOOTER_V1_SIZE)
            .writeTo(buf, 512 - FOOTER_V1_SIZE);

        try (FileChannel ch = FileChannel.open(file,
            StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            buf.rewind();
            ch.write(buf);
        }

        // fileSize=512, pagesPageSize=1024, pagesPageOffset=512-1024=-512 → negative
        assertThatThrownBy(() -> new SlabReader(file))
            .isInstanceOf(Exception.class);
    }

    // ── Reader: pages page with overlapping page regions ────────────

    @Test
    void readerHandlesOverlappingPageRegions() throws IOException {
        // Write a valid file first, then craft a pages page that has two entries
        // pointing to the same file offset (overlapping regions)
        Path file = tempDir.resolve("overlap.slab");

        // Write one data page at offset 0
        SlabPage dataPage = new SlabPage(0L, PAGE_TYPE_DATA, List.of("hello".getBytes()));
        ByteBuffer dataBuf = dataPage.toByteBuffer();
        int dataSize = dataPage.serializedSize();

        // Build pages page with two entries both pointing to offset 0
        // but claiming different ordinals
        PagesPageEntry entry0 = new PagesPageEntry(0L, 0L);
        PagesPageEntry entry1 = new PagesPageEntry(100L, 0L); // same offset!
        ByteBuffer e0Buf = ByteBuffer.allocate(PAGES_PAGE_RECORD_SIZE);
        entry0.writeTo(e0Buf, 0);
        ByteBuffer e1Buf = ByteBuffer.allocate(PAGES_PAGE_RECORD_SIZE);
        entry1.writeTo(e1Buf, 0);

        SlabPage pagesPage = new SlabPage(0L, PAGE_TYPE_PAGES_PAGE,
            List.of(e0Buf.array(), e1Buf.array()));
        ByteBuffer pagesBuf = pagesPage.toByteBuffer();

        try (FileChannel ch = FileChannel.open(file,
            StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            dataBuf.rewind();
            ch.write(dataBuf);
            pagesBuf.rewind();
            ch.write(pagesBuf);
        }

        // Reader should open successfully (overlapping offsets aren't
        // structurally invalid). The pages page is authoritative per the spec,
        // so entry1 claims startOrdinal=100 at offset 0 with recordCount=1.
        // Ordinal 100 → localIndex 0 → returns the data at that page.
        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.pageCount()).isEqualTo(2);
            assertThat(reader.get(0)).isPresent();
            // ordinal 100 maps to localIndex 0 in the page at offset 0
            // because the pages page is the authoritative index
            assertThat(reader.get(100)).isPresent();
            // ordinal 101 would be localIndex 1 which is >= recordCount=1
            assertThat(reader.get(101)).isEmpty();
        }
    }

    // ── Reader: duplicate start ordinals in pages page ──────────────

    @Test
    void readerHandlesDuplicateStartOrdinals() throws IOException {
        // Two pages page entries with the same startOrdinal but different offsets
        Path file = tempDir.resolve("dup_ordinals.slab");

        SlabPage page0 = new SlabPage(0L, PAGE_TYPE_DATA, List.of("first".getBytes()));
        SlabPage page1 = new SlabPage(0L, PAGE_TYPE_DATA, List.of("second".getBytes()));
        ByteBuffer buf0 = page0.toByteBuffer();
        ByteBuffer buf1 = page1.toByteBuffer();
        int off0 = 0;
        int off1 = page0.serializedSize();

        PagesPageEntry entry0 = new PagesPageEntry(0L, off0);
        PagesPageEntry entry1 = new PagesPageEntry(0L, off1);
        ByteBuffer e0 = ByteBuffer.allocate(PAGES_PAGE_RECORD_SIZE);
        entry0.writeTo(e0, 0);
        ByteBuffer e1 = ByteBuffer.allocate(PAGES_PAGE_RECORD_SIZE);
        entry1.writeTo(e1, 0);

        SlabPage pagesPage = new SlabPage(0L, PAGE_TYPE_PAGES_PAGE,
            List.of(e0.array(), e1.array()));
        ByteBuffer pagesBuf = pagesPage.toByteBuffer();

        try (FileChannel ch = FileChannel.open(file,
            StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            buf0.rewind();
            ch.write(buf0);
            buf1.rewind();
            ch.write(buf1);
            pagesBuf.rewind();
            ch.write(pagesBuf);
        }

        // Reader opens. Binary search for ordinal 0 returns one of the two
        // pages deterministically. The important thing is it doesn't crash.
        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.pageCount()).isEqualTo(2);
            Optional<ByteBuffer> result = reader.get(0);
            assertThat(result).isPresent();
            byte[] bytes = new byte[result.get().remaining()];
            result.get().get(bytes);
            // Should return data from one of the two pages
            assertThat(new String(bytes)).isIn("first", "second");
        }
    }

    // ── Reader: get() on a closed reader ────────────────────────────

    @Test
    void getOnClosedReaderThrows() throws IOException {
        Path file = tempDir.resolve("closed_get.slab");
        try (SlabWriter writer = new SlabWriter(file, 512)) {
            writer.write(0, "test".getBytes());
        }

        SlabReader reader = new SlabReader(file);
        reader.close();

        assertThatThrownBy(() -> reader.get(0))
            .isInstanceOf(Exception.class);
    }

    // ── Reader: close() idempotency ─────────────────────────────────

    @Test
    void readerCloseIsIdempotent() throws IOException {
        Path file = tempDir.resolve("reader_close2.slab");
        try (SlabWriter writer = new SlabWriter(file, 512)) {
            writer.write(0, "test".getBytes());
        }

        SlabReader reader = new SlabReader(file);
        reader.close();
        assertThatCode(reader::close).doesNotThrowAnyException();
    }

    // ── Writer: close() idempotency ─────────────────────────────────

    @Test
    void writerCloseIsIdempotent() throws IOException {
        Path file = tempDir.resolve("writer_close2.slab");
        SlabWriter writer = new SlabWriter(file, 512);
        writer.write(0, "test".getBytes());
        writer.close();

        // Second close should not throw or write a second pages page
        assertThatCode(writer::close).doesNotThrowAnyException();

        // File should still be valid
        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.recordCount()).isEqualTo(1);
            assertRecordEquals(reader, 0, "test");
        }
    }

    @Test
    void writerWriteAfterCloseThrows() throws IOException {
        Path file = tempDir.resolve("write_after_close.slab");
        SlabWriter writer = new SlabWriter(file, 512);
        writer.close();

        assertThatThrownBy(() -> writer.write(0, "x".getBytes()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("closed");
    }

    // ── PageFooter: footerLength != 16 ──────────────────────────────

    @Test
    void footerValidateRejectsBadFooterLength() {
        PageFooter footer = new PageFooter(0L, 1, 512, PAGE_TYPE_DATA, NAMESPACE_DEFAULT, (short) 32);
        assertThatThrownBy(footer::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Footer length");
    }

    @Test
    void footerValidateRejectsZeroFooterLength() {
        PageFooter footer = new PageFooter(0L, 1, 512, PAGE_TYPE_DATA, NAMESPACE_DEFAULT, (short) 0);
        assertThatThrownBy(footer::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Footer length");
    }

    // ── PageFooter: ordinal sign-extension neighbors ────────────────

    @Test
    void ordinalNeighborsOfSignExtensionBoundary() {
        // Just below the sign bit: 2^39 - 2 (positive)
        roundTripOrdinal(MAX_ORDINAL - 1);
        // Just above the sign bit: -2^39 + 1 (negative)
        roundTripOrdinal(MIN_ORDINAL + 1);
        // The transition points: where bit 39 flips
        roundTripOrdinal((1L << 38) - 1);   // 0x3FFFFFFFFF → positive
        roundTripOrdinal(1L << 38);          // 0x4000000000 → still positive
        roundTripOrdinal(-(1L << 38));       // negative
        roundTripOrdinal(-(1L << 38) - 1);   // more negative
        roundTripOrdinal(-1L);               // all-ones in 5 bytes
        roundTripOrdinal(1L);
        roundTripOrdinal(0L);
    }

    private void roundTripOrdinal(long ordinal) {
        PageFooter footer = new PageFooter(ordinal, 1, 512, PAGE_TYPE_DATA, NAMESPACE_DEFAULT, (short) FOOTER_V1_SIZE);
        ByteBuffer buf = ByteBuffer.allocate(16);
        footer.writeTo(buf, 0);
        PageFooter read = PageFooter.readFrom(buf, 0);
        assertThat(read.startOrdinal()).as("ordinal %d", ordinal).isEqualTo(ordinal);
    }

    // ── SlabPage.parseFrom: footer with huge recordCount ────────────

    @Test
    void parseFromRejectsNegativeOffsetsStart() {
        // A footer claiming a recordCount so large that the offsetsStart
        // calculation goes negative
        ByteBuffer buf = ByteBuffer.allocate(512).order(ByteOrder.LITTLE_ENDIAN);
        new PageHeader(512).writeTo(buf, 0);
        // 100 records → (100+1)*4 = 404 bytes of offsets
        // offsetsStart = 512 - 16 - 404 = 92 (valid, but the data won't be meaningful)
        // Use MAX_RECORD_COUNT instead → (16777215+1)*4 = way more than 512
        new PageFooter(0L, MAX_RECORD_COUNT, 512, PAGE_TYPE_DATA, NAMESPACE_DEFAULT, (short) FOOTER_V1_SIZE)
            .writeTo(buf, 512 - FOOTER_V1_SIZE);

        assertThatThrownBy(() -> SlabPage.parseFrom(buf))
            .isInstanceOf(Exception.class);
    }

    // ── File with pages page whose pageSize field exceeds file ──────

    @Test
    void pagesPagePageSizeExceedsFileButFooterAtEOF() throws IOException {
        // The footer is at the very end of the file, but the pageSize in the
        // footer is larger than the file, so pagesPageOffset goes negative
        Path file = tempDir.resolve("pagespage_oversize.slab");

        ByteBuffer buf = ByteBuffer.allocate(512).order(ByteOrder.LITTLE_ENDIAN);
        new PageHeader(512).writeTo(buf, 0);
        // Footer says pageSize=4096 but file is only 512 bytes
        new PageFooter(0L, 0, 4096, PAGE_TYPE_PAGES_PAGE, NAMESPACE_DEFAULT, (short) FOOTER_V1_SIZE)
            .writeTo(buf, 512 - FOOTER_V1_SIZE);

        try (FileChannel ch = FileChannel.open(file,
            StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            buf.rewind();
            ch.write(buf);
        }

        // pagesPageOffset = 512 - 4096 = -3584 → readBytes at negative position
        assertThatThrownBy(() -> new SlabReader(file))
            .isInstanceOf(Exception.class);
    }

    // ── PagesPageEntry: natural ordering ────────────────────────────

    @Test
    void pagesPageEntriesSortCorrectly() {
        PagesPageEntry a = new PagesPageEntry(-10L, 0L);
        PagesPageEntry b = new PagesPageEntry(0L, 100L);
        PagesPageEntry c = new PagesPageEntry(5L, 200L);

        List<PagesPageEntry> entries = new java.util.ArrayList<>(List.of(c, a, b));
        java.util.Collections.sort(entries);

        assertThat(entries).containsExactly(a, b, c);
    }

    // ── End-to-end: single record exactly at MIN_PAGE_SIZE ──────────

    @Test
    void singleRecordAtMinPageSizeViaWriterReader() throws IOException {
        // Payload that, with overhead, exactly fills 512 bytes
        int payload = MIN_PAGE_SIZE - HEADER_SIZE - 2 * OFFSET_ENTRY_SIZE - FOOTER_V1_SIZE;
        byte[] data = new byte[payload];
        for (int i = 0; i < data.length; i++) data[i] = (byte) 0xCC;

        Path file = tempDir.resolve("min_page_e2e.slab");
        try (SlabWriter writer = new SlabWriter(file, MIN_PAGE_SIZE)) {
            writer.write(0, data);
        }

        // Data page is exactly 512 bytes, pages page is also 512 bytes
        // Total file = 1024 bytes
        assertThat(Files.size(file)).isEqualTo(1024);

        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.pageCount()).isEqualTo(1);
            Optional<ByteBuffer> r = reader.get(0);
            assertThat(r).isPresent();
            byte[] got = new byte[r.get().remaining()];
            r.get().get(got);
            assertThat(got).isEqualTo(data);
        }
    }

    // ── Reader: fileSize() on closed reader ─────────────────────────

    @Test
    void fileSizeOnClosedReaderThrows() throws IOException {
        Path file = tempDir.resolve("closed_size.slab");
        try (SlabWriter writer = new SlabWriter(file, 512)) {
            writer.write(0, "x".getBytes());
        }
        SlabReader reader = new SlabReader(file);
        reader.close();

        assertThatThrownBy(reader::fileSize)
            .isInstanceOf(Exception.class);
    }

    // ── Writer: many ascending ordinals with large gaps ──────────────

    @Test
    void manyPagesFromLargeGaps() throws IOException {
        Path file = tempDir.resolve("large_gaps.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            for (int i = 0; i < 100; i++) {
                writer.write(i * 1_000_000L, ("gap-" + i).getBytes());
            }
        }
        try (SlabReader reader = new SlabReader(file)) {
            // Each ordinal is non-contiguous, so 100 pages
            assertThat(reader.pageCount()).isEqualTo(100);
            assertThat(reader.recordCount()).isEqualTo(100);

            assertRecordEquals(reader, 0, "gap-0");
            assertRecordEquals(reader, 50_000_000L, "gap-50");
            assertRecordEquals(reader, 99_000_000L, "gap-99");

            // In between should be empty
            assertThat(reader.get(1L)).isEmpty();
            assertThat(reader.get(999_999L)).isEmpty();
        }
    }

    private void assertRecordEquals(SlabReader reader, long ordinal, String expected) {
        Optional<ByteBuffer> result = reader.get(ordinal);
        assertThat(result).as("ordinal %d", ordinal).isPresent();
        byte[] bytes = new byte[result.get().remaining()];
        result.get().get(bytes);
        assertThat(new String(bytes)).as("ordinal %d value", ordinal).isEqualTo(expected);
    }
}
