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

/// Adversarial and boundary-condition tests for the slabtastic format.
///
/// These tests probe edge cases, corruption scenarios, and format limits
/// that well-behaved writers would never produce but that readers must
/// handle gracefully.
class SlabAdversarialTest implements SlabConstants {

    @TempDir
    Path tempDir;

    // ── Corrupted magic bytes ────────────────────────────────────────

    @Test
    void corruptedMagicInDataPageIsRejected() {
        ByteBuffer buf = ByteBuffer.allocate(512).order(ByteOrder.LITTLE_ENDIAN);
        // Write garbage magic, valid page size
        buf.putInt(0, 0xDEADBEEF);
        buf.putInt(4, 512);

        assertThatThrownBy(() -> SlabPage.parseFrom(buf))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Bad magic");
    }

    @Test
    void partialMagicCorruptionDetected() {
        // Start with a valid page, then corrupt one magic byte
        SlabPage page = new SlabPage(0L, PAGE_TYPE_DATA, List.of("test".getBytes()));
        ByteBuffer buf = page.toByteBuffer();
        buf.put(2, (byte) 0xFF); // corrupt 3rd byte of magic

        assertThatThrownBy(() -> SlabPage.parseFrom(buf))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Bad magic");
    }

    // ── File truncation ─────────────────────────────────────────────

    @Test
    void truncatedFileAt1ByteIsRejected() throws IOException {
        Path file = tempDir.resolve("trunc1.slab");
        Files.write(file, new byte[1]);

        assertThatThrownBy(() -> new SlabReader(file))
            .isInstanceOf(IOException.class)
            .hasMessageContaining("too small");
    }

    @Test
    void truncatedFileAt15BytesIsRejected() throws IOException {
        Path file = tempDir.resolve("trunc15.slab");
        Files.write(file, new byte[15]);

        assertThatThrownBy(() -> new SlabReader(file))
            .isInstanceOf(IOException.class)
            .hasMessageContaining("too small");
    }

    @Test
    void truncatedFileAtExactly16BytesWithGarbageIsRejected() throws IOException {
        Path file = tempDir.resolve("trunc16.slab");
        Files.write(file, new byte[16]);

        // 16 zero bytes → namespace_index=0 which is invalid
        assertThatThrownBy(() -> new SlabReader(file))
            .hasMessageContaining("namespace index");
    }

    // ── Zero-length records ─────────────────────────────────────────

    @Test
    void zeroLengthRecordsRoundTrip() throws IOException {
        Path file = tempDir.resolve("zerolen.slab");
        try (SlabWriter writer = new SlabWriter(file, 512)) {
            writer.write(0, new byte[0]);
            writer.write(1, new byte[0]);
            writer.write(2, "notempty".getBytes());
        }
        try (SlabReader reader = new SlabReader(file)) {
            Optional<ByteBuffer> r0 = reader.get(0);
            assertThat(r0).isPresent();
            assertThat(r0.get().remaining()).isZero();

            Optional<ByteBuffer> r1 = reader.get(1);
            assertThat(r1).isPresent();
            assertThat(r1.get().remaining()).isZero();

            Optional<ByteBuffer> r2 = reader.get(2);
            assertThat(r2).isPresent();
            byte[] bytes = new byte[r2.get().remaining()];
            r2.get().get(bytes);
            assertThat(new String(bytes)).isEqualTo("notempty");
        }
    }

    @Test
    void allZeroLengthRecordsOnOnePage() throws IOException {
        Path file = tempDir.resolve("allzero.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            for (int i = 0; i < 100; i++) {
                writer.write(i, new byte[0]);
            }
        }
        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.recordCount()).isEqualTo(100);
            for (int i = 0; i < 100; i++) {
                Optional<ByteBuffer> r = reader.get(i);
                assertThat(r).as("ordinal %d", i).isPresent();
                assertThat(r.get().remaining()).isZero();
            }
        }
    }

    // ── Exact page-boundary sizing ──────────────────────────────────

    @Test
    void recordThatExactlyFillsMinPageSize() throws IOException {
        // Calculate exact payload that fills 512-byte page:
        // header(8) + payload + 2*offset(8) + footer(16) = 512
        // payload = 512 - 8 - 8 - 16 = 480
        int payloadSize = MIN_PAGE_SIZE - HEADER_SIZE - 2 * OFFSET_ENTRY_SIZE - FOOTER_V1_SIZE;
        byte[] payload = new byte[payloadSize];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) i;
        }

        Path file = tempDir.resolve("exactfit.slab");
        try (SlabWriter writer = new SlabWriter(file, MIN_PAGE_SIZE)) {
            writer.write(0, payload);
        }
        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.recordCount()).isEqualTo(1);
            Optional<ByteBuffer> r = reader.get(0);
            assertThat(r).isPresent();
            byte[] got = new byte[r.get().remaining()];
            r.get().get(got);
            assertThat(got).isEqualTo(payload);
        }
    }

    @Test
    void recordOneByteOverPageBoundary() throws IOException {
        // This record is 1 byte too large for 512, so page grows to 1024
        int payloadSize = MIN_PAGE_SIZE - HEADER_SIZE - 2 * OFFSET_ENTRY_SIZE - FOOTER_V1_SIZE + 1;
        byte[] payload = new byte[payloadSize];

        SlabPage page = new SlabPage(0L, PAGE_TYPE_DATA, List.of(payload));
        assertThat(page.serializedSize()).isEqualTo(1024);
    }

    // ── Negative ordinals ───────────────────────────────────────────

    @Test
    void negativeOrdinalsWriteAndRead() throws IOException {
        Path file = tempDir.resolve("negative.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(-10, "neg10".getBytes());
            writer.write(-9, "neg9".getBytes());
            writer.write(-1, "neg1".getBytes());
            writer.write(0, "zero".getBytes());
            writer.write(1, "pos1".getBytes());
        }
        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.recordCount()).isEqualTo(5);
            assertRecordEquals(reader, -10, "neg10");
            assertRecordEquals(reader, -9, "neg9");
            assertRecordEquals(reader, -1, "neg1");
            assertRecordEquals(reader, 0, "zero");
            assertRecordEquals(reader, 1, "pos1");
        }
    }

    @Test
    void extremeOrdinalValues() throws IOException {
        Path file = tempDir.resolve("extreme.slab");
        try (SlabWriter writer = new SlabWriter(file, 512)) {
            writer.write(MIN_ORDINAL, "min".getBytes());
            writer.write(MAX_ORDINAL, "max".getBytes());
        }
        try (SlabReader reader = new SlabReader(file)) {
            assertRecordEquals(reader, MIN_ORDINAL, "min");
            assertRecordEquals(reader, MAX_ORDINAL, "max");
            assertThat(reader.get(0)).isEmpty();
        }
    }

    // ── Large records ───────────────────────────────────────────────

    @Test
    void largeRecordNearPageLimit() throws IOException {
        // ~3500 bytes of payload on a 4096-byte page
        byte[] big = new byte[3500];
        for (int i = 0; i < big.length; i++) big[i] = (byte) (i % 251);

        Path file = tempDir.resolve("bigrecord.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(0, big);
        }
        try (SlabReader reader = new SlabReader(file)) {
            Optional<ByteBuffer> r = reader.get(0);
            assertThat(r).isPresent();
            byte[] got = new byte[r.get().remaining()];
            r.get().get(got);
            assertThat(got).isEqualTo(big);
        }
    }

    // ── Many small pages ────────────────────────────────────────────

    @Test
    void manyPagesStressTest() throws IOException {
        Path file = tempDir.resolve("manypages.slab");
        int count = 500;
        // Force each record onto its own page by using min page size with
        // large enough records
        try (SlabWriter writer = new SlabWriter(file, 512)) {
            for (int i = 0; i < count; i++) {
                byte[] data = ("record-" + i).getBytes();
                writer.write(i, data);
            }
        }
        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.recordCount()).isEqualTo(count);
            // Spot-check some records
            assertRecordEquals(reader, 0, "record-0");
            assertRecordEquals(reader, 249, "record-249");
            assertRecordEquals(reader, 499, "record-499");
            assertThat(reader.get(500)).isEmpty();
        }
    }

    // ── Ordinal gap patterns ────────────────────────────────────────

    @Test
    void singletonOrdinalsEachOnOwnPage() throws IOException {
        Path file = tempDir.resolve("singletons.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            // Large gaps force separate pages
            writer.write(0, "a".getBytes());
            writer.write(1000, "b".getBytes());
            writer.write(2000, "c".getBytes());
        }
        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.pageCount()).isEqualTo(3);
            assertRecordEquals(reader, 0, "a");
            assertRecordEquals(reader, 1000, "b");
            assertRecordEquals(reader, 2000, "c");

            // Everything in between is empty
            assertThat(reader.get(1)).isEmpty();
            assertThat(reader.get(999)).isEmpty();
            assertThat(reader.get(1001)).isEmpty();
            assertThat(reader.get(1999)).isEmpty();
        }
    }

    // ── Footer field corruption ─────────────────────────────────────

    @Test
    void footerWithPageSizeSmallerThanFileSizeReportsInvalid() throws IOException {
        Path file = tempDir.resolve("badpagesz.slab");

        // Craft a 512-byte buffer with valid header but footer claims 256-byte page
        ByteBuffer buf = ByteBuffer.allocate(512).order(ByteOrder.LITTLE_ENDIAN);
        new PageHeader(512).writeTo(buf, 0);
        new PageFooter(0L, 0, 256, PAGE_TYPE_PAGES_PAGE, NAMESPACE_DEFAULT, (short) FOOTER_V1_SIZE)
            .writeTo(buf, 512 - FOOTER_V1_SIZE);

        try (FileChannel ch = FileChannel.open(file,
            StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            buf.rewind();
            ch.write(buf);
        }

        // Reader reads footer pageSize=256, which is less than min
        assertThatThrownBy(() -> new SlabReader(file))
            .hasMessageContaining("below minimum");
    }

    @Test
    void footerRecordCountOverflowingOffsetTable() {
        // A footer claiming more records than could fit in the page
        // should cause offset table to overlap the header
        ByteBuffer buf = ByteBuffer.allocate(512).order(ByteOrder.LITTLE_ENDIAN);
        new PageHeader(512).writeTo(buf, 0);
        // Claim 200 records → (200+1)*4 = 804 bytes of offsets, larger than the page
        new PageFooter(0L, 200, 512, PAGE_TYPE_DATA, NAMESPACE_DEFAULT, (short) FOOTER_V1_SIZE)
            .writeTo(buf, 512 - FOOTER_V1_SIZE);

        // parseFrom should detect that offsetsStart < HEADER_SIZE or produce garbage
        assertThatThrownBy(() -> SlabPage.parseFrom(buf))
            .isInstanceOf(Exception.class);
    }

    // ── Writer validation ───────────────────────────────────────────

    @Test
    void writerRejectsPageSizeBelowMinimum() {
        assertThatThrownBy(() -> new SlabWriter(tempDir.resolve("bad.slab"), 256))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("below minimum");
    }

    @Test
    void writerRejectsUnalignedPageSize() {
        assertThatThrownBy(() -> new SlabWriter(tempDir.resolve("bad.slab"), 1000))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("not a multiple");
    }

    // ── PageFooter boundary values ──────────────────────────────────

    @Test
    void footerOrdinalSignExtensionEdgeCases() {
        // Value that has bit 39 set (negative in 5-byte space)
        long edgeNegative = -(1L << 38); // -2^38
        PageFooter footer = new PageFooter(edgeNegative, 1, 512, PAGE_TYPE_DATA, NAMESPACE_DEFAULT, (short) FOOTER_V1_SIZE);
        ByteBuffer buf = ByteBuffer.allocate(16);
        footer.writeTo(buf, 0);
        PageFooter read = PageFooter.readFrom(buf, 0);
        assertThat(read.startOrdinal()).isEqualTo(edgeNegative);

        // Value just below the positive boundary (all lower 39 bits set)
        long edgePositive = (1L << 38) - 1; // 2^38 - 1
        PageFooter footer2 = new PageFooter(edgePositive, 1, 512, PAGE_TYPE_DATA, NAMESPACE_DEFAULT, (short) FOOTER_V1_SIZE);
        buf.clear();
        footer2.writeTo(buf, 0);
        PageFooter read2 = PageFooter.readFrom(buf, 0);
        assertThat(read2.startOrdinal()).isEqualTo(edgePositive);
    }

    @Test
    void footerRecordCountBoundaryValues() {
        // recordCount = 1
        roundTripRecordCount(1);
        // recordCount = 255 (max of first byte)
        roundTripRecordCount(255);
        // recordCount = 256 (overflows first byte)
        roundTripRecordCount(256);
        // recordCount = 65535 (max of first two bytes)
        roundTripRecordCount(65535);
        // recordCount = 65536 (overflows into third byte)
        roundTripRecordCount(65536);
        // recordCount = MAX_RECORD_COUNT
        roundTripRecordCount(MAX_RECORD_COUNT);
    }

    private void roundTripRecordCount(int count) {
        PageFooter footer = new PageFooter(0L, count, 512, PAGE_TYPE_DATA, NAMESPACE_DEFAULT, (short) FOOTER_V1_SIZE);
        ByteBuffer buf = ByteBuffer.allocate(16);
        footer.writeTo(buf, 0);
        PageFooter read = PageFooter.readFrom(buf, 0);
        assertThat(read.recordCount()).as("recordCount=%d", count).isEqualTo(count);
    }

    // ── SlabPage edge cases ─────────────────────────────────────────

    @Test
    void emptyDataPageRoundTrips() throws IOException {
        Path file = tempDir.resolve("empty_page.slab");
        // SlabWriter with no records should still produce a valid file
        try (SlabWriter writer = new SlabWriter(file, 512)) {
            // write nothing
        }
        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.pageCount()).isZero();
            assertThat(reader.recordCount()).isZero();
            assertThat(reader.get(0)).isEmpty();
        }
    }

    @Test
    void pageWithManySmallRecords() {
        // Pack many 1-byte records to stress the offset table
        int n = 50;
        java.util.List<byte[]> records = new java.util.ArrayList<>();
        for (int i = 0; i < n; i++) {
            records.add(new byte[]{(byte) i});
        }
        SlabPage page = new SlabPage(0L, PAGE_TYPE_DATA, records);
        ByteBuffer buf = page.toByteBuffer();
        SlabPage parsed = SlabPage.parseFrom(buf);

        assertThat(parsed.recordCount()).isEqualTo(n);
        for (int i = 0; i < n; i++) {
            ByteBuffer rec = parsed.getRecord(i);
            assertThat(rec.remaining()).isEqualTo(1);
            assertThat(rec.get()).isEqualTo((byte) i);
        }
    }

    // ── Binary content integrity ────────────────────────────────────

    @Test
    void binaryPayloadsPreservedExactly() throws IOException {
        // Write records containing every possible byte value
        byte[] allBytes = new byte[256];
        for (int i = 0; i < 256; i++) allBytes[i] = (byte) i;

        Path file = tempDir.resolve("binary.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(0, allBytes);
        }
        try (SlabReader reader = new SlabReader(file)) {
            Optional<ByteBuffer> r = reader.get(0);
            assertThat(r).isPresent();
            byte[] got = new byte[r.get().remaining()];
            r.get().get(got);
            assertThat(got).isEqualTo(allBytes);
        }
    }

    // ── Repack round-trip ───────────────────────────────────────────

    @Test
    void repackPreservesAllRecords() throws IOException {
        Path original = tempDir.resolve("orig.slab");
        try (SlabWriter writer = new SlabWriter(original, 512)) {
            for (int i = 0; i < 20; i++) {
                writer.write(i, ("data-" + i).getBytes());
            }
        }

        // Repack into a file with different page size
        Path repacked = tempDir.resolve("repacked.slab");
        try (SlabReader reader = new SlabReader(original)) {
            try (SlabWriter writer = new SlabWriter(repacked, 4096)) {
                for (SlabReader.PageSummary ps : reader.pages()) {
                    for (int i = 0; i < ps.recordCount(); i++) {
                        long ordinal = ps.startOrdinal() + i;
                        ByteBuffer data = reader.get(ordinal).orElseThrow();
                        byte[] bytes = new byte[data.remaining()];
                        data.get(bytes);
                        writer.write(ordinal, bytes);
                    }
                }
            }
        }

        // Verify repacked file
        try (SlabReader reader = new SlabReader(repacked)) {
            assertThat(reader.recordCount()).isEqualTo(20);
            for (int i = 0; i < 20; i++) {
                assertRecordEquals(reader, i, "data-" + i);
            }
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
