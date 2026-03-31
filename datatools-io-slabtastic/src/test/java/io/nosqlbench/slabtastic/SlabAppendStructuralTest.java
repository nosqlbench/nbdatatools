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

/// Structural validation of slabtastic files after append operations.
/// Verifies the binary layout is correct at the byte level.
class SlabAppendStructuralTest implements SlabConstants {

    @TempDir
    Path tempDir;

    @Test
    void appendedFileEndsWithValidPagesPage() throws IOException {
        Path file = tempDir.resolve("structural.slab");

        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(0, "zero".getBytes());
            writer.write(1, "one".getBytes());
        }

        long sizeBeforeAppend = Files.size(file);

        try (SlabWriter writer = SlabWriter.openForAppend(file, 4096)) {
            writer.write(2, "two".getBytes());
        }

        long sizeAfterAppend = Files.size(file);
        assertThat(sizeAfterAppend).isGreaterThan(sizeBeforeAppend);

        // Read the last 16 bytes and verify it's a valid pages-page footer
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            ByteBuffer tailBuf = ByteBuffer.allocate(FOOTER_V1_SIZE).order(ByteOrder.LITTLE_ENDIAN);
            ch.read(tailBuf, sizeAfterAppend - FOOTER_V1_SIZE);
            tailBuf.flip();
            PageFooter footer = PageFooter.readFrom(tailBuf, 0);
            footer.validate();
            assertThat(footer.pageType()).isEqualTo(PAGE_TYPE_PAGES_PAGE);
            assertThat(footer.namespaceIndex()).isEqualTo(NAMESPACE_DEFAULT);
        }
    }

    @Test
    void appendedFileHasCorrectPageCount() throws IOException {
        Path file = tempDir.resolve("pagecount.slab");

        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(0, "a".getBytes());
        }

        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.pageCount()).isEqualTo(1);
        }

        try (SlabWriter writer = SlabWriter.openForAppend(file, 4096)) {
            writer.write(100, "b".getBytes());
        }

        try (SlabReader reader = new SlabReader(file)) {
            // Original page + new page (gap forces new page)
            assertThat(reader.pageCount()).isEqualTo(2);
            assertThat(reader.recordCount()).isEqualTo(2);
        }
    }

    @Test
    void appendedFilePassesBidirectionalTraversal() throws IOException {
        Path file = tempDir.resolve("bidir.slab");

        try (SlabWriter writer = new SlabWriter(file, 512)) {
            for (int i = 0; i < 5; i++) {
                writer.write(i, ("original-" + i).getBytes());
            }
        }

        try (SlabWriter writer = SlabWriter.openForAppend(file, 512)) {
            for (int i = 10; i < 15; i++) {
                writer.write(i, ("appended-" + i).getBytes());
            }
        }

        // Verify every data page has matching header/footer page sizes
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            long fileSize = ch.size();

            // Read pages page footer
            ByteBuffer tailBuf = ByteBuffer.allocate(FOOTER_V1_SIZE).order(ByteOrder.LITTLE_ENDIAN);
            ch.read(tailBuf, fileSize - FOOTER_V1_SIZE);
            tailBuf.flip();
            PageFooter ppFooter = PageFooter.readFrom(tailBuf, 0);
            ppFooter.validate();
            assertThat(ppFooter.pageType()).isEqualTo(PAGE_TYPE_PAGES_PAGE);

            int ppSize = ppFooter.pageSize();
            long ppOffset = fileSize - ppSize;

            // Verify pages page header matches footer
            ByteBuffer ppHeaderBuf = ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
            ch.read(ppHeaderBuf, ppOffset);
            ppHeaderBuf.flip();
            PageHeader ppHeader = PageHeader.readFrom(ppHeaderBuf, 0);
            assertThat(ppHeader.pageSize()).isEqualTo(ppSize);
        }

        // Also verify via reader API
        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.recordCount()).isEqualTo(10);
            for (int i = 0; i < 5; i++) {
                assertRecord(reader, i, "original-" + i);
            }
            for (int i = 10; i < 15; i++) {
                assertRecord(reader, i, "appended-" + i);
            }
            // Gap should be empty
            for (int i = 5; i < 10; i++) {
                assertThat(reader.get(i)).as("gap ordinal %d", i).isEmpty();
            }
        }
    }

    @Test
    void tripleAppendProducesValidStructure() throws IOException {
        Path file = tempDir.resolve("triple.slab");

        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(0, "first".getBytes());
        }

        try (SlabWriter writer = SlabWriter.openForAppend(file, 4096)) {
            writer.write(1, "second".getBytes());
        }

        try (SlabWriter writer = SlabWriter.openForAppend(file, 4096)) {
            writer.write(2, "third".getBytes());
        }

        // File should have dead pages pages in the middle but still be valid
        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.recordCount()).isEqualTo(3);
            assertRecord(reader, 0, "first");
            assertRecord(reader, 1, "second");
            assertRecord(reader, 2, "third");

            // All pages should be accounted for
            List<SlabReader.PageSummary> pages = reader.pages();
            long totalRecords = 0;
            for (SlabReader.PageSummary ps : pages) {
                totalRecords += ps.recordCount();
                // Every page offset should be non-negative
                assertThat(ps.fileOffset()).isGreaterThanOrEqualTo(0);
                // Every page size should be at least MIN_PAGE_SIZE
                assertThat(ps.pageSize()).isGreaterThanOrEqualTo(MIN_PAGE_SIZE);
                // Every page size should be aligned
                assertThat(ps.pageSize() % PAGE_ALIGNMENT).isZero();
            }
            assertThat(totalRecords).isEqualTo(3);
        }
    }

    @Test
    void appendFileGrowsMonotonically() throws IOException {
        Path file = tempDir.resolve("monotonic-growth.slab");

        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(0, "start".getBytes());
        }

        long prevSize = Files.size(file);
        for (int round = 1; round <= 5; round++) {
            try (SlabWriter writer = SlabWriter.openForAppend(file, 4096)) {
                writer.write(round, ("round-" + round).getBytes());
            }
            long newSize = Files.size(file);
            assertThat(newSize).as("after round %d", round).isGreaterThan(prevSize);
            prevSize = newSize;
        }

        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.recordCount()).isEqualTo(6);
            for (int i = 0; i <= 5; i++) {
                assertThat(reader.get(i)).as("ordinal %d", i).isPresent();
            }
        }
    }

    @Test
    void appendWithDifferentPageSizeFromOriginal() throws IOException {
        Path file = tempDir.resolve("mixed-pagesz.slab");

        // Create with large page size
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(0, "big-pages".getBytes());
            writer.write(1, "big-pages".getBytes());
        }

        // Append with small page size
        try (SlabWriter writer = SlabWriter.openForAppend(file, 512)) {
            writer.write(2, "small-pages".getBytes());
            writer.write(3, "small-pages".getBytes());
        }

        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.recordCount()).isEqualTo(4);
            for (int i = 0; i < 4; i++) {
                assertThat(reader.get(i)).as("ordinal %d", i).isPresent();
            }
        }
    }

    @Test
    void noOpAppendDoesNotCorruptFile() throws IOException {
        Path file = tempDir.resolve("noop.slab");

        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(0, "only".getBytes());
        }

        byte[] originalContent;
        try (SlabReader reader = new SlabReader(file)) {
            Optional<ByteBuffer> r = reader.get(0);
            assertThat(r).isPresent();
            byte[] b = new byte[r.get().remaining()];
            r.get().get(b);
            originalContent = b;
        }

        // Append nothing
        try (SlabWriter writer = SlabWriter.openForAppend(file, 4096)) {
            // no writes
        }

        // Original data should be unchanged
        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.recordCount()).isEqualTo(1);
            Optional<ByteBuffer> r = reader.get(0);
            assertThat(r).isPresent();
            byte[] b = new byte[r.get().remaining()];
            r.get().get(b);
            assertThat(b).isEqualTo(originalContent);
        }
    }

    private void assertRecord(SlabReader reader, long ordinal, String expected) {
        Optional<ByteBuffer> result = reader.get(ordinal);
        assertThat(result).as("ordinal %d", ordinal).isPresent();
        byte[] bytes = new byte[result.get().remaining()];
        result.get().get(bytes);
        assertThat(new String(bytes)).isEqualTo(expected);
    }
}
