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

import static org.assertj.core.api.Assertions.*;

class SlabFormatValidationTest implements SlabConstants {

    @TempDir
    Path tempDir;

    @Test
    void rejectsEmptyFile() throws IOException {
        Path file = tempDir.resolve("empty.slab");
        Files.createFile(file);

        assertThatThrownBy(() -> new SlabReader(file))
            .isInstanceOf(IOException.class)
            .hasMessageContaining("too small");
    }

    @Test
    void rejectsFileSmallerThan16Bytes() throws IOException {
        Path file = tempDir.resolve("tiny.slab");
        Files.write(file, new byte[10]);

        assertThatThrownBy(() -> new SlabReader(file))
            .isInstanceOf(IOException.class)
            .hasMessageContaining("too small");
    }

    @Test
    void rejectsFileEndingWithDataPage() throws IOException {
        Path file = tempDir.resolve("noindex.slab");

        // Write a single data page without a pages page following it
        SlabPage dataPage = new SlabPage(0L, PAGE_TYPE_DATA, java.util.List.of("test".getBytes()));
        ByteBuffer buf = dataPage.toByteBuffer();

        try (FileChannel ch = FileChannel.open(file,
            StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            buf.rewind();
            ch.write(buf);
        }

        assertThatThrownBy(() -> new SlabReader(file))
            .isInstanceOf(IOException.class)
            .hasMessageContaining("pages page");
    }

    @Test
    void rejectsInvalidNamespaceIndex() throws IOException {
        Path file = tempDir.resolve("badnsidx.slab");

        // Manually create a page with namespace index 0 (reserved/invalid)
        ByteBuffer buf = ByteBuffer.allocate(512).order(ByteOrder.LITTLE_ENDIAN);
        // Write valid header
        new PageHeader(512).writeTo(buf, 0);
        // Write footer with invalid namespace index at the end of the page
        int footerOffset = 512 - FOOTER_V1_SIZE;
        new PageFooter(0L, 0, 512, PAGE_TYPE_PAGES_PAGE, NAMESPACE_INVALID, (short) FOOTER_V1_SIZE)
            .writeTo(buf, footerOffset);

        try (FileChannel ch = FileChannel.open(file,
            StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            buf.rewind();
            ch.write(buf);
        }

        assertThatThrownBy(() -> new SlabReader(file))
            .hasMessageContaining("namespace index");
    }

    @Test
    void acceptsNonDefaultNamespaceIndex() throws IOException {
        Path file = tempDir.resolve("ns99.slab");

        // Namespace index 99 is valid (any non-zero byte value in 1-127 is valid)
        ByteBuffer buf = ByteBuffer.allocate(512).order(ByteOrder.LITTLE_ENDIAN);
        new PageHeader(512).writeTo(buf, 0);
        int footerOffset = 512 - FOOTER_V1_SIZE;
        new PageFooter(0L, 0, 512, PAGE_TYPE_PAGES_PAGE, (byte) 99, (short) FOOTER_V1_SIZE)
            .writeTo(buf, footerOffset);

        try (FileChannel ch = FileChannel.open(file,
            StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            buf.rewind();
            ch.write(buf);
        }

        // Should not throw — namespace index 99 is valid
        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.pageCount()).isZero();
        }
    }
}
