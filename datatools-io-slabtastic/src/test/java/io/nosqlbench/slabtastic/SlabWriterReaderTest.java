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
import java.nio.file.Path;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

class SlabWriterReaderTest implements SlabConstants {

    @TempDir
    Path tempDir;

    @Test
    void singleRecordWriteAndRead() throws IOException {
        Path file = tempDir.resolve("single.slab");
        try (SlabWriter writer = new SlabWriter(file, 512)) {
            writer.write(0, "hello".getBytes());
        }
        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.pageCount()).isEqualTo(1);
            assertThat(reader.recordCount()).isEqualTo(1);

            Optional<ByteBuffer> result = reader.get(0);
            assertThat(result).isPresent();
            byte[] bytes = new byte[result.get().remaining()];
            result.get().get(bytes);
            assertThat(bytes).isEqualTo("hello".getBytes());
        }
    }

    @Test
    void multipleRecordsOnOnePage() throws IOException {
        Path file = tempDir.resolve("multi.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(0, "alpha".getBytes());
            writer.write(1, "beta".getBytes());
            writer.write(2, "gamma".getBytes());
        }
        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.pageCount()).isEqualTo(1);
            assertThat(reader.recordCount()).isEqualTo(3);

            assertRecord(reader, 0, "alpha");
            assertRecord(reader, 1, "beta");
            assertRecord(reader, 2, "gamma");
        }
    }

    @Test
    void multiplePages() throws IOException {
        Path file = tempDir.resolve("pages.slab");
        // Use small page size to force multiple pages
        try (SlabWriter writer = new SlabWriter(file, 512)) {
            // Each record + overhead will fill a page quickly
            byte[] payload = new byte[200];
            for (int i = 0; i < 10; i++) {
                for (int j = 0; j < payload.length; j++) {
                    payload[j] = (byte) (i + j);
                }
                writer.write(i, payload.clone());
            }
        }
        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.pageCount()).isGreaterThan(1);
            assertThat(reader.recordCount()).isEqualTo(10);

            for (int i = 0; i < 10; i++) {
                Optional<ByteBuffer> result = reader.get(i);
                assertThat(result).as("ordinal %d", i).isPresent();
                byte[] expected = new byte[200];
                for (int j = 0; j < expected.length; j++) {
                    expected[j] = (byte) (i + j);
                }
                byte[] actual = new byte[result.get().remaining()];
                result.get().get(actual);
                assertThat(actual).as("ordinal %d data", i).isEqualTo(expected);
            }
        }
    }

    @Test
    void sparseOrdinalsReturnEmpty() throws IOException {
        Path file = tempDir.resolve("sparse.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(0, "zero".getBytes());
            writer.write(1, "one".getBytes());
            // Gap: ordinals 2-9 not written
            writer.write(10, "ten".getBytes());
            writer.write(11, "eleven".getBytes());
        }
        try (SlabReader reader = new SlabReader(file)) {
            assertRecord(reader, 0, "zero");
            assertRecord(reader, 1, "one");
            assertRecord(reader, 10, "ten");
            assertRecord(reader, 11, "eleven");

            // Ordinals in the gap should be empty
            for (int i = 2; i < 10; i++) {
                assertThat(reader.get(i)).as("ordinal %d", i).isEmpty();
            }
            // Ordinals before the start should be empty
            assertThat(reader.get(-1)).isEmpty();
            // Ordinals after the end should be empty
            assertThat(reader.get(12)).isEmpty();
        }
    }

    @Test
    void pageCountAndRecordCountAccuracy() throws IOException {
        Path file = tempDir.resolve("counts.slab");
        try (SlabWriter writer = new SlabWriter(file, 512)) {
            for (int i = 0; i < 50; i++) {
                writer.write(i, ("record-" + i).getBytes());
            }
        }
        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.recordCount()).isEqualTo(50);
            assertThat(reader.pageCount()).isGreaterThan(0);

            // Verify all records are readable
            for (int i = 0; i < 50; i++) {
                assertThat(reader.get(i)).as("ordinal %d", i).isPresent();
            }
        }
    }

    @Test
    void nonContiguousPageOrdinals() throws IOException {
        Path file = tempDir.resolve("noncontig.slab");
        // Force pages by using small page size, with ordinal jumps
        try (SlabWriter writer = new SlabWriter(file, 512)) {
            writer.write(100, "hundred".getBytes());
            writer.write(101, "hundred-one".getBytes());
            writer.write(200, "two-hundred".getBytes());
            writer.write(201, "two-hundred-one".getBytes());
        }
        try (SlabReader reader = new SlabReader(file)) {
            assertRecord(reader, 100, "hundred");
            assertRecord(reader, 101, "hundred-one");
            assertRecord(reader, 200, "two-hundred");
            assertRecord(reader, 201, "two-hundred-one");

            assertThat(reader.get(0)).isEmpty();
            assertThat(reader.get(102)).isEmpty();
            assertThat(reader.get(199)).isEmpty();
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
