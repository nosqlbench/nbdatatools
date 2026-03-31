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

/// Tests for {@link SlabWriter#openForAppend(Path, int)} and the append
/// workflow.
class SlabAppendTest implements SlabConstants {

    @TempDir
    Path tempDir;

    @Test
    void appendAddsRecordsAfterExistingData() throws IOException {
        Path file = tempDir.resolve("append.slab");

        // Write initial records
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(0, "zero".getBytes());
            writer.write(1, "one".getBytes());
            writer.write(2, "two".getBytes());
        }

        // Append more records
        try (SlabWriter writer = SlabWriter.openForAppend(file, 4096)) {
            writer.write(3, "three".getBytes());
            writer.write(4, "four".getBytes());
        }

        // Verify all records are readable
        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.recordCount()).isEqualTo(5);
            assertRecord(reader, 0, "zero");
            assertRecord(reader, 1, "one");
            assertRecord(reader, 2, "two");
            assertRecord(reader, 3, "three");
            assertRecord(reader, 4, "four");
        }
    }

    @Test
    void appendWithOrdinalGapFromExisting() throws IOException {
        Path file = tempDir.resolve("append-gap.slab");

        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(0, "zero".getBytes());
            writer.write(1, "one".getBytes());
        }

        // Append with a gap from the existing max ordinal (1)
        try (SlabWriter writer = SlabWriter.openForAppend(file, 4096)) {
            writer.write(100, "hundred".getBytes());
            writer.write(101, "hundred-one".getBytes());
        }

        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.recordCount()).isEqualTo(4);
            assertRecord(reader, 0, "zero");
            assertRecord(reader, 1, "one");
            assertRecord(reader, 100, "hundred");
            assertRecord(reader, 101, "hundred-one");

            // Gap ordinals should be empty
            for (int i = 2; i < 100; i++) {
                assertThat(reader.get(i)).as("ordinal %d", i).isEmpty();
            }
        }
    }

    @Test
    void appendPreservesExistingPagesPageStructure() throws IOException {
        Path file = tempDir.resolve("append-structure.slab");

        // Write records that span multiple pages
        try (SlabWriter writer = new SlabWriter(file, 512)) {
            byte[] payload = new byte[200];
            for (int i = 0; i < 5; i++) {
                for (int j = 0; j < payload.length; j++) {
                    payload[j] = (byte) (i + j);
                }
                writer.write(i, payload.clone());
            }
        }

        int originalPageCount;
        try (SlabReader reader = new SlabReader(file)) {
            originalPageCount = reader.pageCount();
            assertThat(originalPageCount).isGreaterThan(1);
        }

        // Append more records
        try (SlabWriter writer = SlabWriter.openForAppend(file, 512)) {
            byte[] payload = new byte[200];
            for (int j = 0; j < payload.length; j++) {
                payload[j] = (byte) (99 + j);
            }
            writer.write(99, payload);
        }

        try (SlabReader reader = new SlabReader(file)) {
            // Should have at least the original pages + the new one
            assertThat(reader.pageCount()).isGreaterThanOrEqualTo(originalPageCount + 1);
            assertThat(reader.recordCount()).isEqualTo(6);

            // Verify original records
            for (int i = 0; i < 5; i++) {
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

            // Verify appended record
            Optional<ByteBuffer> result = reader.get(99);
            assertThat(result).isPresent();
            byte[] expected = new byte[200];
            for (int j = 0; j < expected.length; j++) {
                expected[j] = (byte) (99 + j);
            }
            byte[] actual = new byte[result.get().remaining()];
            result.get().get(actual);
            assertThat(actual).isEqualTo(expected);
        }
    }

    @Test
    void appendRejectsOrdinalNotStrictlyAscending() throws IOException {
        Path file = tempDir.resolve("append-reject.slab");

        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(0, "zero".getBytes());
            writer.write(1, "one".getBytes());
        }

        // Ordinal 1 is the max existing — writing 1 again should fail
        try (SlabWriter writer = SlabWriter.openForAppend(file, 4096)) {
            assertThatThrownBy(() -> writer.write(1, "duplicate".getBytes()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not strictly ascending");
        }
    }

    @Test
    void appendRejectsOrdinalBeforeExisting() throws IOException {
        Path file = tempDir.resolve("append-before.slab");

        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(10, "ten".getBytes());
            writer.write(11, "eleven".getBytes());
        }

        // Ordinal 5 is before existing max (11) — should fail
        try (SlabWriter writer = SlabWriter.openForAppend(file, 4096)) {
            assertThatThrownBy(() -> writer.write(5, "five".getBytes()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not strictly ascending");
        }
    }

    @Test
    void appendWithNoNewRecordsProducesValidFile() throws IOException {
        Path file = tempDir.resolve("append-noop.slab");

        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(0, "zero".getBytes());
        }

        // Open for append but write nothing
        try (SlabWriter writer = SlabWriter.openForAppend(file, 4096)) {
            // no writes
        }

        // File should still be valid
        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.recordCount()).isEqualTo(1);
            assertRecord(reader, 0, "zero");
        }
    }

    @Test
    void multipleAppendCycles() throws IOException {
        Path file = tempDir.resolve("multi-append.slab");

        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(0, "a".getBytes());
        }

        // First append
        try (SlabWriter writer = SlabWriter.openForAppend(file, 4096)) {
            writer.write(1, "b".getBytes());
        }

        // Second append
        try (SlabWriter writer = SlabWriter.openForAppend(file, 4096)) {
            writer.write(2, "c".getBytes());
        }

        // Third append
        try (SlabWriter writer = SlabWriter.openForAppend(file, 4096)) {
            writer.write(3, "d".getBytes());
        }

        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.recordCount()).isEqualTo(4);
            assertRecord(reader, 0, "a");
            assertRecord(reader, 1, "b");
            assertRecord(reader, 2, "c");
            assertRecord(reader, 3, "d");
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
