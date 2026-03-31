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

package io.nosqlbench.slabtastic.cli;

import io.nosqlbench.slabtastic.SlabReader;
import io.nosqlbench.slabtastic.SlabWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

/// Tests for {@link CMD_slab_export}.
class SlabExportTest {

    @TempDir
    Path tempDir;

    @Test
    void exportToSlabFormat() throws IOException {
        Path source = createTestSlab("rec0", "rec1", "rec2");
        Path dest = tempDir.resolve("exported.slab");

        int exitCode = runExport(source, "--to", dest.toString(), "--format", "slab");
        assertThat(exitCode).isEqualTo(0);

        try (SlabReader reader = new SlabReader(dest)) {
            assertThat(reader.recordCount()).isEqualTo(3);
            assertRecord(reader, 0, "rec0");
            assertRecord(reader, 1, "rec1");
            assertRecord(reader, 2, "rec2");
        }
    }

    @Test
    void exportWithRange() throws IOException {
        Path source = createTestSlab("a", "b", "c", "d", "e");
        Path dest = tempDir.resolve("ranged.slab");

        int exitCode = runExport(source, "--to", dest.toString(),
            "--format", "slab", "--range", "[1,3)");
        assertThat(exitCode).isEqualTo(0);

        try (SlabReader reader = new SlabReader(dest)) {
            assertThat(reader.recordCount()).isEqualTo(2);
            assertRecord(reader, 1, "b");
            assertRecord(reader, 2, "c");
        }
    }

    @Test
    void exportRawToFile() throws IOException {
        Path source = createTestSlab("hello", "world");
        Path dest = tempDir.resolve("raw.bin");

        int exitCode = runExport(source, "--to", dest.toString(), "--format", "raw");
        assertThat(exitCode).isEqualTo(0);

        byte[] content = Files.readAllBytes(dest);
        assertThat(new String(content)).isEqualTo("helloworld");
    }

    @Test
    void exportTextFormat() throws IOException {
        Path source = createTestSlab("hello", "world");
        Path dest = tempDir.resolve("lines.txt");

        int exitCode = runExport(source, "--to", dest.toString(), "--format", "text");
        assertThat(exitCode).isEqualTo(0);

        String content = Files.readString(dest);
        assertThat(content).isEqualTo("hello\nworld\n");
    }

    @Test
    void exportRejectsExistingWithoutForce() throws IOException {
        Path source = createTestSlab("test");
        Path dest = tempDir.resolve("existing.txt");
        Files.writeString(dest, "old");

        int exitCode = runExport(source, "--to", dest.toString(), "--format", "raw");
        assertThat(exitCode).isEqualTo(1);
    }

    @Test
    void exportOverwritesWithForce() throws IOException {
        Path source = createTestSlab("new-data");
        Path dest = tempDir.resolve("existing.txt");
        Files.writeString(dest, "old");

        int exitCode = runExport(source, "--to", dest.toString(), "--format", "raw", "--force");
        assertThat(exitCode).isEqualTo(0);

        String content = Files.readString(dest);
        assertThat(content).isEqualTo("new-data");
    }

    @Test
    void exportSlabRequiresOutputPath() throws IOException {
        Path source = createTestSlab("test");

        int exitCode = runExport(source, "--format", "slab");
        assertThat(exitCode).isEqualTo(1);
    }

    @Test
    void exportWithSingleOrdinalRange() throws IOException {
        Path source = createTestSlab("a", "b", "c");
        Path dest = tempDir.resolve("single.slab");

        int exitCode = runExport(source, "--to", dest.toString(),
            "--format", "slab", "--range", "[1]");
        assertThat(exitCode).isEqualTo(0);

        try (SlabReader reader = new SlabReader(dest)) {
            assertThat(reader.recordCount()).isEqualTo(1);
            assertRecord(reader, 1, "b");
        }
    }

    @Test
    void exportAsHex() throws IOException {
        Path source = createTestSlab("AB");
        Path dest = tempDir.resolve("hex.txt");

        int exitCode = runExport(source, "--to", dest.toString(), "--as-hex");
        assertThat(exitCode).isEqualTo(0);

        String content = Files.readString(dest);
        // 'A' = 0x41, 'B' = 0x42
        assertThat(content.trim()).isEqualTo("41 42");
    }

    @Test
    void exportAsBase64() throws IOException {
        Path source = createTestSlab("AB");
        Path dest = tempDir.resolve("b64.txt");

        int exitCode = runExport(source, "--to", dest.toString(), "--as-base64");
        assertThat(exitCode).isEqualTo(0);

        String content = Files.readString(dest);
        // Base64 of "AB" = "QUI="
        assertThat(content.trim()).isEqualTo("QUI=");
    }

    @Test
    void exportAsHexAndBase64MutuallyExclusive() throws IOException {
        Path source = createTestSlab("test");
        Path dest = tempDir.resolve("mutex.txt");

        int exitCode = runExport(source, "--to", dest.toString(), "--as-hex", "--as-base64");
        assertThat(exitCode).isEqualTo(1);
    }

    /// Creates a test slab file with the given records at ordinals 0, 1, 2, ...
    private Path createTestSlab(String... records) throws IOException {
        Path file = tempDir.resolve("source_" + System.nanoTime() + ".slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            for (int i = 0; i < records.length; i++) {
                writer.write(i, records[i].getBytes());
            }
        }
        return file;
    }

    private int runExport(Path source, String... extraArgs) {
        String[] baseArgs = {"export", source.toString()};
        String[] allArgs = new String[baseArgs.length + extraArgs.length];
        System.arraycopy(baseArgs, 0, allArgs, 0, baseArgs.length);
        System.arraycopy(extraArgs, 0, allArgs, baseArgs.length, extraArgs.length);

        return new CommandLine(new CMD_slab())
            .setCaseInsensitiveEnumValuesAllowed(true)
            .setOptionsCaseInsensitive(true)
            .execute(allArgs);
    }

    private void assertRecord(SlabReader reader, long ordinal, String expected) {
        Optional<ByteBuffer> result = reader.get(ordinal);
        assertThat(result).as("ordinal %d", ordinal).isPresent();
        byte[] bytes = new byte[result.get().remaining()];
        result.get().get(bytes);
        assertThat(new String(bytes)).as("ordinal %d content", ordinal).isEqualTo(expected);
    }
}
