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

import io.nosqlbench.slabtastic.SlabConstants;
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

/// Tests for {@link CMD_slab_import}.
class SlabImportTest implements SlabConstants {

    @TempDir
    Path tempDir;

    @Test
    void importTextFormat() throws IOException {
        Path source = tempDir.resolve("source.txt");
        Files.writeString(source, "hello\nworld\n");

        Path target = tempDir.resolve("target.slab");
        int exitCode = runImport(target, source, "--format", "text");

        assertThat(exitCode).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(2);
            assertRecord(reader, 0, "hello\n");
            assertRecord(reader, 1, "world\n");
        }
    }

    @Test
    void importCstringsFormat() throws IOException {
        Path source = tempDir.resolve("source.bin");
        Files.write(source, new byte[]{'a', 'b', 0, 'c', 'd', 0});

        Path target = tempDir.resolve("target.slab");
        int exitCode = runImport(target, source, "--format", "cstrings");

        assertThat(exitCode).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(2);
            assertRecordBytes(reader, 0, new byte[]{'a', 'b', 0});
            assertRecordBytes(reader, 1, new byte[]{'c', 'd', 0});
        }
    }

    @Test
    void importSlabFile() throws IOException {
        Path slabSource = tempDir.resolve("source.slab");
        try (SlabWriter writer = new SlabWriter(slabSource, 4096)) {
            writer.write(10, "ten".getBytes());
            writer.write(11, "eleven".getBytes());
        }

        Path target = tempDir.resolve("target.slab");
        int exitCode = runImport(target, slabSource);

        assertThat(exitCode).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(2);
            // Ordinals preserved from source
            assertRecord(reader, 10, "ten");
            assertRecord(reader, 11, "eleven");
        }
    }

    @Test
    void autoDetectNewlineTerminated() throws IOException {
        Path source = tempDir.resolve("source.dat");
        Files.writeString(source, "line1\nline2\n");

        Path target = tempDir.resolve("target.slab");
        int exitCode = runImport(target, source);

        assertThat(exitCode).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(2);
            assertRecord(reader, 0, "line1\n");
            assertRecord(reader, 1, "line2\n");
        }
    }

    @Test
    void autoDetectNullTerminated() throws IOException {
        Path source = tempDir.resolve("source.dat");
        Files.write(source, new byte[]{'x', 0, 'y', 0});

        Path target = tempDir.resolve("target.slab");
        int exitCode = runImport(target, source);

        assertThat(exitCode).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(2);
            assertRecordBytes(reader, 0, new byte[]{'x', 0});
            assertRecordBytes(reader, 1, new byte[]{'y', 0});
        }
    }

    @Test
    void autoDetectSlabExtension() throws IOException {
        Path slabSource = tempDir.resolve("data.slab");
        try (SlabWriter writer = new SlabWriter(slabSource, 4096)) {
            writer.write(0, "record".getBytes());
        }

        Path target = tempDir.resolve("target.slab");
        int exitCode = runImport(target, slabSource);

        assertThat(exitCode).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(1);
            assertRecord(reader, 0, "record");
        }
    }

    @Test
    void autoDetectTxtExtension() throws IOException {
        Path source = tempDir.resolve("data.txt");
        Files.writeString(source, "line1\nline2\n");

        Path target = tempDir.resolve("target.slab");
        int exitCode = runImport(target, source);

        assertThat(exitCode).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(2);
            assertRecord(reader, 0, "line1\n");
            assertRecord(reader, 1, "line2\n");
        }
    }

    @Test
    void autoDetectJsonExtension() throws IOException {
        Path source = tempDir.resolve("data.json");
        Files.writeString(source, "{\"a\":1}{\"b\":2}");

        Path target = tempDir.resolve("target.slab");
        int exitCode = runImport(target, source);

        assertThat(exitCode).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(2);
        }
    }

    @Test
    void autoDetectJsonlExtension() throws IOException {
        Path source = tempDir.resolve("data.jsonl");
        Files.writeString(source, "{\"a\":1}\n{\"b\":2}\n");

        Path target = tempDir.resolve("target.slab");
        int exitCode = runImport(target, source);

        assertThat(exitCode).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(2);
        }
    }

    @Test
    void autoDetectCsvExtension() throws IOException {
        Path source = tempDir.resolve("data.csv");
        Files.writeString(source, "name,age\nAlice,30\n");

        Path target = tempDir.resolve("target.slab");
        int exitCode = runImport(target, source);

        assertThat(exitCode).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(2);
        }
    }

    @Test
    void autoDetectTsvExtension() throws IOException {
        Path source = tempDir.resolve("data.tsv");
        Files.writeString(source, "name\tage\nAlice\t30\n");

        Path target = tempDir.resolve("target.slab");
        int exitCode = runImport(target, source);

        assertThat(exitCode).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(2);
        }
    }

    @Test
    void autoDetectYamlExtension() throws IOException {
        Path source = tempDir.resolve("data.yaml");
        Files.writeString(source, "key: value1\n---\nkey: value2\n");

        Path target = tempDir.resolve("target.slab");
        int exitCode = runImport(target, source);

        assertThat(exitCode).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(2);
        }
    }

    @Test
    void autoDetectYmlExtension() throws IOException {
        Path source = tempDir.resolve("data.yml");
        Files.writeString(source, "key: value1\n---\nkey: value2\n");

        Path target = tempDir.resolve("target.slab");
        int exitCode = runImport(target, source);

        assertThat(exitCode).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(2);
        }
    }

    @Test
    void forceTextOverridesAutoDetect() throws IOException {
        // File with null bytes, but force text mode
        Path source = tempDir.resolve("source.dat");
        Files.write(source, new byte[]{'a', 0, '\n', 'b', 0, '\n'});

        Path target = tempDir.resolve("target.slab");
        int exitCode = runImport(target, source, "--format", "text");

        assertThat(exitCode).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(2);
            assertRecordBytes(reader, 0, new byte[]{'a', 0, '\n'});
            assertRecordBytes(reader, 1, new byte[]{'b', 0, '\n'});
        }
    }

    @Test
    void forceCstringsOverridesAutoDetect() throws IOException {
        // File with newlines but force cstrings mode
        Path source = tempDir.resolve("source.dat");
        Files.write(source, new byte[]{'a', '\n', 0, 'b', '\n', 0});

        Path target = tempDir.resolve("target.slab");
        int exitCode = runImport(target, source, "--format", "cstrings");

        assertThat(exitCode).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(2);
            assertRecordBytes(reader, 0, new byte[]{'a', '\n', 0});
            assertRecordBytes(reader, 1, new byte[]{'b', '\n', 0});
        }
    }

    @Test
    void rejectTargetExistsWithoutForce() throws IOException {
        Path source = tempDir.resolve("source.txt");
        Files.writeString(source, "data\n");

        Path target = tempDir.resolve("target.slab");
        Files.writeString(target, "existing");

        int exitCode = runImport(target, source, "--format", "text");
        assertThat(exitCode).isEqualTo(1);
    }

    @Test
    void overwriteWithForce() throws IOException {
        Path source = tempDir.resolve("source.txt");
        Files.writeString(source, "new-data\n");

        Path target = tempDir.resolve("target.slab");
        Files.writeString(target, "existing");

        int exitCode = runImport(target, source, "--format", "text", "--force");

        assertThat(exitCode).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(1);
            assertRecord(reader, 0, "new-data\n");
        }
    }

    @Test
    void customStartOrdinal() throws IOException {
        Path source = tempDir.resolve("source.txt");
        Files.writeString(source, "a\nb\nc\n");

        Path target = tempDir.resolve("target.slab");
        int exitCode = runImport(target, source, "--format", "text", "--start-ordinal", "100");

        assertThat(exitCode).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(3);
            assertRecord(reader, 100, "a\n");
            assertRecord(reader, 101, "b\n");
            assertRecord(reader, 102, "c\n");
        }
    }

    @Test
    void customPageSize() throws IOException {
        Path source = tempDir.resolve("source.txt");
        Files.writeString(source, "record1\nrecord2\nrecord3\n");

        Path target = tempDir.resolve("target.slab");
        int exitCode = runImport(target, source, "--format", "text", "--page-size", "512");

        assertThat(exitCode).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(3);
            assertRecord(reader, 0, "record1\n");
            assertRecord(reader, 1, "record2\n");
            assertRecord(reader, 2, "record3\n");
        }
    }

    @Test
    void trailingContentWithoutDelimiter() throws IOException {
        Path source = tempDir.resolve("source.txt");
        Files.writeString(source, "first\nsecond");  // no trailing newline

        Path target = tempDir.resolve("target.slab");
        int exitCode = runImport(target, source, "--format", "text");

        assertThat(exitCode).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(2);
            assertRecord(reader, 0, "first\n");
            assertRecord(reader, 1, "second");  // no trailing newline
        }
    }

    @Test
    void emptySourceFileProducesZeroRecords() throws IOException {
        Path source = tempDir.resolve("empty.txt");
        Files.writeString(source, "");

        Path target = tempDir.resolve("target.slab");
        int exitCode = runImport(target, source, "--format", "text");

        assertThat(exitCode).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(0);
        }
    }

    @Test
    void importJsonlRecords() throws IOException {
        Path source = tempDir.resolve("source.jsonl");
        Files.writeString(source, "{\"a\":1}\n{\"b\":2}\n");

        Path target = tempDir.resolve("target.slab");
        int exitCode = runImport(target, source, "--format", "jsonl");

        assertThat(exitCode).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(2);
        }
    }

    @Test
    void importCsvRecords() throws IOException {
        Path source = tempDir.resolve("source.csv");
        Files.writeString(source, "name,age\nAlice,30\nBob,25\n");

        Path target = tempDir.resolve("target.slab");
        int exitCode = runImport(target, source, "--format", "csv");

        assertThat(exitCode).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(3);
            assertRecord(reader, 0, "name,age\n");
            assertRecord(reader, 1, "Alice,30\n");
            assertRecord(reader, 2, "Bob,25\n");
        }
    }

    @Test
    void importCsvWithQuotedNewline() throws IOException {
        Path source = tempDir.resolve("source.csv");
        Files.writeString(source, "\"line1\nline2\",val\nrow2,val2\n");

        Path target = tempDir.resolve("target.slab");
        int exitCode = runImport(target, source, "--format", "csv");

        assertThat(exitCode).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            // The quoted newline should NOT split the record
            assertThat(reader.recordCount()).isEqualTo(2);
        }
    }

    @Test
    void importTsvRecords() throws IOException {
        Path source = tempDir.resolve("source.tsv");
        Files.writeString(source, "name\tage\nAlice\t30\n");

        Path target = tempDir.resolve("target.slab");
        int exitCode = runImport(target, source, "--format", "tsv");

        assertThat(exitCode).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(2);
        }
    }

    @Test
    void importJsonRecords() throws IOException {
        Path source = tempDir.resolve("source.json");
        Files.writeString(source, "{\"a\":1}{\"b\":2}");

        Path target = tempDir.resolve("target.slab");
        int exitCode = runImport(target, source, "--format", "json");

        assertThat(exitCode).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(2);
        }
    }

    @Test
    void importYamlRecords() throws IOException {
        Path source = tempDir.resolve("source.yaml");
        Files.writeString(source, "key: value1\n---\nkey: value2\n");

        Path target = tempDir.resolve("target.slab");
        int exitCode = runImport(target, source, "--format", "yaml");

        assertThat(exitCode).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(2);
        }
    }

    @Test
    void importSlabFormatFlag() throws IOException {
        // Create a slab file without .slab extension
        Path slabSource = tempDir.resolve("source.bin");
        try (SlabWriter writer = new SlabWriter(slabSource, 4096)) {
            writer.write(0, "test".getBytes());
        }

        Path target = tempDir.resolve("target.slab");
        int exitCode = runImport(target, slabSource, "--format", "slab");

        assertThat(exitCode).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(1);
            assertRecord(reader, 0, "test");
        }
    }

    @Test
    void appendToExistingTarget() throws IOException {
        // Create initial target
        Path target = tempDir.resolve("target.slab");
        try (SlabWriter writer = new SlabWriter(target, 4096)) {
            writer.write(0, "first".getBytes());
            writer.write(1, "second".getBytes());
        }

        // Import with --append
        Path source = tempDir.resolve("source.txt");
        Files.writeString(source, "third\nfourth\n");

        int exitCode = runImport(target, source, "--format", "text", "--append");
        assertThat(exitCode).isEqualTo(0);

        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(4);
            assertRecord(reader, 0, "first");
            assertRecord(reader, 1, "second");
            assertRecord(reader, 2, "third\n");
            assertRecord(reader, 3, "fourth\n");
        }
    }

    @Test
    void appendToNonExistentTargetCreatesNew() throws IOException {
        Path source = tempDir.resolve("source.txt");
        Files.writeString(source, "hello\nworld\n");

        Path target = tempDir.resolve("new-target.slab");
        int exitCode = runImport(target, source, "--format", "text", "--append");
        assertThat(exitCode).isEqualTo(0);

        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(2);
            assertRecord(reader, 0, "hello\n");
            assertRecord(reader, 1, "world\n");
        }
    }

    @Test
    void appendWithExplicitStartOrdinal() throws IOException {
        Path target = tempDir.resolve("target.slab");
        try (SlabWriter writer = new SlabWriter(target, 4096)) {
            writer.write(0, "first".getBytes());
        }

        Path source = tempDir.resolve("source.txt");
        Files.writeString(source, "second\n");

        int exitCode = runImport(target, source, "--format", "text", "--append", "--start-ordinal", "100");
        assertThat(exitCode).isEqualTo(0);

        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(2);
            assertRecord(reader, 0, "first");
            assertRecord(reader, 100, "second\n");
        }
    }

    /// Runs the import command with the given arguments.
    private int runImport(Path target, Path source, String... extraArgs) {
        String[] baseArgs = {"import", target.toString(), "--from", source.toString()};
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

    private void assertRecordBytes(SlabReader reader, long ordinal, byte[] expected) {
        Optional<ByteBuffer> result = reader.get(ordinal);
        assertThat(result).as("ordinal %d", ordinal).isPresent();
        byte[] bytes = new byte[result.get().remaining()];
        result.get().get(bytes);
        assertThat(bytes).as("ordinal %d content", ordinal).isEqualTo(expected);
    }
}
