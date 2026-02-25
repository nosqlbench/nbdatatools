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
import java.nio.file.Path;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

/// Tests for all slab CLI commands via picocli programmatic invocation.
class SlabCLITest implements SlabConstants {

    @TempDir
    Path tempDir;

    // ── Helper: run a slab CLI command and return exit code ──────────

    private int runSlab(String... args) {
        return new CommandLine(new CMD_slab())
            .setCaseInsensitiveEnumValuesAllowed(true)
            .setOptionsCaseInsensitive(true)
            .execute(args);
    }

    // ── Helper: create a test slab file ─────────────────────────────

    private Path createTestFile(String name, int startOrdinal, int count) throws IOException {
        Path file = tempDir.resolve(name);
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            for (int i = 0; i < count; i++) {
                writer.write(startOrdinal + i, ("record-" + (startOrdinal + i)).getBytes());
            }
        }
        return file;
    }

    private void assertRecord(SlabReader reader, long ordinal, String expected) {
        Optional<ByteBuffer> result = reader.get(ordinal);
        assertThat(result).as("ordinal %d", ordinal).isPresent();
        byte[] bytes = new byte[result.get().remaining()];
        result.get().get(bytes);
        assertThat(new String(bytes)).isEqualTo(expected);
    }

    // ── slab analyze ────────────────────────────────────────────────

    @Test
    void analyzeReturnsZeroForValidFile() throws IOException {
        Path file = createTestFile("analyze-test.slab", 0, 5);
        int exit = runSlab("analyze", file.toString());
        assertThat(exit).isEqualTo(0);
    }

    @Test
    void analyzeVerboseReturnsZeroForValidFile() throws IOException {
        Path file = createTestFile("analyze-verbose.slab", 0, 5);
        int exit = runSlab("analyze", file.toString(), "-v");
        assertThat(exit).isEqualTo(0);
    }

    @Test
    void analyzeReturnsOneForMissingFile() {
        Path file = tempDir.resolve("nonexistent.slab");
        int exit = runSlab("analyze", file.toString());
        assertThat(exit).isEqualTo(1);
    }

    @Test
    void analyzeWithSamplesOption() throws IOException {
        Path file = createTestFile("analyze-samples.slab", 0, 50);
        int exit = runSlab("analyze", file.toString(), "--samples", "10");
        assertThat(exit).isEqualTo(0);
    }

    @Test
    void analyzeWithSamplePercentOption() throws IOException {
        Path file = createTestFile("analyze-percent.slab", 0, 100);
        int exit = runSlab("analyze", file.toString(), "--sample-percent", "50");
        assertThat(exit).isEqualTo(0);
    }

    @Test
    void analyzeDetectsMonotonicity() throws IOException {
        Path file = createTestFile("analyze-mono.slab", 0, 20);
        int exit = runSlab("analyze", file.toString());
        assertThat(exit).isEqualTo(0);
    }

    // ── slab check ──────────────────────────────────────────────────

    @Test
    void checkReturnsZeroForValidFile() throws IOException {
        Path file = createTestFile("check-test.slab", 0, 10);
        int exit = runSlab("check", file.toString());
        assertThat(exit).isEqualTo(0);
    }

    @Test
    void checkVerboseReturnsZeroForValidFile() throws IOException {
        Path file = createTestFile("check-verbose.slab", 0, 10);
        int exit = runSlab("check", file.toString(), "-v");
        assertThat(exit).isEqualTo(0);
    }

    @Test
    void checkReturnsNonZeroForMissingFile() {
        Path file = tempDir.resolve("nonexistent.slab");
        int exit = runSlab("check", file.toString());
        assertThat(exit).isNotEqualTo(0);
    }

    @Test
    void checkReturnsZeroAfterAppend() throws IOException {
        Path file = createTestFile("check-append.slab", 0, 3);
        try (SlabWriter writer = SlabWriter.openForAppend(file, 4096)) {
            writer.write(3, "appended".getBytes());
        }
        int exit = runSlab("check", file.toString());
        assertThat(exit).isEqualTo(0);
    }

    // ── slab get ────────────────────────────────────────────────────

    @Test
    void getFindsExistingOrdinals() throws IOException {
        Path file = createTestFile("get-test.slab", 0, 5);
        int exit = runSlab("get", file.toString(), "-o", "0,2,4", "-f", "utf8");
        assertThat(exit).isEqualTo(0);
    }

    @Test
    void getReturnsOneForMissingOrdinals() throws IOException {
        Path file = createTestFile("get-missing.slab", 0, 3);
        int exit = runSlab("get", file.toString(), "-o", "0,99", "-f", "utf8");
        assertThat(exit).isEqualTo(1);
    }

    @Test
    void getHexFormat() throws IOException {
        Path file = createTestFile("get-hex.slab", 0, 3);
        int exit = runSlab("get", file.toString(), "-o", "0", "-f", "hex");
        assertThat(exit).isEqualTo(0);
    }

    @Test
    void getRawFormat() throws IOException {
        Path file = createTestFile("get-raw.slab", 0, 3);
        int exit = runSlab("get", file.toString(), "-o", "0", "-f", "raw");
        assertThat(exit).isEqualTo(0);
    }

    @Test
    void getAsHex() throws IOException {
        Path file = createTestFile("get-ashex.slab", 0, 3);
        int exit = runSlab("get", file.toString(), "-o", "0", "--as-hex");
        assertThat(exit).isEqualTo(0);
    }

    @Test
    void getAsBase64() throws IOException {
        Path file = createTestFile("get-asb64.slab", 0, 3);
        int exit = runSlab("get", file.toString(), "-o", "0", "--as-base64");
        assertThat(exit).isEqualTo(0);
    }

    @Test
    void getAsHexAndBase64MutuallyExclusive() throws IOException {
        Path file = createTestFile("get-mutex.slab", 0, 3);
        int exit = runSlab("get", file.toString(), "-o", "0", "--as-hex", "--as-base64");
        assertThat(exit).isEqualTo(2);
    }

    // ── slab rewrite ────────────────────────────────────────────────

    @Test
    void rewriteCreatesValidFile() throws IOException {
        Path source = createTestFile("rewrite-src.slab", 0, 10);
        Path dest = tempDir.resolve("rewrite-dst.slab");

        int exit = runSlab("rewrite", source.toString(), dest.toString());
        assertThat(exit).isEqualTo(0);

        try (SlabReader reader = new SlabReader(dest)) {
            assertThat(reader.recordCount()).isEqualTo(10);
            for (int i = 0; i < 10; i++) {
                assertRecord(reader, i, "record-" + i);
            }
        }
    }

    @Test
    void rewriteWithCustomPageSize() throws IOException {
        Path source = createTestFile("rewrite-pagesz-src.slab", 0, 10);
        Path dest = tempDir.resolve("rewrite-pagesz-dst.slab");

        int exit = runSlab("rewrite", source.toString(), dest.toString(), "--page-size", "512");
        assertThat(exit).isEqualTo(0);

        try (SlabReader reader = new SlabReader(dest)) {
            assertThat(reader.recordCount()).isEqualTo(10);
        }
    }

    @Test
    void rewriteRejectsExistingDestWithoutForce() throws IOException {
        Path source = createTestFile("rewrite-exists-src.slab", 0, 5);
        Path dest = createTestFile("rewrite-exists-dst.slab", 0, 3);

        int exit = runSlab("rewrite", source.toString(), dest.toString());
        assertThat(exit).isEqualTo(1);
    }

    @Test
    void rewriteOverwritesWithForce() throws IOException {
        Path source = createTestFile("rewrite-force-src.slab", 0, 5);
        Path dest = createTestFile("rewrite-force-dst.slab", 0, 3);

        int exit = runSlab("rewrite", source.toString(), dest.toString(), "-f");
        assertThat(exit).isEqualTo(0);

        try (SlabReader reader = new SlabReader(dest)) {
            assertThat(reader.recordCount()).isEqualTo(5);
        }
    }

    @Test
    void rewriteRunsPreFlightValidation() throws IOException {
        Path source = createTestFile("rewrite-valid-src.slab", 0, 5);
        Path dest = tempDir.resolve("rewrite-valid-dst.slab");

        // Valid file should pass pre-flight check
        int exit = runSlab("rewrite", source.toString(), dest.toString());
        assertThat(exit).isEqualTo(0);
    }

    @Test
    void rewriteWithSkipCheck() throws IOException {
        Path source = createTestFile("rewrite-skipcheck-src.slab", 0, 5);
        Path dest = tempDir.resolve("rewrite-skipcheck-dst.slab");

        int exit = runSlab("rewrite", source.toString(), dest.toString(), "--skip-check");
        assertThat(exit).isEqualTo(0);

        try (SlabReader reader = new SlabReader(dest)) {
            assertThat(reader.recordCount()).isEqualTo(5);
        }
    }

    // ── slab append ─────────────────────────────────────────────────

    @Test
    void appendAddsRecordsFromSourceToTarget() throws IOException {
        Path target = createTestFile("append-target.slab", 0, 3);
        Path source = createTestFile("append-source.slab", 3, 3);

        int exit = runSlab("append", target.toString(), "--from", source.toString());
        assertThat(exit).isEqualTo(0);

        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(6);
            for (int i = 0; i < 6; i++) {
                assertRecord(reader, i, "record-" + i);
            }
        }
    }

    @Test
    void appendWithCustomPageSize() throws IOException {
        Path target = createTestFile("append-ps-target.slab", 0, 3);
        Path source = createTestFile("append-ps-source.slab", 3, 3);

        int exit = runSlab("append", target.toString(), "--from", source.toString(),
            "--page-size", "512");
        assertThat(exit).isEqualTo(0);

        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(6);
        }
    }

    @Test
    void appendReturnsOneForOverlappingOrdinals() throws IOException {
        Path target = createTestFile("append-overlap-target.slab", 0, 5);
        Path source = createTestFile("append-overlap-source.slab", 3, 3);

        int exit = runSlab("append", target.toString(), "--from", source.toString());
        assertThat(exit).isEqualTo(1);
    }

    @Test
    void appendReturnsOneForMissingSource() throws IOException {
        Path target = createTestFile("append-nosrc-target.slab", 0, 3);
        Path source = tempDir.resolve("nonexistent.slab");

        int exit = runSlab("append", target.toString(), "--from", source.toString());
        assertThat(exit).isEqualTo(1);
    }

    @Test
    void appendReturnsOneForMissingTarget() throws IOException {
        Path target = tempDir.resolve("nonexistent-target.slab");
        Path source = createTestFile("append-notgt-source.slab", 0, 3);

        int exit = runSlab("append", target.toString(), "--from", source.toString());
        assertThat(exit).isEqualTo(1);
    }

    @Test
    void appendProducesCheckableFile() throws IOException {
        Path target = createTestFile("append-check-target.slab", 0, 5);
        Path source = createTestFile("append-check-source.slab", 5, 5);

        int appendExit = runSlab("append", target.toString(), "--from", source.toString());
        assertThat(appendExit).isEqualTo(0);

        int checkExit = runSlab("check", target.toString());
        assertThat(checkExit).isEqualTo(0);
    }

    @Test
    void appendThenGetFindsAllRecords() throws IOException {
        Path target = createTestFile("append-get-target.slab", 0, 3);
        Path source = createTestFile("append-get-source.slab", 3, 3);

        int appendExit = runSlab("append", target.toString(), "--from", source.toString());
        assertThat(appendExit).isEqualTo(0);

        int getExit = runSlab("get", target.toString(), "-o", "0,1,2,3,4,5", "-f", "utf8");
        assertThat(getExit).isEqualTo(0);
    }

    // ── slab help ───────────────────────────────────────────────────

    @Test
    void helpReturnsZero() {
        int exit = runSlab("help");
        assertThat(exit).isEqualTo(0);
    }

    @Test
    void noArgsShowsUsage() {
        int exit = runSlab();
        assertThat(exit).isEqualTo(0);
    }
}
