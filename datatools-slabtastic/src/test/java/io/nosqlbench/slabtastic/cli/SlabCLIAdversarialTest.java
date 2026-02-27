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

/// Adversarial and corner-case tests for all slab CLI commands.
///
/// Covers namespace options, edge-case formats, conflicting options,
/// missing parameters, empty files, and cross-command interactions.
class SlabCLIAdversarialTest implements SlabConstants {

    @TempDir
    Path tempDir;

    private int runSlab(String... args) {
        return new CommandLine(new CMD_slab())
            .setCaseInsensitiveEnumValuesAllowed(true)
            .setOptionsCaseInsensitive(true)
            .execute(args);
    }

    private Path createTestFile(String name, int startOrdinal, int count) throws IOException {
        Path file = tempDir.resolve(name);
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            for (int i = 0; i < count; i++) {
                writer.write(startOrdinal + i, ("record-" + (startOrdinal + i)).getBytes());
            }
        }
        return file;
    }

    private Path createMultiNsFile(String name) throws IOException {
        Path file = tempDir.resolve(name);
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write("ns1", 0, "ns1-0".getBytes());
            writer.write("ns1", 1, "ns1-1".getBytes());
            writer.write("ns2", 0, "ns2-0".getBytes());
            writer.write("ns2", 1, "ns2-1".getBytes());
            writer.write("ns2", 2, "ns2-2".getBytes());
        }
        return file;
    }

    private void assertRecord(SlabReader reader, String ns, long ordinal, String expected) {
        Optional<ByteBuffer> result = reader.get(ns, ordinal);
        assertThat(result).as("ns=%s ordinal=%d", ns, ordinal).isPresent();
        byte[] bytes = new byte[result.get().remaining()];
        result.get().get(bytes);
        assertThat(new String(bytes)).isEqualTo(expected);
    }

    // ── slab namespaces command ─────────────────────────────────────

    @Test
    void namespacesOnSingleNamespaceFile() throws IOException {
        Path file = createTestFile("ns-single.slab", 0, 5);
        int exit = runSlab("namespaces", file.toString());
        assertThat(exit).isEqualTo(0);
    }

    @Test
    void namespacesOnMultiNamespaceFile() throws IOException {
        Path file = createMultiNsFile("ns-multi.slab");
        int exit = runSlab("namespaces", file.toString());
        assertThat(exit).isEqualTo(0);
    }

    @Test
    void namespacesOnMissingFile() {
        int exit = runSlab("namespaces", tempDir.resolve("nope.slab").toString());
        assertThat(exit).isEqualTo(1);
    }

    @Test
    void namespacesOnEmptyFile() throws IOException {
        Path file = tempDir.resolve("empty.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            // no records
        }
        int exit = runSlab("namespaces", file.toString());
        assertThat(exit).isEqualTo(0);
    }

    // ── analyze with --namespace ────────────────────────────────────

    @Test
    void analyzeWithNamespaceOnMultiNsFile() throws IOException {
        Path file = createMultiNsFile("analyze-ns.slab");
        int exit = runSlab("analyze", file.toString(), "-n", "ns1");
        assertThat(exit).isEqualTo(0);
    }

    @Test
    void analyzeWithNonexistentNamespace() throws IOException {
        Path file = createMultiNsFile("analyze-bad-ns.slab");
        int exit = runSlab("analyze", file.toString(), "-n", "doesnotexist");
        // Unknown namespace should fail with an error
        assertThat(exit).isEqualTo(1);
    }

    @Test
    void analyzeWithNamespaceOnSingleNsFile() throws IOException {
        Path file = createTestFile("analyze-single-ns.slab", 0, 5);
        // Using -n on a single-namespace file — the default namespace is ""
        int exit = runSlab("analyze", file.toString(), "-n", "");
        assertThat(exit).isEqualTo(0);
    }

    // ── get with --namespace ────────────────────────────────────────

    @Test
    void getWithNamespaceOnMultiNsFile() throws IOException {
        Path file = createMultiNsFile("get-ns.slab");
        int exit = runSlab("get", file.toString(), "-n", "ns1", "-o", "0,1", "-f", "utf8");
        assertThat(exit).isEqualTo(0);
    }

    @Test
    void getWithNonexistentNamespaceReportsError() throws IOException {
        Path file = createMultiNsFile("get-bad-ns.slab");
        int exit = runSlab("get", file.toString(), "-n", "nope", "-o", "0");
        // Unknown namespace should fail with error (exit 2)
        assertThat(exit).isEqualTo(2);
    }

    @Test
    void getWrongNamespaceOrdinalMissing() throws IOException {
        Path file = createMultiNsFile("get-wrong-ns.slab");
        // ns1 has ordinals 0,1 — getting ordinal 2 which only exists in ns2
        int exit = runSlab("get", file.toString(), "-n", "ns1", "-o", "2");
        assertThat(exit).isEqualTo(1);
    }

    // ── rewrite with --namespace ────────────────────────────────────

    @Test
    void rewriteWithNamespaceFromMultiNsFile() throws IOException {
        Path source = createMultiNsFile("rewrite-src-ns.slab");
        Path dest = tempDir.resolve("rewrite-dst-ns.slab");

        int exit = runSlab("rewrite", source.toString(), dest.toString(), "-n", "ns2");
        assertThat(exit).isEqualTo(0);

        try (SlabReader reader = new SlabReader(dest)) {
            assertThat(reader.recordCount("ns2")).isEqualTo(3);
        }
    }

    @Test
    void rewriteWithNonexistentNamespaceReportsError() throws IOException {
        Path source = createMultiNsFile("rewrite-empty-ns-src.slab");
        Path dest = tempDir.resolve("rewrite-empty-ns-dst.slab");

        int exit = runSlab("rewrite", source.toString(), dest.toString(), "-n", "nope");
        // Unknown namespace should fail with error
        assertThat(exit).isEqualTo(1);
    }

    // ── append with --namespace ─────────────────────────────────────

    @Test
    void appendWithNamespaceFromMultiNsFile() throws IOException {
        Path target = tempDir.resolve("append-ns-target.slab");
        try (SlabWriter writer = new SlabWriter(target, 4096)) {
            writer.write(0, "existing-0".getBytes());
            writer.write(1, "existing-1".getBytes());
        }

        Path source = createMultiNsFile("append-ns-source.slab");

        // Append ns2 records (ordinals 0,1,2) — but target already has 0,1 in default ns
        // Since --namespace applies to both source and target and the target is default ns,
        // this reads ns2 from source and writes with ns2 namespace to target
        int exit = runSlab("append", target.toString(), "--from", source.toString(), "-n", "ns2");
        assertThat(exit).isEqualTo(0);
    }

    // ── import with --namespace ─────────────────────────────────────

    @Test
    void importWithNamespaceCreatesNamedNamespace() throws IOException {
        Path source = tempDir.resolve("import-ns-src.txt");
        Files.writeString(source, "line1\nline2\n");

        Path target = tempDir.resolve("import-ns-target.slab");
        int exit = runSlab("import", target.toString(), "--from", source.toString(),
            "--format", "text", "-n", "myns");
        assertThat(exit).isEqualTo(0);

        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.namespaces()).containsExactlyInAnyOrder("", "myns");
            assertThat(reader.recordCount("myns")).isEqualTo(2);
            // Default namespace is present but empty
            assertThat(reader.recordCount()).isZero();
        }
    }

    @Test
    void importWithDefaultNamespaceIsBackwardCompat() throws IOException {
        Path source = tempDir.resolve("import-default-ns-src.txt");
        Files.writeString(source, "hello\nworld\n");

        Path target = tempDir.resolve("import-default-ns-target.slab");
        int exit = runSlab("import", target.toString(), "--from", source.toString(),
            "--format", "text");
        assertThat(exit).isEqualTo(0);

        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.namespaces()).containsExactly("");
            assertThat(reader.recordCount()).isEqualTo(2);
        }
    }

    // ── export with --namespace ─────────────────────────────────────

    @Test
    void exportWithNamespaceToSlabFormat() throws IOException {
        Path source = createMultiNsFile("export-ns-src.slab");
        Path dest = tempDir.resolve("export-ns-dst.slab");

        int exit = runSlab("export", source.toString(), "--to", dest.toString(),
            "--format", "slab", "-n", "ns1");
        assertThat(exit).isEqualTo(0);

        try (SlabReader reader = new SlabReader(dest)) {
            // ns1 has 2 records in the source
            assertThat(reader.recordCount("ns1")).isEqualTo(2);
        }
    }

    @Test
    void exportWithNonexistentNamespaceReportsError() throws IOException {
        Path source = createMultiNsFile("export-empty-ns-src.slab");
        Path dest = tempDir.resolve("export-empty-ns-dst.slab");

        int exit = runSlab("export", source.toString(), "--to", dest.toString(),
            "--format", "slab", "-n", "nope");
        // Unknown namespace should fail with error
        assertThat(exit).isEqualTo(1);
    }

    // ── check on multi-namespace files ──────────────────────────────

    @Test
    void checkAcceptsMultiNamespaceFile() throws IOException {
        Path file = createMultiNsFile("check-multi-ns.slab");
        int exit = runSlab("check", file.toString());
        assertThat(exit).isEqualTo(0);
    }

    @Test
    void checkAcceptsSingleNamedNamespaceFile() throws IOException {
        Path file = tempDir.resolve("check-single-named.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write("named", 0, "x".getBytes());
        }
        int exit = runSlab("check", file.toString());
        assertThat(exit).isEqualTo(0);
    }

    // ── import format corner cases ──────────────────────────────────

    @Test
    void importEmptyTextFile() throws IOException {
        Path source = tempDir.resolve("empty.txt");
        Files.writeString(source, "");
        Path target = tempDir.resolve("empty-import.slab");
        int exit = runSlab("import", target.toString(), "--from", source.toString(),
            "--format", "text");
        assertThat(exit).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isZero();
        }
    }

    @Test
    void importTextFileWithNoTrailingNewline() throws IOException {
        Path source = tempDir.resolve("no-newline.txt");
        Files.writeString(source, "only-line");
        Path target = tempDir.resolve("no-newline.slab");
        int exit = runSlab("import", target.toString(), "--from", source.toString(),
            "--format", "text");
        assertThat(exit).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(1);
        }
    }

    @Test
    void importCsvWithQuotedNewlines() throws IOException {
        Path source = tempDir.resolve("quoted.csv");
        Files.writeString(source, "a,\"b\nc\",d\ne,f,g\n");
        Path target = tempDir.resolve("quoted.slab");
        int exit = runSlab("import", target.toString(), "--from", source.toString(),
            "--format", "csv");
        assertThat(exit).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            // First record spans the quoted newline, second is "e,f,g\n"
            assertThat(reader.recordCount()).isEqualTo(2);
        }
    }

    @Test
    void importJsonlWithBlankLines() throws IOException {
        Path source = tempDir.resolve("blanks.jsonl");
        Files.writeString(source, "{\"a\":1}\n\n{\"b\":2}\n");
        Path target = tempDir.resolve("blanks.slab");
        int exit = runSlab("import", target.toString(), "--from", source.toString(),
            "--format", "jsonl");
        assertThat(exit).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            // Blank lines are skipped
            assertThat(reader.recordCount()).isEqualTo(2);
        }
    }

    @Test
    void importJsonlWithInvalidJsonFails() throws IOException {
        Path source = tempDir.resolve("invalid.jsonl");
        Files.writeString(source, "{\"valid\":1}\nnot json\n");
        Path target = tempDir.resolve("invalid-jsonl.slab");
        int exit = runSlab("import", target.toString(), "--from", source.toString(),
            "--format", "jsonl");
        assertThat(exit).isEqualTo(1);
    }

    @Test
    void importCstringsWithNullTerminators() throws IOException {
        Path source = tempDir.resolve("cstrings.dat");
        Files.write(source, new byte[]{'h', 'e', 'l', 'l', 'o', 0, 'w', 'o', 'r', 'l', 'd', 0});
        Path target = tempDir.resolve("cstrings.slab");
        int exit = runSlab("import", target.toString(), "--from", source.toString(),
            "--format", "cstrings");
        assertThat(exit).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(2);
        }
    }

    @Test
    void importJsonSinglePrimitive() throws IOException {
        Path source = tempDir.resolve("primitive.json");
        Files.writeString(source, "42");
        Path target = tempDir.resolve("primitive.slab");
        int exit = runSlab("import", target.toString(), "--from", source.toString(),
            "--format", "json");
        assertThat(exit).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(1);
        }
    }

    @Test
    void importJsonMultipleTopLevelValues() throws IOException {
        Path source = tempDir.resolve("multi.json");
        Files.writeString(source, "{\"a\":1} {\"b\":2} [3]");
        Path target = tempDir.resolve("multi.slab");
        int exit = runSlab("import", target.toString(), "--from", source.toString(),
            "--format", "json");
        assertThat(exit).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(3);
        }
    }

    // ── import auto-detection ───────────────────────────────────────

    @Test
    void importAutoDetectsTextByExtension() throws IOException {
        Path source = tempDir.resolve("auto.txt");
        Files.writeString(source, "line1\nline2\n");
        Path target = tempDir.resolve("auto-text.slab");
        int exit = runSlab("import", target.toString(), "--from", source.toString());
        assertThat(exit).isEqualTo(0);
    }

    @Test
    void importAutoDetectsJsonByExtension() throws IOException {
        Path source = tempDir.resolve("auto.json");
        Files.writeString(source, "[1, 2, 3]");
        Path target = tempDir.resolve("auto-json.slab");
        int exit = runSlab("import", target.toString(), "--from", source.toString());
        assertThat(exit).isEqualTo(0);
    }

    @Test
    void importAutoDetectsJsonlByExtension() throws IOException {
        Path source = tempDir.resolve("auto.jsonl");
        Files.writeString(source, "{\"a\":1}\n{\"b\":2}\n");
        Path target = tempDir.resolve("auto-jsonl.slab");
        int exit = runSlab("import", target.toString(), "--from", source.toString());
        assertThat(exit).isEqualTo(0);
    }

    @Test
    void importAutoDetectsCsvByExtension() throws IOException {
        Path source = tempDir.resolve("auto.csv");
        Files.writeString(source, "a,b\n1,2\n");
        Path target = tempDir.resolve("auto-csv.slab");
        int exit = runSlab("import", target.toString(), "--from", source.toString());
        assertThat(exit).isEqualTo(0);
    }

    @Test
    void importAutoDetectsYamlByYmlExtension() throws IOException {
        Path source = tempDir.resolve("auto.yml");
        Files.writeString(source, "key: value\n");
        Path target = tempDir.resolve("auto-yml.slab");
        int exit = runSlab("import", target.toString(), "--from", source.toString());
        assertThat(exit).isEqualTo(0);
    }

    @Test
    void importAutoDetectsCstringsByContentScan() throws IOException {
        Path source = tempDir.resolve("auto.dat");
        Files.write(source, new byte[]{'a', 'b', 0, 'c', 'd', 0});
        Path target = tempDir.resolve("auto-cstrings.slab");
        int exit = runSlab("import", target.toString(), "--from", source.toString());
        assertThat(exit).isEqualTo(0);
    }

    // ── import --start-ordinal ──────────────────────────────────────

    @Test
    void importWithCustomStartOrdinal() throws IOException {
        Path source = tempDir.resolve("start-ord.txt");
        Files.writeString(source, "a\nb\n");
        Path target = tempDir.resolve("start-ord.slab");
        int exit = runSlab("import", target.toString(), "--from", source.toString(),
            "--format", "text", "--start-ordinal", "100");
        assertThat(exit).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.get(100)).isPresent();
            assertThat(reader.get(101)).isPresent();
            assertThat(reader.get(0)).isEmpty();
        }
    }

    @Test
    void importAppendModeAutoStartOrdinal() throws IOException {
        Path target = tempDir.resolve("append-auto-ord.slab");
        try (SlabWriter writer = new SlabWriter(target, 4096)) {
            writer.write(0, "existing".getBytes());
        }

        Path source = tempDir.resolve("append-src.txt");
        Files.writeString(source, "new\n");

        int exit = runSlab("import", target.toString(), "--from", source.toString(),
            "--format", "text", "--append");
        assertThat(exit).isEqualTo(0);

        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(2);
            assertThat(reader.get(0)).isPresent();
            assertThat(reader.get(1)).isPresent();
        }
    }

    // ── import --force and --append conflicts ───────────────────────

    @Test
    void importRejectsExistingTargetWithoutForce() throws IOException {
        Path target = tempDir.resolve("existing-target.slab");
        try (SlabWriter writer = new SlabWriter(target, 4096)) {
            writer.write(0, "x".getBytes());
        }

        Path source = tempDir.resolve("force-src.txt");
        Files.writeString(source, "data\n");

        int exit = runSlab("import", target.toString(), "--from", source.toString(),
            "--format", "text");
        assertThat(exit).isEqualTo(1);
    }

    @Test
    void importOverwritesWithForce() throws IOException {
        Path target = tempDir.resolve("force-target.slab");
        try (SlabWriter writer = new SlabWriter(target, 4096)) {
            writer.write(0, "old".getBytes());
        }

        Path source = tempDir.resolve("force-overwrite-src.txt");
        Files.writeString(source, "new\n");

        int exit = runSlab("import", target.toString(), "--from", source.toString(),
            "--format", "text", "-f");
        assertThat(exit).isEqualTo(0);

        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(1);
        }
    }

    @Test
    void importFromMissingSourceFails() {
        Path target = tempDir.resolve("no-src-target.slab");
        int exit = runSlab("import", target.toString(), "--from",
            tempDir.resolve("nope.txt").toString(), "--format", "text");
        assertThat(exit).isEqualTo(1);
    }

    // ── export format corner cases ──────────────────────────────────

    @Test
    void exportSlabRequiresToOption() throws IOException {
        Path source = createTestFile("export-no-to.slab", 0, 3);
        int exit = runSlab("export", source.toString(), "--format", "slab");
        assertThat(exit).isEqualTo(1);
    }

    @Test
    void exportRejectsHexAndBase64Together() throws IOException {
        Path source = createTestFile("export-mutex.slab", 0, 3);
        int exit = runSlab("export", source.toString(), "--as-hex", "--as-base64");
        assertThat(exit).isEqualTo(1);
    }

    @Test
    void exportRejectsExistingOutputWithoutForce() throws IOException {
        Path source = createTestFile("export-exists-src.slab", 0, 3);
        Path dest = tempDir.resolve("existing.txt");
        Files.writeString(dest, "existing");
        int exit = runSlab("export", source.toString(), "--to", dest.toString(), "--format", "text");
        assertThat(exit).isEqualTo(1);
    }

    @Test
    void exportWithRangeFilter() throws IOException {
        Path source = createTestFile("export-range.slab", 0, 10);
        Path dest = tempDir.resolve("ranged.slab");
        int exit = runSlab("export", source.toString(), "--to", dest.toString(),
            "--format", "slab", "--range", "[2,5]");
        assertThat(exit).isEqualTo(0);
        try (SlabReader reader = new SlabReader(dest)) {
            assertThat(reader.recordCount()).isEqualTo(4); // ordinals 2,3,4,5
        }
    }

    @Test
    void exportWithRangeExclusiveEnd() throws IOException {
        Path source = createTestFile("export-range-exc.slab", 0, 10);
        Path dest = tempDir.resolve("ranged-exc.slab");
        int exit = runSlab("export", source.toString(), "--to", dest.toString(),
            "--format", "slab", "--range", "[2,5)");
        assertThat(exit).isEqualTo(0);
        try (SlabReader reader = new SlabReader(dest)) {
            assertThat(reader.recordCount()).isEqualTo(3); // ordinals 2,3,4
        }
    }

    @Test
    void exportAllFormatsSucceed() throws IOException {
        Path source = createTestFile("export-all-fmts.slab", 0, 2);
        for (String fmt : new String[]{"raw", "text", "hex", "utf8", "ascii", "cstrings", "csv", "tsv", "yaml"}) {
            Path dest = tempDir.resolve("export-" + fmt + ".out");
            int exit = runSlab("export", source.toString(), "--to", dest.toString(),
                "--format", fmt, "-f");
            assertThat(exit).as("format %s", fmt).isEqualTo(0);
        }
    }

    // ── get format corner cases ─────────────────────────────────────

    @Test
    void getAllFormatsSucceed() throws IOException {
        Path file = createTestFile("get-all-fmts.slab", 0, 2);
        for (String fmt : new String[]{"ascii", "hex", "raw", "utf8", "json", "jsonl"}) {
            int exit = runSlab("get", file.toString(), "-o", "0", "-f", fmt);
            assertThat(exit).as("format %s", fmt).isEqualTo(0);
        }
    }

    @Test
    void getMultipleOrdinalsWithMixOfFoundAndMissing() throws IOException {
        Path file = createTestFile("get-mix.slab", 0, 3);
        // 0,1,2 exist; 5,10 do not
        int exit = runSlab("get", file.toString(), "-o", "0,5,1,10,2", "-f", "utf8");
        assertThat(exit).isEqualTo(1); // some missing
    }

    @Test
    void getAllMissingOrdinals() throws IOException {
        Path file = createTestFile("get-all-missing.slab", 0, 3);
        int exit = runSlab("get", file.toString(), "-o", "99,100,101", "-f", "utf8");
        assertThat(exit).isEqualTo(1);
    }

    // ── rewrite corner cases ────────────────────────────────────────

    @Test
    void rewriteEmptyFile() throws IOException {
        Path source = tempDir.resolve("rewrite-empty-src.slab");
        try (SlabWriter writer = new SlabWriter(source, 4096)) {
            // no records
        }
        Path dest = tempDir.resolve("rewrite-empty-dst.slab");
        int exit = runSlab("rewrite", source.toString(), dest.toString());
        assertThat(exit).isEqualTo(0);
        try (SlabReader reader = new SlabReader(dest)) {
            assertThat(reader.recordCount()).isZero();
        }
    }

    @Test
    void rewriteChangesPageSize() throws IOException {
        Path source = createTestFile("rewrite-ps-src.slab", 0, 20);
        Path dest = tempDir.resolve("rewrite-ps-dst.slab");
        int exit = runSlab("rewrite", source.toString(), dest.toString(), "--page-size", "512");
        assertThat(exit).isEqualTo(0);
        try (SlabReader reader = new SlabReader(dest)) {
            assertThat(reader.recordCount()).isEqualTo(20);
        }
    }

    // ── check corner cases ──────────────────────────────────────────

    @Test
    void checkOnTruncatedFile() throws IOException {
        Path file = tempDir.resolve("truncated.slab");
        Files.write(file, new byte[10]);
        int exit = runSlab("check", file.toString());
        assertThat(exit).isNotEqualTo(0);
    }

    @Test
    void checkOnZeroByteFile() throws IOException {
        Path file = tempDir.resolve("zero.slab");
        Files.write(file, new byte[0]);
        int exit = runSlab("check", file.toString());
        assertThat(exit).isNotEqualTo(0);
    }

    @Test
    void checkOnAppendedFile() throws IOException {
        Path file = createTestFile("check-appended.slab", 0, 5);
        try (SlabWriter writer = SlabWriter.openForAppend(file, 4096)) {
            writer.write(5, "appended".getBytes());
        }
        int exit = runSlab("check", file.toString());
        assertThat(exit).isEqualTo(0);
    }

    // ── import slab-to-slab ─────────────────────────────────────────

    @Test
    void importSlabToSlab() throws IOException {
        Path source = createTestFile("slab2slab-src.slab", 0, 5);
        Path target = tempDir.resolve("slab2slab-dst.slab");
        int exit = runSlab("import", target.toString(), "--from", source.toString(),
            "--format", "slab");
        assertThat(exit).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(5);
        }
    }

    @Test
    void importSlabAutoDetectedByExtension() throws IOException {
        Path source = createTestFile("auto.slab", 0, 3);
        Path target = tempDir.resolve("auto-slab-import.slab");
        int exit = runSlab("import", target.toString(), "--from", source.toString());
        assertThat(exit).isEqualTo(0);
    }

    // ── unknown subcommand ──────────────────────────────────────────

    @Test
    void unknownSubcommandShowsError() {
        int exit = runSlab("boguscmd");
        assertThat(exit).isNotEqualTo(0);
    }

    // ── YAML import with multiple documents ─────────────────────────

    @Test
    void importYamlMultipleDocuments() throws IOException {
        Path source = tempDir.resolve("multi.yaml");
        Files.writeString(source, "---\nkey1: val1\n---\nkey2: val2\n");
        Path target = tempDir.resolve("multi-yaml.slab");
        int exit = runSlab("import", target.toString(), "--from", source.toString(),
            "--format", "yaml");
        assertThat(exit).isEqualTo(0);
        try (SlabReader reader = new SlabReader(target)) {
            assertThat(reader.recordCount()).isEqualTo(2);
        }
    }

    // ── import with --page-size option ──────────────────────────────

    @Test
    void importWithCustomPageSize() throws IOException {
        Path source = tempDir.resolve("custom-ps.txt");
        Files.writeString(source, "a\nb\nc\n");
        Path target = tempDir.resolve("custom-ps.slab");
        int exit = runSlab("import", target.toString(), "--from", source.toString(),
            "--format", "text", "--page-size", "512");
        assertThat(exit).isEqualTo(0);
    }

    // ── export from multi-ns file without --namespace ───────────────

    @Test
    void exportFromMultiNsFileWithoutNamespaceShowsHint() throws IOException {
        Path source = createMultiNsFile("export-multi-default.slab");
        Path dest = tempDir.resolve("export-multi-default-out.slab");

        // No -n flag on a multi-ns file: should prompt user to choose a namespace
        int exit = runSlab("export", source.toString(), "--to", dest.toString(),
            "--format", "slab");
        assertThat(exit).isEqualTo(1);
    }
}
