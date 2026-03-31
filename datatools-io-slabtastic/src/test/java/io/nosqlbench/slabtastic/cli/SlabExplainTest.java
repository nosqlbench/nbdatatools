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
import io.nosqlbench.slabtastic.SlabWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.*;

/// Tests for the `slab explain` CLI command.
class SlabExplainTest implements SlabConstants {

    @TempDir
    Path tempDir;

    /// Runs the slab CLI with the given args, capturing stdout.
    private ExplainResult runExplain(String... args) {
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        ByteArrayOutputStream stderr = new ByteArrayOutputStream();
        PrintStream oldOut = System.out;
        PrintStream oldErr = System.err;
        try {
            System.setOut(new PrintStream(stdout));
            System.setErr(new PrintStream(stderr));
            int exit = new CommandLine(new CMD_slab())
                .setCaseInsensitiveEnumValuesAllowed(true)
                .setOptionsCaseInsensitive(true)
                .execute(args);
            return new ExplainResult(exit, stdout.toString(), stderr.toString());
        } finally {
            System.setOut(oldOut);
            System.setErr(oldErr);
        }
    }

    private record ExplainResult(int exitCode, String stdout, String stderr) {}

    @Test
    void singlePageFileShowsAllSections() throws IOException {
        Path file = tempDir.resolve("single.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(0, "alpha".getBytes());
            writer.write(1, "bravo".getBytes());
            writer.write(2, "charlie".getBytes());
        }

        ExplainResult result = runExplain("explain", file.toString());
        assertThat(result.exitCode()).isEqualTo(0);
        assertThat(result.stdout()).contains("Header");
        assertThat(result.stdout()).contains("Records");
        assertThat(result.stdout()).contains("Offsets");
        assertThat(result.stdout()).contains("Footer");
        assertThat(result.stdout()).contains("0x42414C53");
        assertThat(result.stdout()).contains("alpha");
        assertThat(result.stdout()).contains("bravo");
        assertThat(result.stdout()).contains("charlie");
        assertThat(result.stdout()).contains("start_ordinal");
        assertThat(result.stdout()).contains("record_count");
    }

    @Test
    void multiPageFileRendersMultipleDiagrams() throws IOException {
        Path file = tempDir.resolve("multi.slab");
        try (SlabWriter writer = new SlabWriter(file, 512)) {
            // Use large enough records to force multiple pages
            byte[] payload = new byte[200];
            for (int i = 0; i < 5; i++) {
                for (int j = 0; j < payload.length; j++) {
                    payload[j] = (byte) (i + j);
                }
                writer.write(i, payload.clone());
            }
        }

        ExplainResult result = runExplain("explain", file.toString());
        assertThat(result.exitCode()).isEqualTo(0);
        // Should contain multiple "Page N" headers
        assertThat(result.stdout()).contains("Page 0");
        assertThat(result.stdout()).contains("Page 1");
        // Should also show the pages page
        assertThat(result.stdout()).contains("Pages page");
    }

    @Test
    void pagesFilterShowsOnlySelectedPage() throws IOException {
        Path file = tempDir.resolve("filter.slab");
        try (SlabWriter writer = new SlabWriter(file, 512)) {
            byte[] payload = new byte[200];
            for (int i = 0; i < 5; i++) {
                writer.write(i, payload);
            }
        }

        ExplainResult result = runExplain("explain", file.toString(), "--pages", "0");
        assertThat(result.exitCode()).isEqualTo(0);
        assertThat(result.stdout()).contains("Page 0");
        // Pages page is always shown
        assertThat(result.stdout()).contains("Pages page");
    }

    @Test
    void ordinalsFilterSelectsMatchingPages() throws IOException {
        Path file = tempDir.resolve("ordfilter.slab");
        try (SlabWriter writer = new SlabWriter(file, 512)) {
            // Page 0: ordinals 0-1 (200 bytes each forces at most ~1 per page with 512)
            writer.write(0, new byte[200]);
            // Page 1: ordinal 1
            writer.write(1, new byte[200]);
            // Page 2: ordinal 2
            writer.write(2, new byte[200]);
        }

        ExplainResult result = runExplain("explain", file.toString(), "--ordinals", "[0]");
        assertThat(result.exitCode()).isEqualTo(0);
        assertThat(result.stdout()).contains("Page 0");
    }

    @Test
    void namespaceFilterOnMultiNamespaceFile() throws IOException {
        Path file = tempDir.resolve("ns.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write("ns1", 0, "hello".getBytes());
            writer.write("ns1", 1, "world".getBytes());
            writer.write("ns2", 0, "foo".getBytes());
        }

        ExplainResult result = runExplain("explain", file.toString(), "-n", "ns1");
        assertThat(result.exitCode()).isEqualTo(0);
        assertThat(result.stdout()).contains("ns1");
        assertThat(result.stdout()).contains("hello");
        // Should also render namespaces page
        assertThat(result.stdout()).contains("Namespaces page");
    }

    @Test
    void pagesPageShowsDecodedEntries() throws IOException {
        Path file = tempDir.resolve("ppentries.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(0, "test".getBytes());
        }

        ExplainResult result = runExplain("explain", file.toString());
        assertThat(result.exitCode()).isEqualTo(0);
        // Pages page entries should show decoded [ordinal N → offset N]
        assertThat(result.stdout()).contains("ordinal");
        assertThat(result.stdout()).contains("\u2192"); // → arrow
        assertThat(result.stdout()).contains("offset");
    }

    @Test
    void namespacesPageShowsDecodedEntries() throws IOException {
        Path file = tempDir.resolve("nsentries.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write("myns", 0, "data".getBytes());
            writer.write("other", 0, "data2".getBytes());
        }

        ExplainResult result = runExplain("explain", file.toString(), "-n", "myns");
        assertThat(result.exitCode()).isEqualTo(0);
        assertThat(result.stdout()).contains("Namespaces page");
        assertThat(result.stdout()).contains("myns");
        assertThat(result.stdout()).contains("other");
    }

    @Test
    void missingFileProducesExitCode1() {
        ExplainResult result = runExplain("explain", tempDir.resolve("nonexistent.slab").toString());
        assertThat(result.exitCode()).isEqualTo(1);
    }

    @Test
    void outOfRangePageIndexPrintsWarning() throws IOException {
        Path file = tempDir.resolve("outofrange.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(0, "test".getBytes());
        }

        ExplainResult result = runExplain("explain", file.toString(), "--pages", "99");
        assertThat(result.exitCode()).isEqualTo(0);
        assertThat(result.stderr()).contains("out of range");
    }
}
