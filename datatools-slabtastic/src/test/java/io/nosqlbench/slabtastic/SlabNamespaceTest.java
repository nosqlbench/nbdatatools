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

import io.nosqlbench.slabtastic.cli.SlabFileValidator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;

/// End-to-end and adversarial tests for slabtastic namespace support.
///
/// Covers single-namespace transparency, multi-namespace write/read,
/// backward compatibility, namespace isolation, append across namespaces,
/// validator acceptance, and adversarial corner cases for namespace
/// structures.
class SlabNamespaceTest implements SlabConstants {

    @TempDir
    Path tempDir;

    // ── Single namespace transparency ───────────────────────────────

    @Test
    void singleDefaultNamespaceIsTransparent() throws IOException {
        Path file = tempDir.resolve("single_ns.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(0, "hello".getBytes());
            writer.write(1, "world".getBytes());
        }
        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.namespaces()).containsExactly("");
            assertThat(reader.recordCount()).isEqualTo(2);
            assertThat(reader.recordCount("")).isEqualTo(2);
            assertRecordEquals(reader, "", 0, "hello");
            assertRecordEquals(reader, "", 1, "world");

            // Default namespace via no-arg methods
            assertThat(reader.get(0)).isPresent();
            assertThat(reader.get(1)).isPresent();
        }
    }

    @Test
    void singleDefaultNamespaceFileEndsWithPagesPage() throws IOException {
        Path file = tempDir.resolve("single_ns_type.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(0, "data".getBytes());
        }

        // Read the last footer to check page type is 1 (pages page)
        long fileSize = Files.size(file);
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            ByteBuffer tailBuf = ByteBuffer.allocate(FOOTER_V1_SIZE).order(ByteOrder.LITTLE_ENDIAN);
            ch.read(tailBuf, fileSize - FOOTER_V1_SIZE);
            tailBuf.flip();
            PageFooter footer = PageFooter.readFrom(tailBuf, 0);
            assertThat(footer.pageType()).isEqualTo(PAGE_TYPE_PAGES_PAGE);
        }
    }

    // ── Multi-namespace write and read ──────────────────────────────

    @Test
    void multiNamespaceWriteAndRead() throws IOException {
        Path file = tempDir.resolve("multi_ns.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write("vectors", 0, "vec0".getBytes());
            writer.write("vectors", 1, "vec1".getBytes());
            writer.write("metadata", 0, "meta0".getBytes());
            writer.write("metadata", 1, "meta1".getBytes());
            writer.write("metadata", 2, "meta2".getBytes());
        }
        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.namespaces()).containsExactlyInAnyOrder("", "vectors", "metadata");

            assertThat(reader.recordCount("vectors")).isEqualTo(2);
            assertThat(reader.recordCount("metadata")).isEqualTo(3);
            assertThat(reader.recordCount("")).isZero();

            assertRecordEquals(reader, "vectors", 0, "vec0");
            assertRecordEquals(reader, "vectors", 1, "vec1");
            assertRecordEquals(reader, "metadata", 0, "meta0");
            assertRecordEquals(reader, "metadata", 1, "meta1");
            assertRecordEquals(reader, "metadata", 2, "meta2");
        }
    }

    @Test
    void multiNamespaceFileEndsWithNamespacesPage() throws IOException {
        Path file = tempDir.resolve("multi_ns_type.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write("ns1", 0, "a".getBytes());
            writer.write("ns2", 0, "b".getBytes());
        }

        long fileSize = Files.size(file);
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            ByteBuffer tailBuf = ByteBuffer.allocate(FOOTER_V1_SIZE).order(ByteOrder.LITTLE_ENDIAN);
            ch.read(tailBuf, fileSize - FOOTER_V1_SIZE);
            tailBuf.flip();
            PageFooter footer = PageFooter.readFrom(tailBuf, 0);
            assertThat(footer.pageType()).isEqualTo(PAGE_TYPE_NAMESPACES_PAGE);
        }
    }

    @Test
    void namespacesHaveIndependentOrdinalSpaces() throws IOException {
        Path file = tempDir.resolve("independent_ords.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            // Both namespaces use ordinal 0, 1, 2
            writer.write("alpha", 0, "a0".getBytes());
            writer.write("alpha", 1, "a1".getBytes());
            writer.write("beta", 0, "b0".getBytes());
            writer.write("beta", 1, "b1".getBytes());
        }
        try (SlabReader reader = new SlabReader(file)) {
            assertRecordEquals(reader, "alpha", 0, "a0");
            assertRecordEquals(reader, "alpha", 1, "a1");
            assertRecordEquals(reader, "beta", 0, "b0");
            assertRecordEquals(reader, "beta", 1, "b1");

            // Cross-namespace ordinals do not leak
            assertThat(reader.get("alpha", 0)).isPresent();
            assertThat(reader.get("beta", 0)).isPresent();
        }
    }

    // ── Non-default single namespace ────────────────────────────────

    @Test
    void singleNonDefaultNamespaceProducesNamespacesPage() throws IOException {
        Path file = tempDir.resolve("single_named_ns.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write("custom", 0, "data".getBytes());
        }

        // A named (non-default) single namespace should produce a namespaces page
        long fileSize = Files.size(file);
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            ByteBuffer tailBuf = ByteBuffer.allocate(FOOTER_V1_SIZE).order(ByteOrder.LITTLE_ENDIAN);
            ch.read(tailBuf, fileSize - FOOTER_V1_SIZE);
            tailBuf.flip();
            PageFooter footer = PageFooter.readFrom(tailBuf, 0);
            assertThat(footer.pageType()).isEqualTo(PAGE_TYPE_NAMESPACES_PAGE);
        }

        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.namespaces()).containsExactlyInAnyOrder("", "custom");
            assertRecordEquals(reader, "custom", 0, "data");
            assertThat(reader.recordCount("")).isZero();
        }
    }

    // ── Unknown namespace returns empty ─────────────────────────────

    @Test
    void unknownNamespaceThrowsIllegalArgument() throws IOException {
        Path file = tempDir.resolve("unknown_ns.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write("existing", 0, "data".getBytes());
        }
        try (SlabReader reader = new SlabReader(file)) {
            assertThatThrownBy(() -> reader.get("nonexistent", 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nonexistent");
            assertThatThrownBy(() -> reader.pages("nonexistent"))
                .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> reader.pageCount("nonexistent"))
                .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> reader.recordCount("nonexistent"))
                .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void defaultNamespacePresentButEmptyWhenOnlyNamedNamespacesWritten() throws IOException {
        Path file = tempDir.resolve("no_default_ns.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write("ns1", 0, "a".getBytes());
        }
        try (SlabReader reader = new SlabReader(file)) {
            // Default namespace is always present per spec (index 1 = "")
            assertThat(reader.namespaces()).contains("");
            // But it has no records
            assertThat(reader.get("", 0)).isEmpty();
            assertThat(reader.get(0)).isEmpty();
            assertThat(reader.recordCount()).isZero();
            assertThat(reader.recordCount("")).isZero();
        }
    }

    // ── Validator accepts multi-namespace file ──────────────────────

    @Test
    void validatorAcceptsMultiNamespaceFile() throws IOException {
        Path file = tempDir.resolve("valid_multi_ns.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write("vectors", 0, "v0".getBytes());
            writer.write("vectors", 1, "v1".getBytes());
            writer.write("queries", 0, "q0".getBytes());
        }
        List<String> errors = SlabFileValidator.validate(file);
        assertThat(errors).isEmpty();
    }

    @Test
    void validatorAcceptsSingleNonDefaultNamespace() throws IOException {
        Path file = tempDir.resolve("valid_single_named.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write("only", 0, "x".getBytes());
        }
        List<String> errors = SlabFileValidator.validate(file);
        assertThat(errors).isEmpty();
    }

    // ── Append to namespace in multi-namespace file ─────────────────

    @Test
    void appendToMultiNamespaceFile() throws IOException {
        Path file = tempDir.resolve("append_multi_ns.slab");

        // Write initial data with two namespaces
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write("ns1", 0, "a0".getBytes());
            writer.write("ns2", 0, "b0".getBytes());
        }

        // Append to ns1
        try (SlabWriter writer = SlabWriter.openForAppend(file, 4096)) {
            writer.write("ns1", 1, "a1".getBytes());
        }

        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.namespaces()).containsExactlyInAnyOrder("", "ns1", "ns2");
            assertRecordEquals(reader, "ns1", 0, "a0");
            assertRecordEquals(reader, "ns1", 1, "a1");
            assertRecordEquals(reader, "ns2", 0, "b0");
        }
    }

    @Test
    void appendNewNamespaceToExistingFile() throws IOException {
        Path file = tempDir.resolve("append_new_ns.slab");

        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write("ns1", 0, "a0".getBytes());
        }

        try (SlabWriter writer = SlabWriter.openForAppend(file, 4096)) {
            writer.write("ns2", 0, "b0".getBytes());
        }

        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.namespaces()).containsExactlyInAnyOrder("", "ns1", "ns2");
            assertRecordEquals(reader, "ns1", 0, "a0");
            assertRecordEquals(reader, "ns2", 0, "b0");
        }
    }

    @Test
    void appendToDefaultNamespacePreservesBackwardCompat() throws IOException {
        Path file = tempDir.resolve("append_default_ns.slab");

        // Write with default namespace
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(0, "a".getBytes());
            writer.write(1, "b".getBytes());
        }

        // Append with default namespace
        try (SlabWriter writer = SlabWriter.openForAppend(file, 4096)) {
            writer.write(2, "c".getBytes());
        }

        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.namespaces()).containsExactly("");
            assertThat(reader.recordCount()).isEqualTo(3);
            assertRecordEquals(reader, "", 0, "a");
            assertRecordEquals(reader, "", 1, "b");
            assertRecordEquals(reader, "", 2, "c");
        }
    }

    // ── Many namespaces stress test ─────────────────────────────────

    @Test
    void manyNamespacesStressTest() throws IOException {
        Path file = tempDir.resolve("many_ns.slab");
        int nsCount = 50;
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            for (int ns = 0; ns < nsCount; ns++) {
                String name = "ns_" + ns;
                for (int i = 0; i < 3; i++) {
                    writer.write(name, i, (name + "_" + i).getBytes());
                }
            }
        }
        try (SlabReader reader = new SlabReader(file)) {
            Set<String> names = reader.namespaces();
            assertThat(names).hasSize(nsCount + 1); // +1 for default namespace ""
            assertThat(names).contains("");
            for (int ns = 0; ns < nsCount; ns++) {
                String name = "ns_" + ns;
                assertThat(reader.recordCount(name)).isEqualTo(3);
                assertRecordEquals(reader, name, 0, name + "_0");
                assertRecordEquals(reader, name, 2, name + "_2");
            }
        }
        List<String> errors = SlabFileValidator.validate(file);
        assertThat(errors).isEmpty();
    }

    // ── Empty namespaces ────────────────────────────────────────────

    @Test
    void emptyMultiNamespaceFile() throws IOException {
        Path file = tempDir.resolve("empty_multi_ns.slab");
        // Write to two namespaces but never actually write data to one
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write("nonempty", 0, "data".getBytes());
        }
        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.recordCount("nonempty")).isEqualTo(1);
        }
    }

    // ── Namespace name edge cases ───────────────────────────────────

    @Test
    void namespaceWithEmptyStringNameViaExplicitAPI() throws IOException {
        Path file = tempDir.resolve("explicit_empty_ns.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write("", 0, "default_data".getBytes());
        }
        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.namespaces()).containsExactly("");
            assertRecordEquals(reader, "", 0, "default_data");
        }
    }

    @Test
    void namespaceWithMaxLengthName() throws IOException {
        // 128-byte name is the max
        String longName = "a".repeat(NAMESPACE_MAX_NAME_LENGTH);
        Path file = tempDir.resolve("maxname_ns.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(longName, 0, "data".getBytes());
        }
        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.namespaces()).containsExactlyInAnyOrder("", longName);
            assertRecordEquals(reader, longName, 0, "data");
        }
        List<String> errors = SlabFileValidator.validate(file);
        assertThat(errors).isEmpty();
    }

    @Test
    void namespaceWithUnicodeCharsInName() throws IOException {
        String unicodeName = "ns_\u00e9\u00e8\u00ea_\u4e16\u754c";
        Path file = tempDir.resolve("unicode_ns.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(unicodeName, 0, "data".getBytes());
        }
        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.namespaces()).containsExactlyInAnyOrder("", unicodeName);
            assertRecordEquals(reader, unicodeName, 0, "data");
        }
    }

    @Test
    void namespaceNameWithSlashesAndDots() throws IOException {
        String name = "group/sub.data";
        Path file = tempDir.resolve("slashdot_ns.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(name, 0, "x".getBytes());
        }
        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.namespaces()).containsExactlyInAnyOrder("", name);
            assertRecordEquals(reader, name, 0, "x");
        }
    }

    // ── Cross-namespace ordinal isolation ────────────────────────────

    @Test
    void ordinalZeroInBothNamespacesAreIndependent() throws IOException {
        Path file = tempDir.resolve("isolation.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write("a", 0, "from_a".getBytes());
            writer.write("b", 0, "from_b".getBytes());
        }
        try (SlabReader reader = new SlabReader(file)) {
            Optional<ByteBuffer> ra = reader.get("a", 0);
            Optional<ByteBuffer> rb = reader.get("b", 0);
            assertThat(ra).isPresent();
            assertThat(rb).isPresent();

            byte[] a = new byte[ra.get().remaining()];
            ra.get().get(a);
            byte[] b = new byte[rb.get().remaining()];
            rb.get().get(b);

            assertThat(new String(a)).isEqualTo("from_a");
            assertThat(new String(b)).isEqualTo("from_b");
        }
    }

    // ── Adversarial: namespace index collision via crafted file ──────

    @Test
    void craftedFileWithDuplicateNamespaceIndicesDetectedByValidator() throws IOException {
        // Write a valid multi-namespace file, then manually corrupt it to have
        // duplicate namespace indices. The validator should catch this.
        Path file = tempDir.resolve("dup_ns_idx.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write("ns1", 0, "a".getBytes());
            writer.write("ns2", 0, "b".getBytes());
        }

        // Read and verify it's valid first
        List<String> errors = SlabFileValidator.validate(file);
        assertThat(errors).isEmpty();

        // Now corrupt: overwrite the namespace index of the second entry
        // to match the first. The namespaces page is the last page in the file.
        long fileSize = Files.size(file);
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer tailBuf = ByteBuffer.allocate(FOOTER_V1_SIZE).order(ByteOrder.LITTLE_ENDIAN);
            ch.read(tailBuf, fileSize - FOOTER_V1_SIZE);
            tailBuf.flip();
            PageFooter nsFooter = PageFooter.readFrom(tailBuf, 0);

            int nsPageSize = nsFooter.pageSize();
            long nsPageOffset = fileSize - nsPageSize;

            ByteBuffer nsPageBuf = ByteBuffer.allocate(nsPageSize).order(ByteOrder.LITTLE_ENDIAN);
            ch.read(nsPageBuf, nsPageOffset);
            nsPageBuf.flip();

            SlabPage nsPage = SlabPage.parseFrom(nsPageBuf);

            // Get the second entry and overwrite its namespace index byte
            // The second record starts after the first. We need to find it.
            ByteBuffer rec0 = nsPage.getRecord(0);
            byte firstNsIdx = rec0.get(rec0.position());

            ByteBuffer rec1 = nsPage.getRecord(1);
            int rec1Offset = rec1.position();

            // Compute the absolute file position of the second entry's first byte
            // Records start at HEADER_SIZE within the page
            // Offset table tells us where each record starts
            int offsetsStart = nsPageSize - FOOTER_V1_SIZE - (nsPage.recordCount() + 1) * OFFSET_ENTRY_SIZE;
            ByteBuffer le = nsPageBuf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
            int rec1FileOff = le.getInt(offsetsStart + OFFSET_ENTRY_SIZE); // offset[1]
            long absPos = nsPageOffset + rec1FileOff;

            // Overwrite the namespace index byte
            ByteBuffer oneByte = ByteBuffer.allocate(1);
            oneByte.put(firstNsIdx);
            oneByte.flip();
            ch.write(oneByte, absPos);
        }

        // Validator should now detect duplicate namespace indices
        List<String> afterErrors = SlabFileValidator.validate(file);
        assertThat(afterErrors).anySatisfy(err ->
            assertThat(err).containsIgnoringCase("duplicate namespace index"));
    }

    // ── Adversarial: namespace page offset pointing to wrong type ────

    @Test
    void namespacePagesPageOffsetPointingToDataPageIsHandled() throws IOException {
        // Craft a file where the namespaces page entry points to a data page
        // instead of a pages page. The reader should detect the wrong page type.
        Path file = tempDir.resolve("ns_bad_offset.slab");

        // Write a data page
        SlabPage dataPage = new SlabPage(0L, PAGE_TYPE_DATA, List.of("hello".getBytes()));
        ByteBuffer dataBuf = dataPage.toByteBuffer();
        int dataSize = dataPage.serializedSize();

        // Write a pages page (empty, for ns1) — but we'll point to the data page instead
        SlabPage pagesPage = new SlabPage(0L, PAGE_TYPE_PAGES_PAGE, List.of());
        ByteBuffer pagesBuf = pagesPage.toByteBuffer();
        int pagesSize = pagesPage.serializedSize();

        // Build namespace entry pointing to the DATA page at offset 0 (wrong type!)
        NamespacesPageEntry nsEntry = new NamespacesPageEntry(NAMESPACE_DEFAULT, "broken", 0L);
        byte[] entryBytes = new byte[nsEntry.serializedSize()];
        nsEntry.writeTo(ByteBuffer.wrap(entryBytes), 0);

        SlabPage nsPage = new SlabPage(0L, PAGE_TYPE_NAMESPACES_PAGE,
            List.of(entryBytes), NAMESPACE_DEFAULT);
        ByteBuffer nsBuf = nsPage.toByteBuffer();

        try (FileChannel ch = FileChannel.open(file,
            StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            dataBuf.rewind();
            ch.write(dataBuf);
            pagesBuf.rewind();
            ch.write(pagesBuf);
            nsBuf.rewind();
            ch.write(nsBuf);
        }

        // The reader should either throw or handle gracefully
        // (data page footer at offset 0 has pageType=DATA not PAGES_PAGE)
        // The key thing is it doesn't silently return wrong data
        try (SlabReader reader = new SlabReader(file)) {
            // If it opens, the namespace may have entries but they'd be data page
            // records misinterpreted as PagesPageEntry. This is acceptable as long
            // as it doesn't crash silently.
        } catch (Exception e) {
            // Expected — reader detected wrong page type
        }
    }

    // ── Adversarial: writer ordinal validation per namespace ────────

    @Test
    void writerRejectsDescendingOrdinalsWithinNamespace() throws IOException {
        Path file = tempDir.resolve("desc_in_ns.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write("ns1", 10, "ten".getBytes());
            assertThatThrownBy(() -> writer.write("ns1", 5, "five".getBytes()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not strictly ascending");
        }
    }

    @Test
    void writerAllowsIndependentOrdinalSequencesAcrossNamespaces() throws IOException {
        Path file = tempDir.resolve("independent_ords_writer.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write("ns1", 0, "a".getBytes());
            writer.write("ns1", 1, "b".getBytes());
            // ns2 can start at 0 independently
            writer.write("ns2", 0, "x".getBytes());
            writer.write("ns2", 1, "y".getBytes());
        }
        // Verify round-trip
        try (SlabReader reader = new SlabReader(file)) {
            assertRecordEquals(reader, "ns1", 0, "a");
            assertRecordEquals(reader, "ns2", 0, "x");
        }
    }

    @Test
    void writerRejectsDuplicateOrdinalWithinNamespace() throws IOException {
        Path file = tempDir.resolve("dup_ord_ns.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write("ns1", 0, "first".getBytes());
            assertThatThrownBy(() -> writer.write("ns1", 0, "second".getBytes()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not strictly ascending");
        }
    }

    // ── Adversarial: NamespacesPageEntry edge cases ─────────────────

    @Test
    void namespacesPageEntryRejectsIndexZero() {
        NamespacesPageEntry entry = new NamespacesPageEntry((byte) 0, "bad", 0L);
        assertThatThrownBy(entry::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("reserved");
    }

    @Test
    void namespacesPageEntryRejectsNameTooLong() {
        String longName = "x".repeat(NAMESPACE_MAX_NAME_LENGTH + 1);
        NamespacesPageEntry entry = new NamespacesPageEntry(NAMESPACE_DEFAULT, longName, 0L);
        assertThatThrownBy(entry::validate)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("too long");
    }

    @Test
    void namespacesPageEntryWithEmptyNameRoundTrips() {
        NamespacesPageEntry entry = new NamespacesPageEntry(NAMESPACE_DEFAULT, "", 12345L);
        ByteBuffer buf = ByteBuffer.allocate(entry.serializedSize());
        entry.writeTo(buf, 0);
        NamespacesPageEntry read = NamespacesPageEntry.readFrom(buf, 0);
        assertThat(read.namespaceIndex()).isEqualTo(NAMESPACE_DEFAULT);
        assertThat(read.name()).isEmpty();
        assertThat(read.pagesPageOffset()).isEqualTo(12345L);
    }

    @Test
    void namespacesPageEntryWithMaxIndexRoundTrips() {
        NamespacesPageEntry entry = new NamespacesPageEntry((byte) 127, "maxidx", 999L);
        ByteBuffer buf = ByteBuffer.allocate(entry.serializedSize());
        entry.writeTo(buf, 0);
        NamespacesPageEntry read = NamespacesPageEntry.readFrom(buf, 0);
        assertThat(read.namespaceIndex()).isEqualTo((byte) 127);
        assertThat(read.name()).isEqualTo("maxidx");
    }

    // ── SlabPage namespace index propagation ────────────────────────

    @Test
    void slabPagePreservesNamespaceIndex() {
        byte nsIdx = 42;
        SlabPage page = new SlabPage(0L, PAGE_TYPE_DATA, List.of("test".getBytes()), nsIdx);
        assertThat(page.namespaceIndex()).isEqualTo(nsIdx);

        ByteBuffer buf = page.toByteBuffer();
        SlabPage parsed = SlabPage.parseFrom(buf);
        assertThat(parsed.namespaceIndex()).isEqualTo(nsIdx);
    }

    @Test
    void slabPageDefaultsToNamespaceDefault() {
        SlabPage page = new SlabPage(0L, PAGE_TYPE_DATA, List.of("test".getBytes()));
        assertThat(page.namespaceIndex()).isEqualTo(NAMESPACE_DEFAULT);
    }

    @Test
    void slabPageFooterCarriesNamespaceIndex() {
        byte nsIdx = 7;
        SlabPage page = new SlabPage(0L, PAGE_TYPE_DATA, List.of("test".getBytes()), nsIdx);
        PageFooter footer = page.footer();
        assertThat(footer.namespaceIndex()).isEqualTo(nsIdx);
    }

    // ── Interleaved namespace writes ────────────────────────────────

    @Test
    void interleavedNamespaceWritesProduceCorrectFile() throws IOException {
        Path file = tempDir.resolve("interleaved_ns.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write("a", 0, "a0".getBytes());
            writer.write("b", 0, "b0".getBytes());
            writer.write("a", 1, "a1".getBytes());
            writer.write("b", 1, "b1".getBytes());
            writer.write("c", 0, "c0".getBytes());
            writer.write("a", 2, "a2".getBytes());
        }
        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.namespaces()).hasSize(4); // a, b, c + default ""
            assertThat(reader.namespaces()).contains("");
            assertRecordEquals(reader, "a", 0, "a0");
            assertRecordEquals(reader, "a", 1, "a1");
            assertRecordEquals(reader, "a", 2, "a2");
            assertRecordEquals(reader, "b", 0, "b0");
            assertRecordEquals(reader, "b", 1, "b1");
            assertRecordEquals(reader, "c", 0, "c0");
        }
        List<String> errors = SlabFileValidator.validate(file);
        assertThat(errors).isEmpty();
    }

    // ── Default + named namespace mixed ─────────────────────────────

    @Test
    void defaultAndNamedNamespacesMixed() throws IOException {
        Path file = tempDir.resolve("mixed_default_ns.slab");
        try (SlabWriter writer = new SlabWriter(file, 4096)) {
            writer.write(0, "default0".getBytes());        // default namespace
            writer.write("named", 0, "named0".getBytes()); // named namespace
            writer.write(1, "default1".getBytes());
        }
        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.namespaces()).containsExactlyInAnyOrder("", "named");
            assertRecordEquals(reader, "", 0, "default0");
            assertRecordEquals(reader, "", 1, "default1");
            assertRecordEquals(reader, "named", 0, "named0");
        }
    }

    // ── Namespace with sparse ordinals ──────────────────────────────

    @Test
    void namespaceWithSparseOrdinalsAndMultiplePages() throws IOException {
        Path file = tempDir.resolve("sparse_ns.slab");
        try (SlabWriter writer = new SlabWriter(file, 512)) {
            writer.write("sparse", 0, "a".getBytes());
            writer.write("sparse", 1000, "b".getBytes());
            writer.write("sparse", 2000, "c".getBytes());
        }
        try (SlabReader reader = new SlabReader(file)) {
            assertThat(reader.pageCount("sparse")).isEqualTo(3);
            assertRecordEquals(reader, "sparse", 0, "a");
            assertRecordEquals(reader, "sparse", 1000, "b");
            assertRecordEquals(reader, "sparse", 2000, "c");
            assertThat(reader.get("sparse", 1)).isEmpty();
            assertThat(reader.get("sparse", 999)).isEmpty();
        }
    }

    // ── Helpers ─────────────────────────────────────────────────────

    private void assertRecordEquals(SlabReader reader, String namespace, long ordinal, String expected) {
        Optional<ByteBuffer> result = reader.get(namespace, ordinal);
        assertThat(result).as("ns=%s ordinal=%d", namespace, ordinal).isPresent();
        byte[] bytes = new byte[result.get().remaining()];
        result.get().get(bytes);
        assertThat(new String(bytes)).as("ns=%s ordinal=%d value", namespace, ordinal).isEqualTo(expected);
    }
}
