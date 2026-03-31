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

import io.nosqlbench.slabtastic.*;
import picocli.CommandLine;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/// Illustrates slab page layout on the console using block-diagrammatic
/// notation.
///
/// For a given page number, set of pages, ordinal range, or namespace, each
/// matching page is printed in diagrammatic form showing the page layout
/// including header, records, offsets, and footer fields.
///
/// Selection logic:
/// 1. If `--pages` is specified, only those page indices are shown.
/// 2. If `--ordinals` is specified, only pages whose ordinal range overlaps
///    the specified range are shown.
/// 3. If neither is specified, all pages are shown.
/// 4. Structural pages (pages page and namespaces page) are always shown at
///    the end.
@CommandLine.Command(
    name = "explain",
    header = "Illustrate slab page layout",
    description = "Prints block-diagrammatic views of slab page layout including header, "
        + "records, offsets, and footer fields.",
    exitCodeList = {"0: Success", "1: Error"}
)
public class CMD_slab_explain implements Callable<Integer>, SlabConstants {

    /// Creates a new instance of the explain subcommand.
    public CMD_slab_explain() {}

    private static final int DIAGRAM_WIDTH = 80;
    private static final int MAX_HEX_BYTES = 16;

    @CommandLine.Parameters(index = "0", description = "Path to the slabtastic file")
    private Path file;

    @CommandLine.Option(names = {"-p", "--pages"}, split = ",",
        description = "Comma-separated 0-based page indices to display")
    private List<Integer> pages;

    @CommandLine.Option(names = {"-o", "--ordinals"},
        converter = OrdinalRange.Converter.class,
        description = "Ordinal range filter (e.g. [0,10), 5, 0..9)")
    private OrdinalRange.Range ordinals;

    @CommandLine.Option(names = {"-n", "--namespace"}, defaultValue = "",
        description = "Namespace to operate on; required when the file has named namespaces")
    private String namespace;

    @Override
    public Integer call() {
        PrintStream out = System.out;
        PrintStream err = System.err;

        // Resolve namespace before proceeding
        try (SlabReader probe = new SlabReader(file)) {
            String resolved = NamespaceResolver.resolveForRead(namespace, probe);
            if (resolved == null) {
                err.println("Error: " + NamespaceResolver.formatNamespaceHint(probe));
                return 1;
            }
            namespace = resolved;
        } catch (Exception e) {
            err.println("Error: " + e.getMessage());
            return 1;
        }

        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            long fileSize = ch.size();

            if (fileSize < FOOTER_V1_SIZE) {
                err.println("Error: file too small (" + fileSize + " bytes)");
                return 1;
            }

            // Read the tail footer to determine file structure
            ByteBuffer tailBuf = SlabFileValidator.readAt(ch, fileSize - FOOTER_V1_SIZE, FOOTER_V1_SIZE);
            PageFooter tailFooter = PageFooter.readFrom(tailBuf, 0);
            tailFooter.validate();

            boolean isMultiNamespace = tailFooter.pageType() == PAGE_TYPE_NAMESPACES_PAGE;

            // Resolve namespace to find data pages and the pages page
            long pagesPageOffset;
            int pagesPageSize;
            long nsPageOffset = -1;
            int nsPageSize = 0;

            if (isMultiNamespace) {
                nsPageSize = tailFooter.pageSize();
                nsPageOffset = fileSize - nsPageSize;
                ByteBuffer nsPageBuf = SlabFileValidator.readAt(ch, nsPageOffset, nsPageSize);
                SlabPage nsPage = SlabPage.parseFrom(nsPageBuf);

                // Find the matching namespace entry
                NamespacesPageEntry matchedEntry = null;
                for (int i = 0; i < nsPage.recordCount(); i++) {
                    ByteBuffer rec = nsPage.getRecord(i);
                    byte[] recBytes = new byte[rec.remaining()];
                    rec.get(recBytes);
                    NamespacesPageEntry entry = NamespacesPageEntry.readFrom(ByteBuffer.wrap(recBytes), 0);
                    if (entry.name().equals(namespace)) {
                        matchedEntry = entry;
                        break;
                    }
                }
                if (matchedEntry == null) {
                    err.println("Error: namespace '" + namespace + "' not found");
                    return 1;
                }

                pagesPageOffset = matchedEntry.pagesPageOffset();
                ByteBuffer ppHeaderBuf = SlabFileValidator.readAt(ch, pagesPageOffset, HEADER_SIZE);
                PageHeader ppHeader = PageHeader.readFrom(ppHeaderBuf, 0);
                pagesPageSize = ppHeader.pageSize();
            } else {
                pagesPageSize = tailFooter.pageSize();
                pagesPageOffset = fileSize - pagesPageSize;
            }

            // Parse pages page to get data page entries
            ByteBuffer pagesPageBuf = SlabFileValidator.readAt(ch, pagesPageOffset, pagesPageSize);
            SlabPage pagesPage = SlabPage.parseFrom(pagesPageBuf);

            List<PagesPageEntry> entries = new ArrayList<>();
            for (int i = 0; i < pagesPage.recordCount(); i++) {
                ByteBuffer rec = pagesPage.getRecord(i);
                byte[] recBytes = new byte[rec.remaining()];
                rec.get(recBytes);
                entries.add(PagesPageEntry.readFrom(ByteBuffer.wrap(recBytes), 0));
            }

            // Determine which data pages to show
            List<Integer> selectedIndices = selectPages(ch, entries);

            // Render selected data pages
            for (int idx : selectedIndices) {
                PagesPageEntry entry = entries.get(idx);
                ByteBuffer headerBuf = SlabFileValidator.readAt(ch, entry.fileOffset(), HEADER_SIZE);
                PageHeader header = PageHeader.readFrom(headerBuf, 0);
                int pageSz = header.pageSize();

                ByteBuffer pageBuf = SlabFileValidator.readAt(ch, entry.fileOffset(), pageSz);
                SlabPage page = SlabPage.parseFrom(pageBuf);
                PageFooter footer = page.footer();

                String nsLabel = namespace.isEmpty() ? "(default)" : namespace;
                out.println();
                renderDataPage(out, idx, page, footer, pageBuf, pageSz, entry.fileOffset(), nsLabel);
            }

            // Render pages page
            out.println();
            renderStructuralPage(out, "Pages page", pagesPage, pagesPageBuf, pagesPageSize,
                pagesPageOffset, PAGE_TYPE_PAGES_PAGE);

            // Render namespaces page if multi-namespace
            if (isMultiNamespace) {
                ByteBuffer nsPageBuf = SlabFileValidator.readAt(ch, nsPageOffset, nsPageSize);
                SlabPage nsPage = SlabPage.parseFrom(nsPageBuf);
                out.println();
                renderStructuralPage(out, "Namespaces page", nsPage, nsPageBuf, nsPageSize,
                    nsPageOffset, PAGE_TYPE_NAMESPACES_PAGE);
            }

            return 0;
        } catch (Exception e) {
            err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    /// Selects data page indices based on --pages and --ordinals filters.
    private List<Integer> selectPages(FileChannel ch, List<PagesPageEntry> entries) throws Exception {
        if (pages != null && !pages.isEmpty()) {
            List<Integer> result = new ArrayList<>();
            for (int idx : pages) {
                if (idx >= 0 && idx < entries.size()) {
                    result.add(idx);
                } else {
                    System.err.println("Warning: page index " + idx + " out of range [0, "
                        + (entries.size() - 1) + "]");
                }
            }
            return result;
        }

        if (ordinals != null) {
            List<Integer> result = new ArrayList<>();
            for (int i = 0; i < entries.size(); i++) {
                PagesPageEntry entry = entries.get(i);
                // Read footer to get record count
                ByteBuffer headerBuf = SlabFileValidator.readAt(ch, entry.fileOffset(), HEADER_SIZE);
                PageHeader header = PageHeader.readFrom(headerBuf, 0);
                int pageSz = header.pageSize();
                ByteBuffer footerBuf = SlabFileValidator.readAt(ch,
                    entry.fileOffset() + pageSz - FOOTER_V1_SIZE, FOOTER_V1_SIZE);
                PageFooter footer = PageFooter.readFrom(footerBuf, 0);

                long pageStart = footer.startOrdinal();
                long pageEnd = pageStart + footer.recordCount(); // exclusive
                // Check if page range overlaps the ordinal range
                if (pageStart < ordinals.end() && pageEnd > ordinals.start()) {
                    result.add(i);
                }
            }
            return result;
        }

        // No filter: all pages
        List<Integer> result = new ArrayList<>(entries.size());
        for (int i = 0; i < entries.size(); i++) {
            result.add(i);
        }
        return result;
    }

    /// Renders a data page diagram.
    private void renderDataPage(PrintStream out, int pageIndex, SlabPage page,
                                PageFooter footer, ByteBuffer pageBuf, int pageSize,
                                long fileOffset, String nsLabel) {
        int recordCount = page.recordCount();
        long startOrd = page.startOrdinal();
        long endOrd = startOrd + recordCount - 1;

        out.printf("Page %d \u2014 data page, namespace: %s, %d bytes at file offset %d%n",
            pageIndex, nsLabel, pageSize, fileOffset);
        out.printf("Ordinals [%d, %d], %d records%n", startOrd, endOrd, recordCount);
        out.println();

        // Header section
        printTopBorder(out, "Header (%d bytes)".formatted(HEADER_SIZE));
        ByteBuffer le = pageBuf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        int magic = le.getInt(0);
        out.printf("\u2502 Magic: 0x%08X (\"SLAB\")    Page size: %-36d \u2502%n", magic, pageSize);

        // Records section
        int totalRecordBytes = 0;
        for (int i = 0; i < recordCount; i++) {
            ByteBuffer rec = page.getRecord(i);
            totalRecordBytes += rec.remaining();
        }
        printMidBorder(out, "Records (%d bytes, %d records)".formatted(totalRecordBytes, recordCount));
        for (int i = 0; i < recordCount; i++) {
            ByteBuffer rec = page.getRecord(i);
            byte[] bytes = new byte[rec.remaining()];
            rec.duplicate().get(bytes);
            printRecordLine(out, i, startOrd + i, bytes);
        }

        // Gap
        int offsetsStart = pageSize - FOOTER_V1_SIZE - (recordCount + 1) * OFFSET_ENTRY_SIZE;
        int recordsEnd = HEADER_SIZE + totalRecordBytes;
        int gapSize = offsetsStart - recordsEnd;
        if (gapSize > 0) {
            printMidBorder(out, "Gap (%d bytes, zero-fill)".formatted(gapSize));
        }

        // Offsets section
        int offsetEntries = recordCount + 1;
        int offsetBytes = offsetEntries * OFFSET_ENTRY_SIZE;
        printMidBorder(out, "Offsets (%d bytes, %d entries)".formatted(offsetBytes, offsetEntries));
        int[] offsets = new int[offsetEntries];
        for (int i = 0; i < offsetEntries; i++) {
            offsets[i] = le.getInt(offsetsStart + i * OFFSET_ENTRY_SIZE);
        }
        printOffsetsLine(out, offsets);

        // Footer section
        printMidBorder(out, "Footer (%d bytes)".formatted(FOOTER_V1_SIZE));
        printFooterLines(out, footer);
        printBottomBorder(out);
    }

    /// Renders a structural page (pages page or namespaces page).
    private void renderStructuralPage(PrintStream out, String title, SlabPage page,
                                      ByteBuffer pageBuf, int pageSize, long fileOffset,
                                      byte pageType) {
        int recordCount = page.recordCount();
        String typeName = pageType == PAGE_TYPE_PAGES_PAGE ? "pages page" : "namespaces page";

        out.printf("%s \u2014 %s, %d bytes at file offset %d%n", title, typeName, pageSize, fileOffset);
        out.printf("%d entries%n", recordCount);
        out.println();

        // Header
        printTopBorder(out, "Header (%d bytes)".formatted(HEADER_SIZE));
        ByteBuffer le = pageBuf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        int magic = le.getInt(0);
        out.printf("\u2502 Magic: 0x%08X (\"SLAB\")    Page size: %-36d \u2502%n", magic, pageSize);

        // Records
        int totalRecordBytes = 0;
        for (int i = 0; i < recordCount; i++) {
            ByteBuffer rec = page.getRecord(i);
            totalRecordBytes += rec.remaining();
        }
        printMidBorder(out, "Records (%d bytes, %d entries)".formatted(totalRecordBytes, recordCount));

        for (int i = 0; i < recordCount; i++) {
            ByteBuffer rec = page.getRecord(i);
            byte[] bytes = new byte[rec.remaining()];
            rec.duplicate().get(bytes);

            if (pageType == PAGE_TYPE_PAGES_PAGE) {
                // Decode as PagesPageEntry: [ordinal → offset]
                PagesPageEntry entry = PagesPageEntry.readFrom(ByteBuffer.wrap(bytes), 0);
                String decoded = "[ordinal %d \u2192 offset %d]".formatted(
                    entry.startOrdinal(), entry.fileOffset());
                String line = "\u2502 [%d] %s".formatted(i, decoded);
                out.printf("%-" + (DIAGRAM_WIDTH - 1) + "s\u2502%n", line);
            } else if (pageType == PAGE_TYPE_NAMESPACES_PAGE) {
                // Decode as NamespacesPageEntry: [index: name → offset]
                NamespacesPageEntry entry = NamespacesPageEntry.readFrom(ByteBuffer.wrap(bytes), 0);
                String decoded = "[index %d: \"%s\" \u2192 offset %d]".formatted(
                    entry.namespaceIndex(), entry.name(), entry.pagesPageOffset());
                String line = "\u2502 [%d] %s".formatted(i, decoded);
                out.printf("%-" + (DIAGRAM_WIDTH - 1) + "s\u2502%n", line);
            }
        }

        // Gap
        int offsetsStart = pageSize - FOOTER_V1_SIZE - (recordCount + 1) * OFFSET_ENTRY_SIZE;
        int recordsEnd = HEADER_SIZE + totalRecordBytes;
        int gapSize = offsetsStart - recordsEnd;
        if (gapSize > 0) {
            printMidBorder(out, "Gap (%d bytes, zero-fill)".formatted(gapSize));
        }

        // Offsets
        int offsetEntries = recordCount + 1;
        int offsetBytes = offsetEntries * OFFSET_ENTRY_SIZE;
        printMidBorder(out, "Offsets (%d bytes, %d entries)".formatted(offsetBytes, offsetEntries));
        int[] offsets = new int[offsetEntries];
        for (int i = 0; i < offsetEntries; i++) {
            offsets[i] = le.getInt(offsetsStart + i * OFFSET_ENTRY_SIZE);
        }
        printOffsetsLine(out, offsets);

        // Footer
        PageFooter footer = page.footer();
        printMidBorder(out, "Footer (%d bytes)".formatted(FOOTER_V1_SIZE));
        printFooterLines(out, footer);
        printBottomBorder(out);
    }

    /// Prints a single record line with hex preview and ASCII sidebar.
    private void printRecordLine(PrintStream out, int localIndex, long ordinal, byte[] bytes) {
        StringBuilder hex = new StringBuilder();
        int showLen = Math.min(bytes.length, MAX_HEX_BYTES);
        for (int i = 0; i < showLen; i++) {
            if (i > 0) hex.append(' ');
            hex.append(String.format("%02X", bytes[i]));
        }
        if (bytes.length > MAX_HEX_BYTES) {
            hex.append(" ...");
        }

        StringBuilder ascii = new StringBuilder("\"");
        int asciiLen = Math.min(bytes.length, MAX_HEX_BYTES);
        for (int i = 0; i < asciiLen; i++) {
            ascii.append(bytes[i] >= 0x20 && bytes[i] < 0x7F ? (char) bytes[i] : '.');
        }
        if (bytes.length > MAX_HEX_BYTES) {
            ascii.append("...");
        }
        ascii.append('"');

        // Format: │ [idx] ordinal N, M bytes: HH HH ...    "text"    │
        String prefix = "\u2502 [%d] ordinal %d, %d bytes: ".formatted(localIndex, ordinal, bytes.length);
        String hexStr = hex.toString();
        String asciiStr = ascii.toString();

        // Calculate available space: DIAGRAM_WIDTH - 1 for trailing │, - prefix length
        int available = DIAGRAM_WIDTH - 1 - prefix.length();
        // Right-align ASCII with some padding
        int hexLen = hexStr.length();
        int asciiPad = available - hexLen - asciiStr.length();
        if (asciiPad < 2) asciiPad = 2;

        String line = prefix + hexStr + " ".repeat(asciiPad) + asciiStr;
        out.printf("%-" + (DIAGRAM_WIDTH - 1) + "s\u2502%n", line);
    }

    /// Prints the offsets array in a compact line.
    private void printOffsetsLine(PrintStream out, int[] offsets) {
        StringBuilder sb = new StringBuilder("\u2502 ");
        for (int i = 0; i < offsets.length; i++) {
            if (i > 0) sb.append("  ");
            sb.append("[%d]=%d".formatted(i, offsets[i]));
            // Line wrap if getting too long
            if (sb.length() > DIAGRAM_WIDTH - 10 && i < offsets.length - 1) {
                out.printf("%-" + (DIAGRAM_WIDTH - 1) + "s\u2502%n", sb);
                sb = new StringBuilder("\u2502 ");
            }
        }
        out.printf("%-" + (DIAGRAM_WIDTH - 1) + "s\u2502%n", sb);
    }

    /// Prints footer field lines.
    private void printFooterLines(PrintStream out, PageFooter footer) {
        String typeName = switch (footer.pageType()) {
            case PAGE_TYPE_DATA -> "data";
            case PAGE_TYPE_PAGES_PAGE -> "pages";
            case PAGE_TYPE_NAMESPACES_PAGE -> "namespaces";
            default -> "unknown(" + footer.pageType() + ")";
        };

        String line1 = "\u2502 start_ordinal: %-12d record_count: %-14d page_size: %d"
            .formatted(footer.startOrdinal(), footer.recordCount(), footer.pageSize());
        out.printf("%-" + (DIAGRAM_WIDTH - 1) + "s\u2502%n", line1);

        String line2 = "\u2502 page_type: %d (%s)%s namespace_index: %-12d footer_length: %d"
            .formatted(footer.pageType(), typeName,
                " ".repeat(Math.max(1, 8 - typeName.length())),
                footer.namespaceIndex(), footer.footerLength());
        out.printf("%-" + (DIAGRAM_WIDTH - 1) + "s\u2502%n", line2);
    }

    // ── Box-drawing helpers ──────────────────────────────────────────────────

    private void printTopBorder(PrintStream out, String label) {
        // ┌─ Label ─────...─┐
        String prefix = "\u250C\u2500 " + label + " ";
        int fillLen = DIAGRAM_WIDTH - prefix.length() - 1;
        out.println(prefix + "\u2500".repeat(Math.max(0, fillLen)) + "\u2510");
    }

    private void printMidBorder(PrintStream out, String label) {
        // ├─ Label ─────...─┤
        String prefix = "\u251C\u2500 " + label + " ";
        int fillLen = DIAGRAM_WIDTH - prefix.length() - 1;
        out.println(prefix + "\u2500".repeat(Math.max(0, fillLen)) + "\u2524");
    }

    private void printBottomBorder(PrintStream out) {
        // └─────...─────────┘
        out.println("\u2514" + "\u2500".repeat(DIAGRAM_WIDTH - 2) + "\u2518");
    }
}
