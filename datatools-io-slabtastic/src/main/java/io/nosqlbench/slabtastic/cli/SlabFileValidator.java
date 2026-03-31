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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

/// Reusable structural validation for slabtastic files.
///
/// Performs the same checks as the `slab check` command but returns
/// error messages as a list rather than printing them. This allows
/// pre-flight validation in commands like `slab append` and
/// `slab export`.
///
/// Supports both single-namespace files (pages page at EOF, type 1) and
/// multi-namespace files (namespaces page at EOF, type 3).
public final class SlabFileValidator implements SlabConstants {

    private SlabFileValidator() {}

    /// Validates a slabtastic file and returns all error messages found.
    ///
    /// An empty list means the file is structurally valid. A non-empty
    /// list contains one message per error or fatal condition.
    ///
    /// @param file the path to the slabtastic file
    /// @return a list of error messages (empty if valid)
    public static List<String> validate(Path file) {
        List<String> errors = new ArrayList<>();

        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            long fileSize = ch.size();

            if (fileSize < FOOTER_V1_SIZE) {
                errors.add("File too small to contain a footer (" + fileSize + " bytes)");
                return errors;
            }

            // Tail footer
            ByteBuffer tailBuf = readAt(ch, fileSize - FOOTER_V1_SIZE, FOOTER_V1_SIZE);
            PageFooter tailFooter = PageFooter.readFrom(tailBuf, 0);

            try {
                tailFooter.validate();
            } catch (IllegalStateException e) {
                errors.add("Tail footer invalid: " + e.getMessage());
                return errors;
            }

            if (tailFooter.pageType() == PAGE_TYPE_PAGES_PAGE) {
                validateSingleNamespaceFile(ch, fileSize, tailFooter, errors);
            } else if (tailFooter.pageType() == PAGE_TYPE_NAMESPACES_PAGE) {
                validateMultiNamespaceFile(ch, fileSize, tailFooter, errors);
            } else {
                errors.add("File does not end with a pages page or namespaces page (type="
                    + tailFooter.pageType() + ")");
                return errors;
            }

            // Pass 2: Forward traversal
            Map<Long, ForwardPage> forwardPages = forwardTraverse(ch, fileSize, errors);

            // Pass 3: Cross-check forward traversal against index
            if (forwardPages != null) {
                crossCheck(ch, fileSize, tailFooter, forwardPages, errors);
            }
        } catch (IOException e) {
            errors.add("Cannot read file: " + e.getMessage());
        }

        return errors;
    }

    /// Validates a slabtastic file and throws if it is not structurally valid.
    ///
    /// @param file the path to the slabtastic file
    /// @throws IllegalStateException if validation finds any errors
    public static void requireValid(Path file) {
        List<String> errors = validate(file);
        if (!errors.isEmpty()) {
            throw new IllegalStateException(
                "Slab file validation failed for " + file + ":\n  " + String.join("\n  ", errors));
        }
    }

    /// Validates a single-namespace file (pages page at EOF).
    private static void validateSingleNamespaceFile(FileChannel ch, long fileSize,
                                                     PageFooter pagesFooter,
                                                     List<String> errors) throws IOException {
        int pagesPageSize = pagesFooter.pageSize();
        long pagesPageOffset = fileSize - pagesPageSize;

        validatePagesPage(ch, fileSize, pagesPageOffset, pagesPageSize, "", errors);
    }

    /// Validates a multi-namespace file (namespaces page at EOF).
    private static void validateMultiNamespaceFile(FileChannel ch, long fileSize,
                                                    PageFooter nsFooter,
                                                    List<String> errors) throws IOException {
        int nsPageSize = nsFooter.pageSize();
        long nsPageOffset = fileSize - nsPageSize;

        if (nsPageOffset < 0) {
            errors.add("Namespaces-page pageSize (" + nsPageSize
                + ") exceeds file size (" + fileSize + ")");
            return;
        }

        // Validate namespaces page header
        ByteBuffer nsHeaderBuf = readAt(ch, nsPageOffset, HEADER_SIZE);
        try {
            PageHeader nsHeader = PageHeader.readFrom(nsHeaderBuf, 0);
            if (nsHeader.pageSize() != nsPageSize) {
                errors.add("Namespaces-page header size (" + nsHeader.pageSize()
                    + ") != footer size (" + nsPageSize + ")");
            }
        } catch (IllegalStateException e) {
            errors.add("Namespaces-page header: " + e.getMessage());
        }

        // Parse namespaces page
        ByteBuffer nsPageBuf = readAt(ch, nsPageOffset, nsPageSize);
        SlabPage nsPage;
        try {
            nsPage = SlabPage.parseFrom(nsPageBuf);
        } catch (Exception e) {
            errors.add("Cannot parse namespaces page: " + e.getMessage());
            return;
        }

        // Parse entries
        List<NamespacesPageEntry> nsEntries = new ArrayList<>();
        for (int i = 0; i < nsPage.recordCount(); i++) {
            ByteBuffer rec = nsPage.getRecord(i);
            byte[] recBytes = new byte[rec.remaining()];
            rec.get(recBytes);
            try {
                NamespacesPageEntry entry = NamespacesPageEntry.readFrom(ByteBuffer.wrap(recBytes), 0);
                entry.validate();
                nsEntries.add(entry);
            } catch (Exception e) {
                errors.add("Namespace entry " + i + ": " + e.getMessage());
            }
        }

        // Check for duplicate namespace indices
        Set<Byte> seenIndices = new HashSet<>();
        for (int i = 0; i < nsEntries.size(); i++) {
            byte idx = nsEntries.get(i).namespaceIndex();
            if (!seenIndices.add(idx)) {
                errors.add("Duplicate namespace index " + idx + " at entry " + i);
            }
        }

        // Validate each namespace's pages page and data pages
        // Collect all data page regions across all namespaces for overlap checking
        List<long[]> allRegions = new ArrayList<>();

        for (int nsIdx = 0; nsIdx < nsEntries.size(); nsIdx++) {
            NamespacesPageEntry nsEntry = nsEntries.get(nsIdx);
            long ppOffset = nsEntry.pagesPageOffset();

            if (ppOffset < 0 || ppOffset + HEADER_SIZE > fileSize) {
                errors.add("Namespace '" + nsEntry.name() + "': pages-page offset "
                    + ppOffset + " is out of range");
                continue;
            }

            try {
                ByteBuffer ppHeaderBuf = readAt(ch, ppOffset, HEADER_SIZE);
                PageHeader ppHeader = PageHeader.readFrom(ppHeaderBuf, 0);
                int ppSize = ppHeader.pageSize();

                String prefix = "Namespace '" + nsEntry.name() + "': ";
                List<String> nsErrors = new ArrayList<>();
                validatePagesPageContent(ch, fileSize, ppOffset, ppSize, nsErrors);
                for (String err : nsErrors) {
                    errors.add(prefix + err);
                }
            } catch (Exception e) {
                errors.add("Namespace '" + nsEntry.name() + "': " + e.getMessage());
            }
        }
    }

    /// Validates a pages page and its referenced data pages.
    private static void validatePagesPage(FileChannel ch, long fileSize,
                                           long pagesPageOffset, int pagesPageSize,
                                           String context, List<String> errors) throws IOException {
        if (pagesPageOffset < 0) {
            errors.add("Pages-page pageSize (" + pagesPageSize
                + ") exceeds file size (" + fileSize + ")");
            return;
        }

        // Pages page header
        ByteBuffer pagesHeaderBuf = readAt(ch, pagesPageOffset, HEADER_SIZE);
        try {
            PageHeader pagesHeader = PageHeader.readFrom(pagesHeaderBuf, 0);
            if (pagesHeader.pageSize() != pagesPageSize) {
                errors.add("Pages-page header size (" + pagesHeader.pageSize()
                    + ") != footer size (" + pagesPageSize + ")");
            }
        } catch (IllegalStateException e) {
            errors.add("Pages-page header: " + e.getMessage());
        }

        validatePagesPageContent(ch, fileSize, pagesPageOffset, pagesPageSize, errors);
    }

    /// Validates the content of a pages page (entries, data pages).
    private static void validatePagesPageContent(FileChannel ch, long fileSize,
                                                  long pagesPageOffset, int pagesPageSize,
                                                  List<String> errors) throws IOException {
        // Parse pages page
        ByteBuffer pagesPageBuf = readAt(ch, pagesPageOffset, pagesPageSize);
        SlabPage pagesPage;
        try {
            pagesPage = SlabPage.parseFrom(pagesPageBuf);
        } catch (Exception e) {
            errors.add("Cannot parse pages page: " + e.getMessage());
            return;
        }

        // Extract index entries
        List<PagesPageEntry> entries = new ArrayList<>();
        for (int i = 0; i < pagesPage.recordCount(); i++) {
            ByteBuffer rec = pagesPage.getRecord(i);
            byte[] recBytes = new byte[rec.remaining()];
            rec.get(recBytes);
            entries.add(PagesPageEntry.readFrom(ByteBuffer.wrap(recBytes), 0));
        }

        // Duplicate start ordinals
        Set<Long> seenOrdinals = new HashSet<>();
        for (int i = 0; i < entries.size(); i++) {
            long ord = entries.get(i).startOrdinal();
            if (!seenOrdinals.add(ord)) {
                errors.add("Index: duplicate start ordinal " + ord + " at entry " + i);
            }
        }

        // Entries pointing past EOF
        for (int i = 0; i < entries.size(); i++) {
            PagesPageEntry entry = entries.get(i);
            if (entry.fileOffset() < 0) {
                errors.add("Index entry " + i + ": negative file offset " + entry.fileOffset());
            } else if (entry.fileOffset() + HEADER_SIZE > pagesPageOffset) {
                errors.add("Index entry " + i + ": file offset " + entry.fileOffset()
                    + " + header extends into/past pages page at " + pagesPageOffset);
            }
        }

        // Overlapping page regions
        record PageRegion(long start, long end, int index) {}
        List<PageRegion> regions = new ArrayList<>();
        for (int i = 0; i < entries.size(); i++) {
            PagesPageEntry entry = entries.get(i);
            long off = entry.fileOffset();
            if (off >= 0 && off + HEADER_SIZE <= fileSize) {
                try {
                    ByteBuffer hBuf = readAt(ch, off, HEADER_SIZE);
                    PageHeader h = PageHeader.readFrom(hBuf, 0);
                    regions.add(new PageRegion(off, off + h.pageSize(), i));
                } catch (Exception ignored) {
                    // Will be reported in per-page checks
                }
            }
        }
        for (int i = 0; i < regions.size(); i++) {
            for (int j = i + 1; j < regions.size(); j++) {
                PageRegion a = regions.get(i);
                PageRegion b = regions.get(j);
                if (a.start < b.end && b.start < a.end) {
                    errors.add("Index: page " + a.index + " [" + a.start + ", " + a.end
                        + ") overlaps page " + b.index + " [" + b.start + ", " + b.end + ")");
                }
            }
        }

        // Ordinal range in index entries
        for (int i = 0; i < entries.size(); i++) {
            long ord = entries.get(i).startOrdinal();
            if (ord < MIN_ORDINAL || ord > MAX_ORDINAL) {
                errors.add("Index entry " + i + ": start ordinal " + ord
                    + " outside valid range [" + MIN_ORDINAL + ", " + MAX_ORDINAL + "]");
            }
        }

        // Validate each data page
        for (int i = 0; i < entries.size(); i++) {
            validateDataPage(ch, fileSize, pagesPageOffset, entries.get(i), i, errors);
        }
    }

    private static void validateDataPage(FileChannel ch, long fileSize, long pagesPageOffset,
                                          PagesPageEntry entry, int pageIndex, List<String> errors) {
        long offset = entry.fileOffset();

        if (offset < 0 || offset + HEADER_SIZE > fileSize) {
            errors.add("Page " + pageIndex + ": file offset " + offset + " is out of range");
            return;
        }

        try {
            ByteBuffer headerBuf = readAt(ch, offset, HEADER_SIZE);
            PageHeader header = PageHeader.readFrom(headerBuf, 0);
            int pageSize = header.pageSize();

            if (offset + pageSize > fileSize) {
                errors.add("Page " + pageIndex + ": extends beyond file end (offset=" + offset
                    + ", size=" + pageSize + ", fileSize=" + fileSize + ")");
                return;
            }

            if (offset + pageSize > pagesPageOffset) {
                errors.add("Page " + pageIndex + ": extends into pages page region (offset=" + offset
                    + ", size=" + pageSize + ", pagesPageAt=" + pagesPageOffset + ")");
            }

            if (pageSize < MIN_PAGE_SIZE) {
                errors.add("Page " + pageIndex + ": page size " + pageSize + " below minimum " + MIN_PAGE_SIZE);
            }

            if (pageSize % PAGE_ALIGNMENT != 0) {
                errors.add("Page " + pageIndex + ": page size " + pageSize + " not aligned to " + PAGE_ALIGNMENT);
            }

            ByteBuffer footerBuf = readAt(ch, offset + pageSize - FOOTER_V1_SIZE, FOOTER_V1_SIZE);
            PageFooter footer = PageFooter.readFrom(footerBuf, 0);

            try {
                footer.validate();
            } catch (IllegalStateException e) {
                errors.add("Page " + pageIndex + ": footer invalid: " + e.getMessage());
            }

            if (header.pageSize() != footer.pageSize()) {
                errors.add("Page " + pageIndex + ": header size (" + header.pageSize()
                    + ") != footer size (" + footer.pageSize() + ")");
            }

            if (footer.pageType() != PAGE_TYPE_DATA) {
                errors.add("Page " + pageIndex + ": expected data page type but got " + footer.pageType());
            }

            if (footer.startOrdinal() != entry.startOrdinal()) {
                errors.add("Page " + pageIndex + ": index ordinal (" + entry.startOrdinal()
                    + ") != footer ordinal (" + footer.startOrdinal() + ")");
            }

            long footerOrd = footer.startOrdinal();
            if (footerOrd < MIN_ORDINAL || footerOrd > MAX_ORDINAL) {
                errors.add("Page " + pageIndex + ": footer ordinal " + footerOrd
                    + " outside valid range [" + MIN_ORDINAL + ", " + MAX_ORDINAL + "]");
            }

            // Offset table
            int recCount = footer.recordCount();
            long offsetsStartLong = (long) pageSize - FOOTER_V1_SIZE - (long)(recCount + 1) * OFFSET_ENTRY_SIZE;

            if (offsetsStartLong < HEADER_SIZE) {
                errors.add("Page " + pageIndex + ": offset table overflows page (recordCount=" + recCount
                    + ", offsetsStart=" + offsetsStartLong + ", headerSize=" + HEADER_SIZE + ")");
            } else {
                int offsetsStart = (int) offsetsStartLong;
                ByteBuffer pageBuf = readAt(ch, offset, pageSize);
                ByteBuffer le = pageBuf.duplicate().order(ByteOrder.LITTLE_ENDIAN);

                int prevOff = le.getInt(offsetsStart);
                if (prevOff != HEADER_SIZE) {
                    errors.add("Page " + pageIndex + ": first offset is " + prevOff + ", expected " + HEADER_SIZE);
                }

                for (int j = 1; j <= recCount; j++) {
                    int off = le.getInt(offsetsStart + j * OFFSET_ENTRY_SIZE);
                    if (off < prevOff) {
                        errors.add("Page " + pageIndex + ": offset[" + j + "]=" + off
                            + " < offset[" + (j - 1) + "]=" + prevOff + " (non-monotonic)");
                    }
                    if (off > offsetsStart) {
                        errors.add("Page " + pageIndex + ": offset[" + j + "]=" + off
                            + " extends into offset table at " + offsetsStart);
                    }
                    if (off < HEADER_SIZE) {
                        errors.add("Page " + pageIndex + ": offset[" + j + "]=" + off
                            + " is before header end at " + HEADER_SIZE);
                    }
                    prevOff = off;
                }
            }
        } catch (Exception e) {
            errors.add("Page " + pageIndex + ": " + e.getMessage());
        }
    }

    /// A page discovered during forward traversal.
    record ForwardPage(long offset, int pageSize, byte pageType) {}

    /// Pass 2: walks the file from offset 0, reading each page header to
    /// determine its size, then jumping forward by that size.
    ///
    /// Returns a map of offset → ForwardPage, or null if the traversal
    /// fails fatally.
    static Map<Long, ForwardPage> forwardTraverse(FileChannel ch, long fileSize,
                                                    List<String> errors) throws IOException {
        Map<Long, ForwardPage> pages = new LinkedHashMap<>();
        long offset = 0;

        while (offset < fileSize) {
            if (offset + HEADER_SIZE > fileSize) {
                errors.add("Forward traversal: incomplete header at offset " + offset
                    + " (only " + (fileSize - offset) + " bytes remain)");
                return null;
            }

            PageHeader header;
            try {
                ByteBuffer hBuf = readAt(ch, offset, HEADER_SIZE);
                header = PageHeader.readFrom(hBuf, 0);
            } catch (IllegalStateException e) {
                errors.add("Forward traversal: bad magic at offset " + offset + ": " + e.getMessage());
                return null;
            }

            int pageSz = header.pageSize();
            if (pageSz < HEADER_SIZE + FOOTER_V1_SIZE) {
                errors.add("Forward traversal: page at offset " + offset
                    + " has implausible size " + pageSz);
                return null;
            }

            if (offset + pageSz > fileSize) {
                errors.add("Forward traversal: page at offset " + offset
                    + " extends past file end (size=" + pageSz + ", fileSize=" + fileSize + ")");
                return null;
            }

            // Read footer to get page type
            ByteBuffer fBuf = readAt(ch, offset + pageSz - FOOTER_V1_SIZE, FOOTER_V1_SIZE);
            PageFooter footer = PageFooter.readFrom(fBuf, 0);

            try {
                footer.validate();
            } catch (IllegalStateException e) {
                errors.add("Forward traversal: footer invalid at offset " + offset + ": " + e.getMessage());
            }

            if (header.pageSize() != footer.pageSize()) {
                errors.add("Forward traversal: header/footer size mismatch at offset " + offset
                    + " (header=" + header.pageSize() + ", footer=" + footer.pageSize() + ")");
            }

            pages.put(offset, new ForwardPage(offset, pageSz, footer.pageType()));
            offset += pageSz;
        }

        if (offset != fileSize) {
            errors.add("Forward traversal: traversal ended at offset " + offset
                + " but file size is " + fileSize);
        }

        // Verify that the last page is a pages page or namespaces page
        if (!pages.isEmpty()) {
            ForwardPage last = null;
            for (ForwardPage p : pages.values()) {
                last = p;
            }
            if (last != null && last.pageType() != PAGE_TYPE_PAGES_PAGE
                && last.pageType() != PAGE_TYPE_NAMESPACES_PAGE) {
                errors.add("Forward traversal: last page (offset=" + last.offset()
                    + ") is type " + last.pageType() + ", expected pages page or namespaces page");
            }
        }

        return pages;
    }

    /// Pass 3: cross-checks the forward traversal against the index entries.
    ///
    /// Verifies that every index entry points to a page found in forward
    /// traversal, and every data page in the forward traversal is referenced
    /// by the index.
    private static void crossCheck(FileChannel ch, long fileSize, PageFooter tailFooter,
                                    Map<Long, ForwardPage> forwardPages,
                                    List<String> errors) throws IOException {
        // Collect all data page offsets from the index
        Set<Long> indexedOffsets = new HashSet<>();

        if (tailFooter.pageType() == PAGE_TYPE_PAGES_PAGE) {
            int ppSize = tailFooter.pageSize();
            long ppOffset = fileSize - ppSize;
            collectIndexedOffsets(ch, ppOffset, ppSize, indexedOffsets, errors);
        } else if (tailFooter.pageType() == PAGE_TYPE_NAMESPACES_PAGE) {
            int nsSize = tailFooter.pageSize();
            long nsOffset = fileSize - nsSize;
            ByteBuffer nsBuf = readAt(ch, nsOffset, nsSize);
            try {
                SlabPage nsPage = SlabPage.parseFrom(nsBuf);
                for (int i = 0; i < nsPage.recordCount(); i++) {
                    ByteBuffer rec = nsPage.getRecord(i);
                    byte[] recBytes = new byte[rec.remaining()];
                    rec.get(recBytes);
                    NamespacesPageEntry entry = NamespacesPageEntry.readFrom(ByteBuffer.wrap(recBytes), 0);
                    long ppOffset = entry.pagesPageOffset();
                    ByteBuffer ppHdr = readAt(ch, ppOffset, HEADER_SIZE);
                    PageHeader ppHeader = PageHeader.readFrom(ppHdr, 0);
                    collectIndexedOffsets(ch, ppOffset, ppHeader.pageSize(), indexedOffsets, errors);
                }
            } catch (Exception e) {
                errors.add("Cross-check: cannot parse namespaces page: " + e.getMessage());
                return;
            }
        }

        // Collect forward-traversal data page offsets
        Set<Long> forwardDataOffsets = new HashSet<>();
        for (ForwardPage fp : forwardPages.values()) {
            if (fp.pageType() == PAGE_TYPE_DATA) {
                forwardDataOffsets.add(fp.offset());
            }
        }

        // Every indexed offset must appear in forward traversal
        for (long off : indexedOffsets) {
            if (!forwardPages.containsKey(off)) {
                errors.add("Cross-check: index references offset " + off
                    + " which was not found in forward traversal");
            }
        }

        // Every forward data page must appear in the index
        for (long off : forwardDataOffsets) {
            if (!indexedOffsets.contains(off)) {
                errors.add("Cross-check: forward traversal found data page at offset " + off
                    + " which is not referenced by any index entry (orphan page)");
            }
        }

        // Data page count must agree
        if (indexedOffsets.size() != forwardDataOffsets.size()) {
            errors.add("Cross-check: index references " + indexedOffsets.size()
                + " data pages but forward traversal found " + forwardDataOffsets.size());
        }
    }

    /// Collects all data page file offsets from a pages page.
    private static void collectIndexedOffsets(FileChannel ch, long ppOffset, int ppSize,
                                               Set<Long> offsets, List<String> errors) throws IOException {
        ByteBuffer ppBuf = readAt(ch, ppOffset, ppSize);
        try {
            SlabPage pagesPage = SlabPage.parseFrom(ppBuf);
            for (int i = 0; i < pagesPage.recordCount(); i++) {
                ByteBuffer rec = pagesPage.getRecord(i);
                byte[] recBytes = new byte[rec.remaining()];
                rec.get(recBytes);
                PagesPageEntry entry = PagesPageEntry.readFrom(ByteBuffer.wrap(recBytes), 0);
                offsets.add(entry.fileOffset());
            }
        } catch (Exception e) {
            errors.add("Cross-check: cannot parse pages page at offset " + ppOffset + ": " + e.getMessage());
        }
    }

    static ByteBuffer readAt(FileChannel ch, long position, int length) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN);
        int read = 0;
        while (read < length) {
            int n = ch.read(buf, position + read);
            if (n < 0) {
                throw new IOException("Unexpected EOF at position " + (position + read));
            }
            read += n;
        }
        buf.flip();
        return buf;
    }
}
