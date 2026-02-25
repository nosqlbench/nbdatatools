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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutionException;

/// Reads records from a slabtastic file by global ordinal.
///
/// On construction, the reader bootstraps from the file tail: the last 16
/// bytes are read as a {@link PageFooter}, validated, and used to determine
/// the file structure. If the tail page is a pages page (type 1), the file
/// is a single-namespace file and the full pages page is loaded. If the tail
/// page is a namespaces page (type 3), the file is a multi-namespace file
/// and each namespace's pages page is loaded independently.
///
/// Subsequent {@link #get(long)} calls use O(log2 n) binary search over the
/// sorted index and read the target page on demand. The format supports
/// sparse ordinal chunks: ordinal values may not be fully contiguous between
/// the minimum and maximum ordinals in a file. To support this, `get()`
/// returns {@link Optional#empty()} for missing ordinals rather than a
/// default value.
///
/// Namespace-aware methods allow reading from specific namespaces in
/// multi-namespace files. The default (empty string) namespace is used
/// by all legacy single-namespace files and by the no-arg convenience
/// methods.
///
/// The underlying I/O is performed via {@link AsynchronousFileChannel} with
/// blocking {@code Future.get()} calls.
public class SlabReader implements AutoCloseable, SlabConstants {

    private final AsynchronousFileChannel channel;
    private final Map<String, NamespaceData> namespaces = new LinkedHashMap<>();

    /// Internal data for a single namespace within the file.
    private record NamespaceData(
        String name,
        byte index,
        List<PagesPageEntry> pageIndex,
        List<Integer> pageSizes,
        List<Integer> pageRecordCounts
    ) {}

    /// Opens a slabtastic file for reading.
    ///
    /// @param path the file to read
    /// @throws IOException if the file cannot be opened or is not a valid
    ///                     slabtastic file
    public SlabReader(Path path) throws IOException {
        this.channel = AsynchronousFileChannel.open(path, StandardOpenOption.READ);
        long fileSize = channel.size();

        if (fileSize < FOOTER_V1_SIZE) {
            channel.close();
            throw new IOException(
                "Not a slabtastic file (too small: %d bytes): %s".formatted(fileSize, path));
        }

        // Read the last 16 bytes to get the tail footer
        ByteBuffer tailBuf = readBytes(fileSize - FOOTER_V1_SIZE, FOOTER_V1_SIZE);
        PageFooter tailFooter = PageFooter.readFrom(tailBuf, 0);
        try {
            tailFooter.validate();
        } catch (IllegalStateException e) {
            channel.close();
            throw new IOException(
                "Not a valid slabtastic file: %s (%s)".formatted(path, e.getMessage()), e);
        }

        if (tailFooter.pageType() == PAGE_TYPE_PAGES_PAGE) {
            // Path A: single-namespace file (backward compatible)
            loadFromPagesPage(fileSize, tailFooter);
        } else if (tailFooter.pageType() == PAGE_TYPE_NAMESPACES_PAGE) {
            // Path B: multi-namespace file
            loadFromNamespacesPage(fileSize, tailFooter);
        } else {
            channel.close();
            throw new IOException(
                "File does not end with a pages page or namespaces page (type=%d): %s"
                    .formatted(tailFooter.pageType(), path));
        }
    }

    /// Loads a single-namespace file from its pages page.
    private void loadFromPagesPage(long fileSize, PageFooter pagesFooter) throws IOException {
        NamespaceData nsData = loadNamespaceFromPagesPage(fileSize, pagesFooter);
        namespaces.put("", nsData);
    }

    /// Loads namespace data from a pages page at the given location.
    private NamespaceData loadNamespaceFromPagesPage(long fileSize, PageFooter pagesFooter) throws IOException {
        int pagesPageSize = pagesFooter.pageSize();
        long pagesPageOffset = fileSize - pagesPageSize;
        return loadNamespaceFromPagesPageAt(pagesPageOffset, pagesPageSize, "");
    }

    /// Loads namespace data from a pages page at a specific offset.
    private NamespaceData loadNamespaceFromPagesPageAt(long pagesPageOffset, int pagesPageSize,
                                                        String name) throws IOException {
        ByteBuffer pagesBuf = readBytes(pagesPageOffset, pagesPageSize);

        SlabPage pagesPage = SlabPage.parseFrom(pagesBuf);
        int entryCount = pagesPage.recordCount();

        List<PagesPageEntry> entries = new ArrayList<>(entryCount);
        for (int i = 0; i < entryCount; i++) {
            ByteBuffer rec = pagesPage.getRecord(i);
            byte[] recBytes = new byte[rec.remaining()];
            rec.get(recBytes);
            ByteBuffer wrapped = ByteBuffer.wrap(recBytes);
            entries.add(PagesPageEntry.readFrom(wrapped, 0));
        }
        Collections.sort(entries);
        List<PagesPageEntry> sortedEntries = List.copyOf(entries);

        // Pre-read page sizes and record counts for each indexed page
        List<Integer> sizes = new ArrayList<>(sortedEntries.size());
        List<Integer> counts = new ArrayList<>(sortedEntries.size());
        for (PagesPageEntry entry : sortedEntries) {
            ByteBuffer headerBuf = readBytes(entry.fileOffset(), HEADER_SIZE);
            PageHeader header = PageHeader.readFrom(headerBuf, 0);
            int pageSz = header.pageSize();
            sizes.add(pageSz);

            ByteBuffer footerBuf = readBytes(entry.fileOffset() + pageSz - FOOTER_V1_SIZE, FOOTER_V1_SIZE);
            PageFooter footer = PageFooter.readFrom(footerBuf, 0);
            counts.add(footer.recordCount());
        }

        return new NamespaceData(name, pagesPage.namespaceIndex(),
            sortedEntries, List.copyOf(sizes), List.copyOf(counts));
    }

    /// Loads a multi-namespace file from its namespaces page.
    private void loadFromNamespacesPage(long fileSize, PageFooter nsFooter) throws IOException {
        int nsPageSize = nsFooter.pageSize();
        long nsPageOffset = fileSize - nsPageSize;
        ByteBuffer nsPageBuf = readBytes(nsPageOffset, nsPageSize);

        SlabPage nsPage = SlabPage.parseFrom(nsPageBuf);
        int entryCount = nsPage.recordCount();

        for (int i = 0; i < entryCount; i++) {
            ByteBuffer rec = nsPage.getRecord(i);
            byte[] recBytes = new byte[rec.remaining()];
            rec.get(recBytes);
            NamespacesPageEntry nsEntry = NamespacesPageEntry.readFrom(ByteBuffer.wrap(recBytes), 0);

            // Read this namespace's pages page
            long ppOffset = nsEntry.pagesPageOffset();
            ByteBuffer ppFooterBuf = readBytes(ppOffset, HEADER_SIZE);
            PageHeader ppHeader = PageHeader.readFrom(ppFooterBuf, 0);
            int ppSize = ppHeader.pageSize();

            NamespaceData nsData = loadNamespaceFromPagesPageAt(ppOffset, ppSize, nsEntry.name());
            namespaces.put(nsEntry.name(), nsData);
        }
    }

    /// Returns the set of all namespace names in this file.
    ///
    /// @return the namespace names; a single-namespace file returns a set
    ///         containing only the empty string
    public Set<String> namespaces() {
        return Collections.unmodifiableSet(namespaces.keySet());
    }

    /// Retrieves the record at the given global ordinal in the specified
    /// namespace.
    ///
    /// @param namespace the namespace to search
    /// @param ordinal   the global ordinal to look up
    /// @return the record bytes, or empty if the ordinal is not present
    /// @throws IllegalArgumentException if the namespace is not present in
    ///                                  the file
    public Optional<ByteBuffer> get(String namespace, long ordinal) {
        NamespaceData nsData = requireNamespace(namespace);
        return getFromNamespace(nsData, ordinal);
    }

    /// Retrieves the record at the given global ordinal in the default
    /// namespace.
    ///
    /// If the file does not contain a default namespace (i.e. it only
    /// contains named namespaces), this method returns empty rather than
    /// throwing.
    ///
    /// @param ordinal the global ordinal to look up
    /// @return the record bytes, or empty if the ordinal is not present
    public Optional<ByteBuffer> get(long ordinal) {
        NamespaceData nsData = namespaces.get("");
        if (nsData == null) {
            return Optional.empty();
        }
        return getFromNamespace(nsData, ordinal);
    }

    /// Returns summaries of all data pages in the specified namespace,
    /// sorted by ordinal.
    ///
    /// @param namespace the namespace to query
    /// @return page summaries for the namespace
    /// @throws IllegalArgumentException if the namespace is not present in
    ///                                  the file
    public List<PageSummary> pages(String namespace) {
        NamespaceData nsData = requireNamespace(namespace);
        return buildPageSummaries(nsData);
    }

    /// Returns summaries of all data pages in the default namespace,
    /// sorted by ordinal.
    ///
    /// If the file does not contain a default namespace, returns an empty
    /// list rather than throwing.
    public List<PageSummary> pages() {
        NamespaceData nsData = namespaces.get("");
        return nsData != null ? buildPageSummaries(nsData) : List.of();
    }

    /// Returns the number of data pages in the specified namespace.
    ///
    /// @param namespace the namespace to query
    /// @return the page count
    /// @throws IllegalArgumentException if the namespace is not present in
    ///                                  the file
    public int pageCount(String namespace) {
        NamespaceData nsData = requireNamespace(namespace);
        return nsData.pageIndex().size();
    }

    /// Returns the number of data pages indexed in the default namespace.
    ///
    /// If the file does not contain a default namespace, returns 0 rather
    /// than throwing.
    public int pageCount() {
        NamespaceData nsData = namespaces.get("");
        return nsData != null ? nsData.pageIndex().size() : 0;
    }

    /// Returns the total number of records across all data pages in the
    /// specified namespace.
    ///
    /// @param namespace the namespace to query
    /// @return the record count
    /// @throws IllegalArgumentException if the namespace is not present in
    ///                                  the file
    public long recordCount(String namespace) {
        NamespaceData nsData = requireNamespace(namespace);
        long total = 0;
        for (int count : nsData.pageRecordCounts()) {
            total += count;
        }
        return total;
    }

    /// Returns the total number of records across all data pages in the
    /// default namespace.
    ///
    /// If the file does not contain a default namespace, returns 0 rather
    /// than throwing.
    public long recordCount() {
        NamespaceData nsData = namespaces.get("");
        if (nsData == null) return 0;
        long total = 0;
        for (int count : nsData.pageRecordCounts()) {
            total += count;
        }
        return total;
    }

    /// Summary of a single data page in the file.
    ///
    /// @param startOrdinal the first ordinal in the page
    /// @param recordCount  the number of records in the page
    /// @param pageSize     the page size in bytes
    /// @param fileOffset   the byte offset of this page within the file
    public record PageSummary(long startOrdinal, int recordCount, int pageSize, long fileOffset) {}

    /// Returns the file size in bytes.
    public long fileSize() {
        try {
            return channel.size();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    /// Returns the namespace data for the given name, or throws if it is
    /// not present in this file.
    ///
    /// @param namespace the namespace name to look up
    /// @return the namespace data
    /// @throws IllegalArgumentException if the namespace is not present
    private NamespaceData requireNamespace(String namespace) {
        NamespaceData nsData = namespaces.get(namespace);
        if (nsData == null) {
            throw new IllegalArgumentException(
                "Namespace not present in file: \"%s\" (available: %s)"
                    .formatted(namespace, namespaces.keySet()));
        }
        return nsData;
    }

    /// Retrieves a record from a specific namespace's data.
    private Optional<ByteBuffer> getFromNamespace(NamespaceData nsData, long ordinal) {
        int pageIdx = findPageIndex(nsData.pageIndex(), ordinal);
        if (pageIdx < 0) {
            return Optional.empty();
        }

        PagesPageEntry entry = nsData.pageIndex().get(pageIdx);
        int recCount = nsData.pageRecordCounts().get(pageIdx);
        long localIndexLong = ordinal - entry.startOrdinal();

        if (localIndexLong < 0 || localIndexLong >= recCount) {
            return Optional.empty();
        }
        int localIndex = (int) localIndexLong;

        int pageSz = nsData.pageSizes().get(pageIdx);
        ByteBuffer pageBuf = readBytes(entry.fileOffset(), pageSz);
        SlabPage page = SlabPage.parseFrom(pageBuf);
        return Optional.of(page.getRecord(localIndex));
    }

    /// Builds page summaries from namespace data.
    private List<PageSummary> buildPageSummaries(NamespaceData nsData) {
        List<PageSummary> result = new ArrayList<>(nsData.pageIndex().size());
        for (int i = 0; i < nsData.pageIndex().size(); i++) {
            PagesPageEntry entry = nsData.pageIndex().get(i);
            result.add(new PageSummary(
                entry.startOrdinal(), nsData.pageRecordCounts().get(i),
                nsData.pageSizes().get(i), entry.fileOffset()));
        }
        return List.copyOf(result);
    }

    /// Binary-searches the index for the page that could contain the given
    /// ordinal (largest startOrdinal ≤ ordinal).
    private int findPageIndex(List<PagesPageEntry> index, long ordinal) {
        int lo = 0;
        int hi = index.size() - 1;
        int result = -1;

        while (lo <= hi) {
            int mid = lo + (hi - lo) / 2;
            long midOrdinal = index.get(mid).startOrdinal();
            if (midOrdinal <= ordinal) {
                result = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return result;
    }

    private ByteBuffer readBytes(long position, int length) {
        ByteBuffer buf = ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN);
        try {
            int totalRead = 0;
            while (totalRead < length) {
                int read = channel.read(buf, position + totalRead).get();
                if (read < 0) {
                    throw new IOException("Unexpected end of file at position " + (position + totalRead));
                }
                totalRead += read;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UncheckedIOException(new IOException("Read interrupted", e));
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException ioe) {
                throw new UncheckedIOException(ioe);
            }
            throw new RuntimeException("Read failed", cause);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        buf.flip();
        return buf;
    }
}
