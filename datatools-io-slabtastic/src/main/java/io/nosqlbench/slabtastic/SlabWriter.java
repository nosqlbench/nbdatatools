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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Sequentially writes records into a slabtastic file, with optional
/// namespace support.
///
/// Records are buffered in memory and flushed to disk as complete pages when
/// the preferred page size would be exceeded. On {@link #close()}, any
/// remaining buffered records are flushed as a final data page, followed by
/// the pages page that indexes all data pages.
///
/// Writers are required to flush buffers at slab boundaries. This ensures
/// that the pages page is always up to date with the current state of the
/// file across systems which do not share state via a VFS subsystem. Each
/// data page flush calls `channel.force(false)` immediately after writing.
///
/// For creating new files, use the constructor. For appending to an existing
/// slabtastic file, use {@link #openForAppend(Path, int)}. In append mode,
/// new data pages are written after the old pages page (which becomes dead
/// data), and a new pages page is written at close that references both old
/// and new data pages. The last pages page in a slabtastic file is always
/// the authoritative pages page, so this approach avoids overwriting the
/// old pages page (which could be a destructive operation should it fail).
///
/// ## Namespace support
///
/// Use {@link #write(String, long, byte[])} to write records to a named
/// namespace. The no-arg {@link #write(long, byte[])} writes to the default
/// namespace (empty string). When a file contains only the default namespace,
/// it is written as a backward-compatible single-namespace file (pages page
/// at EOF). When multiple namespaces are used, each namespace gets its own
/// pages page and a namespaces page (type 3) is written at EOF.
///
/// Example — creating a new file:
/// ```java
/// try (var writer = new SlabWriter(path, 65536)) {
///     writer.write(0, "hello".getBytes());
///     writer.write(1, "world".getBytes());
/// }
/// ```
///
/// Example — multi-namespace file:
/// ```java
/// try (var writer = new SlabWriter(path, 65536)) {
///     writer.write("vectors", 0, "vec0".getBytes());
///     writer.write("metadata", 0, "meta0".getBytes());
/// }
/// ```
///
/// Example — appending to an existing file:
/// ```java
/// try (var writer = SlabWriter.openForAppend(path, 65536)) {
///     writer.write(100, "appended".getBytes());
/// }
/// ```
///
/// Example — using configuration:
/// ```java
/// var config = new SlabWriter.SlabWriterConfig(65536, 512, false, Integer.MAX_VALUE);
/// try (var writer = new SlabWriter(path, config)) {
///     writer.write(0, "hello".getBytes());
/// }
/// ```
public class SlabWriter implements AutoCloseable, SlabConstants {

    /// Configuration for slab file layout parameters.
    ///
    /// @param preferredPageSize the target page size in bytes; must be at
    ///                          least {@code minPageSize} and a multiple of
    ///                          {@code minPageSize}
    /// @param minPageSize       the minimum page size and alignment
    ///                          granularity in bytes; must be at least 512
    ///                          and a multiple of 512
    /// @param pageAlignment     when true, pages are padded to multiples of
    ///                          {@code minPageSize}; when false, pages are
    ///                          rounded up to multiples of
    ///                          {@link SlabConstants#PAGE_ALIGNMENT} (512)
    /// @param maxPageSize       the maximum allowed page size in bytes;
    ///                          must be at least {@code preferredPageSize}.
    ///                          An aligned page that exceeds this limit
    ///                          causes an error during flush.
    public record SlabWriterConfig(int preferredPageSize, int minPageSize, boolean pageAlignment,
                                   int maxPageSize) {

        /// Default configuration: 65536-byte preferred page size, 512-byte
        /// minimum, no page alignment, no max page size limit.
        public static final SlabWriterConfig DEFAULT = new SlabWriterConfig(65536, 512, false, Integer.MAX_VALUE);

        /// Convenience constructor without maxPageSize (defaults to
        /// {@link Integer#MAX_VALUE}).
        ///
        /// @param preferredPageSize the target page size in bytes
        /// @param minPageSize       the minimum page size in bytes
        /// @param pageAlignment     whether to pad to minPageSize multiples
        public SlabWriterConfig(int preferredPageSize, int minPageSize, boolean pageAlignment) {
            this(preferredPageSize, minPageSize, pageAlignment, Integer.MAX_VALUE);
        }

        /// Validates all configuration constraints.
        public SlabWriterConfig {
            if (minPageSize < MIN_PAGE_SIZE) {
                throw new IllegalArgumentException(
                    "Minimum page size %d is below absolute minimum %d".formatted(minPageSize, MIN_PAGE_SIZE));
            }
            if (minPageSize % PAGE_ALIGNMENT != 0) {
                throw new IllegalArgumentException(
                    "Minimum page size %d is not a multiple of %d".formatted(minPageSize, PAGE_ALIGNMENT));
            }
            if (preferredPageSize < minPageSize) {
                throw new IllegalArgumentException(
                    "Preferred page size %d is below minimum page size %d".formatted(preferredPageSize, minPageSize));
            }
            if (preferredPageSize % PAGE_ALIGNMENT != 0) {
                throw new IllegalArgumentException(
                    "Preferred page size %d is not a multiple of %d".formatted(preferredPageSize, PAGE_ALIGNMENT));
            }
            if (maxPageSize < preferredPageSize) {
                throw new IllegalArgumentException(
                    "Max page size %d is below preferred page size %d".formatted(maxPageSize, preferredPageSize));
            }
        }
    }

    private final FileChannel channel;
    private final int preferredPageSize;
    private final int minPageSize;
    private final boolean pageAlignment;
    private final int maxPageSize;
    private final Path bufferPath;
    private final Path targetPath;
    private boolean closed = false;
    private long filePosition = 0;

    /// Per-namespace write state.
    private static class NamespaceWriteState {
        final String name;
        final byte namespaceIndex;
        final List<byte[]> currentRecords = new ArrayList<>();
        final List<PagesPageEntry> pageIndex = new ArrayList<>();
        long currentStartOrdinal = Long.MIN_VALUE;
        long nextExpectedOrdinal = Long.MIN_VALUE;
        long lastWrittenOrdinal = Long.MIN_VALUE;
        boolean hasWritten = false;
        int currentRecordBytes = 0;

        NamespaceWriteState(String name, byte namespaceIndex) {
            this.name = name;
            this.namespaceIndex = namespaceIndex;
        }
    }

    private final Map<String, NamespaceWriteState> namespaceStates = new LinkedHashMap<>();
    private byte nextNamespaceIndex = NAMESPACE_DEFAULT;

    /// Creates a new writer that truncates any existing file at the given path.
    ///
    /// @param path              the file to write
    /// @param preferredPageSize the target page size in bytes; must be at least
    ///                          {@link SlabConstants#MIN_PAGE_SIZE} and a multiple
    ///                          of {@link SlabConstants#PAGE_ALIGNMENT}
    /// @throws IOException if the file cannot be opened
    public SlabWriter(Path path, int preferredPageSize) throws IOException {
        this(path, new SlabWriterConfig(preferredPageSize, PAGE_ALIGNMENT, false));
    }

    /// Creates a new writer with full configuration control.
    ///
    /// @param path   the file to write
    /// @param config the layout configuration
    /// @throws IOException if the file cannot be opened
    public SlabWriter(Path path, SlabWriterConfig config) throws IOException {
        this.preferredPageSize = config.preferredPageSize();
        this.minPageSize = config.minPageSize();
        this.pageAlignment = config.pageAlignment();
        this.maxPageSize = config.maxPageSize();
        this.bufferPath = null;
        this.targetPath = null;
        this.channel = FileChannel.open(path,
            StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private SlabWriter(FileChannel channel, SlabWriterConfig config) {
        this.channel = channel;
        this.preferredPageSize = config.preferredPageSize();
        this.minPageSize = config.minPageSize();
        this.pageAlignment = config.pageAlignment();
        this.maxPageSize = config.maxPageSize();
        this.bufferPath = null;
        this.targetPath = null;
    }

    private SlabWriter(FileChannel channel, SlabWriterConfig config, Path bufferPath, Path targetPath) {
        this.channel = channel;
        this.preferredPageSize = config.preferredPageSize();
        this.minPageSize = config.minPageSize();
        this.pageAlignment = config.pageAlignment();
        this.maxPageSize = config.maxPageSize();
        this.bufferPath = bufferPath;
        this.targetPath = targetPath;
    }

    /// Creates a writer that writes to a `.buffer` temporary file and
    /// atomically renames it to the target path on successful close.
    ///
    /// The writer opens `<target>.buffer` for writing. On successful
    /// {@link #close()}, the buffer file is atomically moved to the target
    /// path. If close fails, the buffer file is deleted.
    ///
    /// @param target the desired final file path
    /// @param config the layout configuration
    /// @return a new writer backed by a buffer file
    /// @throws IOException if the buffer file cannot be created
    public static SlabWriter createWithBufferNaming(Path target, SlabWriterConfig config) throws IOException {
        Path buffer = Path.of(target + ".buffer");
        FileChannel ch = FileChannel.open(buffer,
            StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        return new SlabWriter(ch, config, buffer, target);
    }

    /// Opens an existing slabtastic file for appending.
    ///
    /// Reads the current file structure to discover existing data pages, then
    /// positions the write cursor at the end of the file. New data pages are
    /// appended after the old pages page (which becomes dead data). On
    /// {@link #close()}, a new pages page is written that references both old
    /// and new data pages. The last pages page is always authoritative per the
    /// spec.
    ///
    /// Ordinals written via {@link #write(long, byte[])} must be strictly
    /// greater than the maximum ordinal already present in the file.
    ///
    /// @param path              the existing slabtastic file to append to
    /// @param preferredPageSize the target page size for new pages
    /// @return a writer positioned for appending
    /// @throws IOException if the file cannot be read or is not a valid
    ///                     slabtastic file
    public static SlabWriter openForAppend(Path path, int preferredPageSize) throws IOException {
        return openForAppend(path, new SlabWriterConfig(preferredPageSize, PAGE_ALIGNMENT, false));
    }

    /// Opens an existing slabtastic file for appending with full configuration.
    ///
    /// @param path   the existing slabtastic file to append to
    /// @param config the layout configuration for new pages
    /// @return a writer positioned for appending
    /// @throws IOException if the file cannot be read or is not a valid
    ///                     slabtastic file
    public static SlabWriter openForAppend(Path path, SlabWriterConfig config) throws IOException {
        try (SlabReader reader = new SlabReader(path)) {
            FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE);
            SlabWriter writer = new SlabWriter(channel, config);
            writer.filePosition = reader.fileSize();

            // Seed state for each namespace
            for (String nsName : reader.namespaces()) {
                List<SlabReader.PageSummary> pages = reader.pages(nsName);
                NamespaceWriteState state = writer.ensureNamespace(nsName);

                for (SlabReader.PageSummary ps : pages) {
                    state.pageIndex.add(new PagesPageEntry(ps.startOrdinal(), ps.fileOffset()));
                    if (ps.recordCount() > 0) {
                        long pageMax = ps.startOrdinal() + ps.recordCount() - 1;
                        if (pageMax > state.lastWrittenOrdinal) {
                            state.lastWrittenOrdinal = pageMax;
                        }
                    }
                }
                if (!state.pageIndex.isEmpty()) {
                    state.hasWritten = true;
                }
            }
            return writer;
        }
    }

    /// Returns the effective alignment granularity for page sizing.
    ///
    /// When page alignment is enabled, pages are padded to multiples of
    /// {@code minPageSize}. When disabled, pages are padded to the absolute
    /// floor of {@link SlabConstants#PAGE_ALIGNMENT} (512 bytes).
    private int effectiveAlignment() {
        return pageAlignment ? minPageSize : PAGE_ALIGNMENT;
    }

    /// Appends a record with the given ordinal to the specified namespace.
    /// Ordinals must be strictly ascending within each namespace (no
    /// duplicates, no descending) and within the 5-byte signed range
    /// [{@link SlabConstants#MIN_ORDINAL}, {@link SlabConstants#MAX_ORDINAL}].
    ///
    /// @param namespace the namespace to write to
    /// @param ordinal   the global ordinal for this record
    /// @param data      the record payload
    /// @throws IOException              if a flush fails
    /// @throws IllegalStateException    if the writer is closed
    /// @throws IllegalArgumentException if the ordinal is out of range or
    ///                                  not strictly ascending
    public void write(String namespace, long ordinal, byte[] data) throws IOException {
        if (closed) {
            throw new IllegalStateException("Writer is closed");
        }
        if (ordinal < MIN_ORDINAL || ordinal > MAX_ORDINAL) {
            throw new IllegalArgumentException(
                "Ordinal %d is outside the 5-byte signed range [%d, %d]"
                    .formatted(ordinal, MIN_ORDINAL, MAX_ORDINAL));
        }

        NamespaceWriteState state = ensureNamespace(namespace);

        if (state.hasWritten && ordinal <= state.lastWrittenOrdinal) {
            throw new IllegalArgumentException(
                "Ordinal %d is not strictly ascending (last written: %d)"
                    .formatted(ordinal, state.lastWrittenOrdinal));
        }

        // Flush on ordinal gap — ordinals within a page must be contiguous
        if (!state.currentRecords.isEmpty() && ordinal != state.nextExpectedOrdinal) {
            flushCurrentPage(state);
        }

        if (state.currentRecords.isEmpty()) {
            state.currentStartOrdinal = ordinal;
        }

        // Check whether adding this record would overflow the preferred page size
        int wouldNeedSize = HEADER_SIZE + state.currentRecordBytes + data.length
            + (state.currentRecords.size() + 2) * OFFSET_ENTRY_SIZE + FOOTER_V1_SIZE;
        int wouldNeedAligned = roundUp(wouldNeedSize, effectiveAlignment());

        if (!state.currentRecords.isEmpty() && wouldNeedAligned > preferredPageSize) {
            flushCurrentPage(state);
            state.currentStartOrdinal = ordinal;
        }

        state.currentRecords.add(data);
        state.currentRecordBytes += data.length;
        state.nextExpectedOrdinal = ordinal + 1;
        state.lastWrittenOrdinal = ordinal;
        state.hasWritten = true;
    }

    /// Appends a record with the given ordinal to the default namespace.
    /// Ordinals must be strictly ascending (no duplicates, no descending)
    /// and within the 5-byte signed range
    /// [{@link SlabConstants#MIN_ORDINAL}, {@link SlabConstants#MAX_ORDINAL}].
    /// Records that would overflow the preferred page size trigger a page flush
    /// first. Ordinal gaps are allowed and start a new page.
    ///
    /// @param ordinal the global ordinal for this record
    /// @param data    the record payload
    /// @throws IOException              if a flush fails
    /// @throws IllegalStateException    if the writer is closed
    /// @throws IllegalArgumentException if the ordinal is out of range or
    ///                                  not strictly ascending
    public void write(long ordinal, byte[] data) throws IOException {
        write("", ordinal, data);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        try {
            // Flush all pending records in each namespace
            for (NamespaceWriteState state : namespaceStates.values()) {
                if (!state.currentRecords.isEmpty()) {
                    flushCurrentPage(state);
                }
            }

            // Determine whether we need multi-namespace layout
            boolean multiNamespace = namespaceStates.size() > 1
                || (namespaceStates.size() == 1 && !namespaceStates.containsKey(""));

            if (multiNamespace) {
                // Ensure the default namespace is present (spec requires index 1 = "")
                ensureNamespace("");

                // Multi-namespace: write one pages page per namespace, then namespaces page
                List<NamespacesPageEntry> nsEntries = new ArrayList<>();
                for (NamespaceWriteState state : namespaceStates.values()) {
                    long ppOffset = filePosition;
                    writePagesPage(state);
                    nsEntries.add(new NamespacesPageEntry(
                        state.namespaceIndex, state.name, ppOffset));
                }
                writeNamespacesPage(nsEntries);
            } else {
                // Single namespace (or empty): write one pages page (backward compat)
                NamespaceWriteState defaultState = namespaceStates.get("");
                if (defaultState == null) {
                    defaultState = ensureNamespace("");
                }
                writePagesPage(defaultState);
            }
            channel.force(false);
        } catch (Exception e) {
            channel.close();
            if (bufferPath != null) {
                try { Files.deleteIfExists(bufferPath); } catch (IOException ignored) {}
            }
            throw e;
        }
        channel.close();

        // Atomic rename from buffer to target
        if (bufferPath != null && targetPath != null) {
            Files.move(bufferPath, targetPath,
                StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /// Lazily creates namespace write state for the given name.
    ///
    /// Per the slabtastic spec, namespace index 1 is always reserved for
    /// the default namespace (`""`). When a non-default namespace is
    /// created first, the default namespace is pre-reserved at index 1 so
    /// that user-defined namespaces start at index 2.
    private NamespaceWriteState ensureNamespace(String name) {
        NamespaceWriteState existing = namespaceStates.get(name);
        if (existing != null) {
            return existing;
        }
        // If this is a non-default namespace and the default has not
        // been reserved yet, reserve index 1 for "" first.
        if (!name.isEmpty() && nextNamespaceIndex == NAMESPACE_DEFAULT
            && !namespaceStates.containsKey("")) {
            namespaceStates.put("", new NamespaceWriteState("", nextNamespaceIndex++));
        }
        byte idx = nextNamespaceIndex++;
        NamespaceWriteState state = new NamespaceWriteState(name, idx);
        namespaceStates.put(name, state);
        return state;
    }

    private void flushCurrentPage(NamespaceWriteState state) throws IOException {
        SlabPage page = new SlabPage(state.currentStartOrdinal, PAGE_TYPE_DATA,
            state.currentRecords, state.namespaceIndex);
        ByteBuffer buf = page.toByteBuffer();

        if (buf.remaining() > maxPageSize) {
            throw new IOException(
                "Aligned page size %d exceeds max page size %d (start ordinal %d, %d records)"
                    .formatted(buf.remaining(), maxPageSize, state.currentStartOrdinal,
                        state.currentRecords.size()));
        }

        state.pageIndex.add(new PagesPageEntry(state.currentStartOrdinal, filePosition));

        buf.rewind();
        while (buf.hasRemaining()) {
            filePosition += channel.write(buf, filePosition);
        }
        channel.force(false);

        state.currentRecords.clear();
        state.currentRecordBytes = 0;
    }

    private void writePagesPage(NamespaceWriteState state) throws IOException {
        List<byte[]> entries = new ArrayList<>(state.pageIndex.size());
        ByteBuffer entryBuf = ByteBuffer.allocate(PAGES_PAGE_RECORD_SIZE);
        for (PagesPageEntry entry : state.pageIndex) {
            entryBuf.clear();
            entry.writeTo(entryBuf, 0);
            entries.add(entryBuf.array().clone());
        }

        SlabPage pagesPage = new SlabPage(0, PAGE_TYPE_PAGES_PAGE, entries, state.namespaceIndex);
        ByteBuffer buf = pagesPage.toByteBuffer();

        buf.rewind();
        while (buf.hasRemaining()) {
            filePosition += channel.write(buf, filePosition);
        }
    }

    private void writeNamespacesPage(List<NamespacesPageEntry> nsEntries) throws IOException {
        List<byte[]> records = new ArrayList<>(nsEntries.size());
        for (NamespacesPageEntry entry : nsEntries) {
            ByteBuffer buf = ByteBuffer.allocate(entry.serializedSize());
            entry.writeTo(buf, 0);
            records.add(buf.array());
        }

        SlabPage nsPage = new SlabPage(0, PAGE_TYPE_NAMESPACES_PAGE, records, NAMESPACE_DEFAULT);
        ByteBuffer buf = nsPage.toByteBuffer();

        buf.rewind();
        while (buf.hasRemaining()) {
            filePosition += channel.write(buf, filePosition);
        }
    }

    private static int roundUp(int value, int alignment) {
        return ((value + alignment - 1) / alignment) * alignment;
    }
}
