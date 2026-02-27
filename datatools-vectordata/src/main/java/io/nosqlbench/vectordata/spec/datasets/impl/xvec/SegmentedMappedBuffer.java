package io.nosqlbench.vectordata.spec.datasets.impl.xvec;

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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/// A multi-segment memory-mapped buffer that supports arbitrarily large files.
///
/// A single {@link MappedByteBuffer} is limited to {@link Integer#MAX_VALUE} bytes
/// (~2 GB) due to int-based addressing. This class maps a file using multiple
/// segments (each up to 1 GB), providing transparent access across segment
/// boundaries.
///
/// ## Usage
///
/// ```java
/// try (SegmentedMappedBuffer buf = new SegmentedMappedBuffer(path, fileSize)) {
///     int dim = buf.getInt(0);
///     ByteBuffer slice = buf.slice(offset, length);
/// }
/// ```
///
/// ## Cross-segment reads
///
/// When a {@link #slice(long, int)} or {@link #getInt(long)} call spans two
/// segments, the data is copied into a temporary heap buffer. Within a single
/// segment, the returned buffer is a zero-copy view.
public class SegmentedMappedBuffer implements Closeable {

    /// Segment size: 1 GB. Chosen to be well below the 2 GB
    /// {@link MappedByteBuffer} limit while keeping the segment count small.
    static final long SEGMENT_SIZE = 1L << 30;

    private final MappedByteBuffer[] segments;
    private final long fileSize;

    /// Creates a new segmented memory-mapped buffer over the given file.
    ///
    /// @param path     the file to map
    /// @param fileSize the size of the file in bytes (must match actual file size)
    /// @throws IOException if the file cannot be opened or mapped
    public SegmentedMappedBuffer(Path path, long fileSize) throws IOException {
        this.fileSize = fileSize;
        int segmentCount = (int) ((fileSize + SEGMENT_SIZE - 1) / SEGMENT_SIZE);
        if (segmentCount == 0) segmentCount = 1; // handle empty file gracefully
        this.segments = new MappedByteBuffer[segmentCount];

        try (FileChannel fc = FileChannel.open(path, StandardOpenOption.READ)) {
            for (int i = 0; i < segmentCount; i++) {
                long offset = (long) i * SEGMENT_SIZE;
                long size = Math.min(SEGMENT_SIZE, fileSize - offset);
                segments[i] = fc.map(FileChannel.MapMode.READ_ONLY, offset, size);
                segments[i].order(ByteOrder.LITTLE_ENDIAN);
            }
        }
    }

    /// Returns the total file size this buffer covers.
    ///
    /// @return the file size in bytes
    public long size() {
        return fileSize;
    }

    /// Reads a 4-byte little-endian integer at the given byte offset.
    ///
    /// @param offset the absolute byte offset in the file
    /// @return the integer value
    /// @throws IndexOutOfBoundsException if the offset is out of range
    public int getInt(long offset) {
        int segIdx = (int) (offset / SEGMENT_SIZE);
        int localOffset = (int) (offset % SEGMENT_SIZE);

        MappedByteBuffer seg = segments[segIdx];
        // Check if the 4-byte read fits within this segment
        if (localOffset + 4 <= seg.capacity()) {
            return seg.getInt(localOffset);
        }

        // Cross-segment read: copy 4 bytes manually
        ByteBuffer tmp = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < 4; i++) {
            int si = (int) ((offset + i) / SEGMENT_SIZE);
            int li = (int) ((offset + i) % SEGMENT_SIZE);
            tmp.put(segments[si].get(li));
        }
        tmp.flip();
        return tmp.getInt();
    }

    /// Returns a {@link ByteBuffer} view of the specified byte range.
    ///
    /// If the range fits within a single segment, returns a zero-copy slice
    /// (no allocation, no copying). If the range spans two segments, allocates
    /// a temporary buffer and copies from both segments.
    ///
    /// @param offset the absolute byte offset in the file
    /// @param length the number of bytes to include in the slice
    /// @return a {@link ByteBuffer} positioned at 0 with the requested data,
    ///         in little-endian byte order
    /// @throws IndexOutOfBoundsException if the range exceeds the file size
    public ByteBuffer slice(long offset, int length) {
        if (offset + length > fileSize) {
            throw new IndexOutOfBoundsException(
                "Range [" + offset + ", " + (offset + length) + ") exceeds file size " + fileSize);
        }

        int segIdx = (int) (offset / SEGMENT_SIZE);
        int localOffset = (int) (offset % SEGMENT_SIZE);
        MappedByteBuffer seg = segments[segIdx];

        if (localOffset + length <= seg.capacity()) {
            // Zero-copy path: entire range within one segment
            ByteBuffer dup = seg.duplicate().order(ByteOrder.LITTLE_ENDIAN);
            dup.position(localOffset);
            dup.limit(localOffset + length);
            return dup.slice().order(ByteOrder.LITTLE_ENDIAN);
        }

        // Cross-segment path: copy from two adjacent segments
        ByteBuffer tmp = ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN);
        int firstLen = seg.capacity() - localOffset;
        ByteBuffer firstDup = seg.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        firstDup.position(localOffset);
        firstDup.limit(localOffset + firstLen);
        tmp.put(firstDup);

        int secondLen = length - firstLen;
        MappedByteBuffer seg2 = segments[segIdx + 1];
        ByteBuffer secondDup = seg2.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        secondDup.position(0);
        secondDup.limit(secondLen);
        tmp.put(secondDup);

        tmp.flip();
        return tmp;
    }

    /// Releases references to the mapped buffers.
    ///
    /// The JVM will unmap the buffers during garbage collection. There is no
    /// portable way to force-unmap a {@link MappedByteBuffer} in Java 11.
    @Override
    public void close() {
        for (int i = 0; i < segments.length; i++) {
            segments[i] = null;
        }
    }
}
