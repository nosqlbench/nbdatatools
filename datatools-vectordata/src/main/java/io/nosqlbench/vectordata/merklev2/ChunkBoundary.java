package io.nosqlbench.vectordata.merklev2;

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

/**
 * Immutable class representing the boundaries of a chunk in a file.
 * Used by ChunkGeometry to provide consistent chunk boundary information
 * across all merkle system components.
 */
public class ChunkBoundary {
    /// The zero-based index of this chunk
    private final int chunkIndex;
    /// The starting byte position of this chunk (inclusive)
    private final long startInclusive;
    /// The ending byte position of this chunk (exclusive)
    private final long endExclusive;
    
    public ChunkBoundary(int chunkIndex, long startInclusive, long endExclusive) {
        if (chunkIndex < 0) {
            throw new IllegalArgumentException("Chunk index cannot be negative: " + chunkIndex);
        }
        if (startInclusive < 0) {
            throw new IllegalArgumentException("Start position cannot be negative: " + startInclusive);
        }
        if (endExclusive <= startInclusive) {
            throw new IllegalArgumentException(
                "End position (" + endExclusive + ") must be greater than start position (" + startInclusive + ")");
        }
        this.chunkIndex = chunkIndex;
        this.startInclusive = startInclusive;
        this.endExclusive = endExclusive;
    }
    
    /// @return The zero-based index of this chunk
    public int chunkIndex() {
        return chunkIndex;
    }
    
    /// @return The starting byte position of this chunk (inclusive)
    public long startInclusive() {
        return startInclusive;
    }
    
    /// @return The ending byte position of this chunk (exclusive)
    public long endExclusive() {
        return endExclusive;
    }
    
    
    /**
     * Gets the size of this chunk in bytes.
     * 
     * @return The size of the chunk (endExclusive - startInclusive)
     */
    public long size() {
        return endExclusive - startInclusive;
    }
    
    /**
     * Gets the length of this chunk in bytes.
     * This is an alias for size() for compatibility.
     * 
     * @return The length of the chunk (endExclusive - startInclusive)
     */
    public long length() {
        return size();
    }
    
    /**
     * Checks if this chunk contains the specified file position.
     * 
     * @param position The file position to check
     * @return true if the position is within this chunk's boundaries
     */
    public boolean contains(long position) {
        return position >= startInclusive && position < endExclusive;
    }
    
    /**
     * Gets the offset of a position within this chunk.
     * 
     * @param position The file position
     * @return The offset within the chunk (0-based)
     * @throws IllegalArgumentException if position is not within this chunk
     */
    public long getOffsetInChunk(long position) {
        if (!contains(position)) {
            throw new IllegalArgumentException(
                "Position " + position + " is not within chunk boundaries [" + 
                startInclusive + ", " + endExclusive + ")");
        }
        return position - startInclusive;
    }
}