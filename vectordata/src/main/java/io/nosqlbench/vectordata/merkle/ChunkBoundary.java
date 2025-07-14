package io.nosqlbench.vectordata.merkle;

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
 * Immutable record representing the boundaries of a chunk in a file.
 * Used by ChunkGeometry to provide consistent chunk boundary information
 * across all merkle system components.
 * 
 * @param chunkIndex The zero-based index of this chunk
 * @param startInclusive The starting byte position of this chunk (inclusive)
 * @param endExclusive The ending byte position of this chunk (exclusive)
 */
public record ChunkBoundary(int chunkIndex, long startInclusive, long endExclusive) {
    
    /**
     * Validates the chunk boundary parameters.
     * 
     * @param chunkIndex The zero-based index of this chunk
     * @param startInclusive The starting byte position of this chunk (inclusive)
     * @param endExclusive The ending byte position of this chunk (exclusive)
     */
    public ChunkBoundary {
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