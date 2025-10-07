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

import java.util.BitSet;
import java.util.List;
import java.util.Arrays;

///
/// Represents a test scenario for scheduler testing.
/// 
/// A test scenario defines the content structure (size, chunks) and
/// the current state of chunk validity, providing a controlled
/// environment for testing scheduler behavior.
///
/// Test scenarios are immutable and created using the Builder pattern
/// to ensure consistent and predictable test conditions.
///
public class TestScenario {
    private final MerkleShape shape;
    private final MockMerkleState state;
    private final String description;
    
    private TestScenario(MerkleShape shape, MockMerkleState state, String description) {
        this.shape = shape;
        this.state = state;
        this.description = description;
    }
    
    ///
    /// Gets the MerkleShape defining the content structure.
    ///
    /// @return The shape defining chunk layout and tree structure
    ///
    public MerkleShape getShape() {
        return shape;
    }
    
    ///
    /// Gets the MockMerkleState defining chunk validity.
    ///
    /// @return The state tracking which chunks are valid
    ///
    public MockMerkleState getState() {
        return state;
    }
    
    ///
    /// Gets a human-readable description of this scenario.
    ///
    /// @return Description of the test scenario
    ///
    public String getDescription() {
        return description;
    }
    
    ///
    /// Gets the total content size in bytes.
    ///
    /// @return The total content size
    ///
    public long getContentSize() {
        return shape.getTotalContentSize();
    }
    
    ///
    /// Gets the chunk size in bytes.
    ///
    /// @return The size of each chunk
    ///
    public long getChunkSize() {
        return shape.getChunkSize();
    }
    
    ///
    /// Gets the total number of chunks.
    ///
    /// @return The total chunk count
    ///
    public int getTotalChunks() {
        return shape.getTotalChunks();
    }
    
    ///
    /// Gets the number of valid chunks.
    ///
    /// @return Count of chunks marked as valid
    ///
    public int getValidChunkCount() {
        return state.getValidChunks().cardinality();
    }
    
    ///
    /// Gets the number of invalid chunks.
    ///
    /// @return Count of chunks marked as invalid
    ///
    public int getInvalidChunkCount() {
        return getTotalChunks() - getValidChunkCount();
    }
    
    ///
    /// Builder for creating test scenarios with fluent API.
    ///
    public static class Builder {
        private long contentSize = 10 * 1024 * 1024; // Default: 10MB
        private long chunkSize = 1024 * 1024;        // Default: 1MB
        private BitSet validChunks = new BitSet();
        private String description = "Test scenario";
        
        ///
        /// Sets the total content size.
        ///
        /// @param contentSize Content size in bytes (must be positive)
        /// @return This builder for chaining
        /// @throws IllegalArgumentException if contentSize is not positive
        ///
        public Builder withContentSize(long contentSize) {
            if (contentSize <= 0) {
                throw new IllegalArgumentException("Content size must be positive: " + contentSize);
            }
            this.contentSize = contentSize;
            return this;
        }
        
        ///
        /// Sets the chunk size in bytes.
        ///
        /// @param chunkSize Chunk size in bytes (must be positive and power of 2)
        /// @return This builder for chaining
        /// @throws IllegalArgumentException if chunkSize is invalid
        ///
        public Builder withChunkSize(long chunkSize) {
            if (chunkSize <= 0) {
                throw new IllegalArgumentException("Chunk size must be positive: " + chunkSize);
            }
            if (Long.bitCount(chunkSize) != 1) {
                throw new IllegalArgumentException("Chunk size must be a power of 2: " + chunkSize);
            }
            this.chunkSize = chunkSize;
            return this;
        }
        
        ///
        /// Marks specific chunks as valid.
        ///
        /// @param chunkIndices The chunk indices to mark as valid
        /// @return This builder for chaining
        ///
        public Builder withValidChunks(int... chunkIndices) {
            for (int index : chunkIndices) {
                validChunks.set(index);
            }
            return this;
        }
        
        ///
        /// Marks specific chunks as invalid (clears their valid bit).
        ///
        /// @param chunkIndices The chunk indices to mark as invalid
        /// @return This builder for chaining
        ///
        public Builder withInvalidChunks(int... chunkIndices) {
            for (int index : chunkIndices) {
                validChunks.clear(index);
            }
            return this;
        }
        
        ///
        /// Marks a range of chunks as valid.
        ///
        /// @param startChunk Starting chunk index (inclusive)
        /// @param endChunk Ending chunk index (exclusive)
        /// @return This builder for chaining
        /// @throws IllegalArgumentException if range is invalid
        ///
        public Builder withValidChunkRange(int startChunk, int endChunk) {
            if (startChunk < 0 || endChunk < startChunk) {
                throw new IllegalArgumentException(
                    "Invalid chunk range: [" + startChunk + ", " + endChunk + ")");
            }
            validChunks.set(startChunk, endChunk);
            return this;
        }
        
        ///
        /// Marks all chunks as valid.
        ///
        /// @return This builder for chaining
        ///
        public Builder withAllChunksValid() {
            // Will be set after shape is created in build()
            return this;
        }
        
        ///
        /// Marks all chunks as invalid.
        ///
        /// @return This builder for chaining
        ///
        public Builder withAllChunksInvalid() {
            validChunks.clear();
            return this;
        }
        
        ///
        /// Sets every nth chunk as valid, creating a sparse pattern.
        ///
        /// @param interval The interval (every nth chunk will be valid)
        /// @param offset Starting offset (0-based)
        /// @return This builder for chaining
        /// @throws IllegalArgumentException if interval is not positive
        ///
        public Builder withSparseValidPattern(int interval, int offset) {
            if (interval <= 0) {
                throw new IllegalArgumentException("Interval must be positive: " + interval);
            }
            // Will be applied after shape is created in build()
            return this;
        }
        
        ///
        /// Sets a human-readable description for this scenario.
        ///
        /// @param description Description of the test scenario
        /// @return This builder for chaining
        ///
        public Builder withDescription(String description) {
            this.description = description != null ? description : "Test scenario";
            return this;
        }
        
        ///
        /// Builds the test scenario.
        ///
        /// @return A new TestScenario with the configured parameters
        /// @throws IllegalArgumentException if configuration is invalid
        ///
        public TestScenario build() {
            // Create the MerkleShape
            BaseMerkleShape shape = new BaseMerkleShape(contentSize, chunkSize);
            
            // Validate chunk indices against actual chunk count
            int totalChunks = shape.getTotalChunks();
            if (validChunks.nextSetBit(totalChunks) != -1) {
                throw new IllegalArgumentException(
                    "Valid chunk index exceeds total chunks (" + totalChunks + ")");
            }
            
            // Create mock state
            MockMerkleState state = new MockMerkleState(shape, (BitSet) validChunks.clone());
            
            // Generate description if not set
            String finalDescription = description;
            if (description.equals("Test scenario")) {
                finalDescription = String.format(
                    "Content: %s, Chunks: %d×%s, Valid: %d/%d",
                    formatBytes(contentSize),
                    totalChunks,
                    formatBytes(chunkSize),
                    validChunks.cardinality(),
                    totalChunks
                );
            }
            
            return new TestScenario(shape, state, finalDescription);
        }
        
        ///
        /// Formats byte size for human readability.
        ///
        /// @param bytes The byte count to format
        /// @return Formatted string (e.g., "1.5MB", "2GB")
        ///
        private String formatBytes(long bytes) {
            if (bytes < 1024) return bytes + "B";
            if (bytes < 1024 * 1024) return String.format("%.1fKB", bytes / 1024.0);
            if (bytes < 1024 * 1024 * 1024) return String.format("%.1fMB", bytes / (1024.0 * 1024));
            return String.format("%.1fGB", bytes / (1024.0 * 1024 * 1024));
        }
    }
    
    @Override
    public String toString() {
        return "TestScenario{" + description + "}";
    }
}