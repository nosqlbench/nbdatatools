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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Factory for creating ChunkGeometryDescriptor instances from various sources.
 * Provides consistent creation patterns to ensure all components
 * use the same geometry calculations.
 */
public class ChunkGeometryFactory {

    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private ChunkGeometryFactory() {
        // Utility class - prevent instantiation
    }

    /**
     * Default chunk size when no other source is available (1MB).
     */
    public static final long DEFAULT_CHUNK_SIZE = 1024 * 1024;

    /**
     * Creates ChunkGeometryDescriptor from an existing MerkleTree.
     * 
     * @param tree The MerkleTree to extract geometry from
     * @param totalFileSize The actual file size
     * @return ChunkGeometryDescriptor based on the tree's configuration
     * @throws IllegalArgumentException if tree parameters are invalid
     */
    public static ChunkGeometryDescriptor fromMerkleTree(MerkleTree tree, long totalFileSize) {
        // Chunk size will be automatically calculated based on content size
        return new ChunkGeometryDescriptor(totalFileSize);
    }

    /**
     * Creates ChunkGeometryDescriptor from a data file.
     * Note: Chunk size is now automatically calculated based on file size.
     * 
     * @param dataFile The data file to create geometry for
     * @param chunkSize The chunk size (ignored - kept for backward compatibility)
     * @return ChunkGeometryDescriptor based on the file size with calculated chunk size
     * @throws IOException if the file cannot be read
     * @deprecated Use fromFile(Path) instead as chunk size is now automatically calculated
     */
    @Deprecated
    public static ChunkGeometryDescriptor fromFile(Path dataFile, long chunkSize) throws IOException {
        if (!Files.exists(dataFile)) {
            throw new IOException("Data file does not exist: " + dataFile);
        }

        long fileSize = Files.size(dataFile);
        return new ChunkGeometryDescriptor(fileSize);
    }

    /**
     * Creates ChunkGeometryDescriptor from a data file using automatic chunk size
     * calculation based on file size for consistent results.
     * 
     * @param dataFile The data file to create geometry for
     * @return ChunkGeometryDescriptor based on the file size with calculated chunk size
     * @throws IOException if the file cannot be read
     */
    public static ChunkGeometryDescriptor fromFile(Path dataFile) throws IOException {
        if (!Files.exists(dataFile)) {
            throw new IOException("Data file does not exist: " + dataFile);
        }

        long fileSize = Files.size(dataFile);
        return new ChunkGeometryDescriptor(fileSize);
    }


    /**
     * Creates ChunkGeometryDescriptor from a merkle file's metadata.
     * This will be implemented when merkle files store chunk size metadata.
     * 
     * @param merkleFile The merkle file to read metadata from
     * @return ChunkGeometryDescriptor based on the stored metadata
     * @throws IOException if the merkle file cannot be read
     * @throws UnsupportedOperationException until merkle metadata is implemented
     */
    public static ChunkGeometryDescriptor fromMerkleFile(Path merkleFile) throws IOException {
        // TODO: Implement when MerkleTreeHeader is available
        throw new UnsupportedOperationException(
            "Reading geometry from merkle file metadata not yet implemented");
    }

    /**
     * Creates ChunkGeometryDescriptor with content size.
     * Note: Chunk size is now automatically calculated based on content size.
     * 
     * @param chunkSize The chunk size (ignored - kept for backward compatibility)
     * @param totalFileSize The total file size in bytes
     * @return ChunkGeometryDescriptor with the specified parameters
     * @deprecated Use new ChunkGeometryDescriptor(totalFileSize) directly as chunk size is now automatically calculated
     */
    @Deprecated
    public static ChunkGeometryDescriptor createExplicit(long chunkSize, long totalFileSize) {
        // Ignore the chunkSize parameter as it's now calculated automatically
        return new ChunkGeometryDescriptor(totalFileSize);
    }

    /**
     * Creates ChunkGeometryDescriptor from a data file.
     * Note: Chunk size is now automatically calculated based on file size.
     * 
     * @param dataFile The data file
     * @param merkleFile The associated merkle file (may not exist)
     * @return ChunkGeometryDescriptor using the best available information
     * @throws IOException if files cannot be read
     */
    public static ChunkGeometryDescriptor fromBestAvailableSource(Path dataFile, Path merkleFile) throws IOException {
        if (!Files.exists(dataFile)) {
            throw new IOException("Data file does not exist: " + dataFile);
        }

        long fileSize = Files.size(dataFile);

        // TODO: When merkle metadata is implemented, check merkle file first
        // if (Files.exists(merkleFile)) {
        //     try {
        //         return fromMerkleFile(merkleFile);
        //     } catch (Exception e) {
        //         // Fall back to automatic calculation if merkle file is corrupted
        //     }
        // }

        return new ChunkGeometryDescriptor(fileSize);
    }
}
