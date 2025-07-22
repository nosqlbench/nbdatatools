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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.BitSet;
import java.util.concurrent.CompletableFuture;

/**
 * Factory class for creating MerkleRef instances from various data sources.
 * This class provides the main entry points for building merkle reference trees
 * in the merklev2 architecture.
 */
public class MerkleRefFactory {

    /**
     * Creates a MerkleRef from a data file with progress tracking.
     * This is the recommended method for creating merkle trees with progress monitoring.
     * 
     * @deprecated Use {@link MerkleRef#fromData(Path)} instead
     * @param dataPath The path to the data file
     * @return A MerkleRefBuildProgress that tracks the building process
     * @throws IOException If an I/O error occurs
     */
    @Deprecated
    public static MerkleRefBuildProgress fromData(Path dataPath) throws IOException {
        return MerkleDataImpl.fromDataWithProgress(dataPath);
    }

    /**
     * Creates a MerkleRef from a data file without progress tracking.
     * This method is simpler but doesn't provide progress information.
     * 
     * @deprecated Use {@link MerkleRef#fromDataSimple(Path)} instead
     * @param dataPath The path to the data file
     * @return A CompletableFuture that will complete with the MerkleDataImpl
     * @throws IOException If an I/O error occurs
     */
    @Deprecated
    public static CompletableFuture<MerkleDataImpl> fromDataSimple(Path dataPath) throws IOException {
        return MerkleDataImpl.fromData(dataPath);
    }

    /**
     * Creates a MerkleRef from a ByteBuffer without progress tracking.
     * 
     * @deprecated Use {@link MerkleRef#fromData(ByteBuffer)} instead
     * @param data The data buffer
     * @return A CompletableFuture that will complete with the MerkleDataImpl
     */
    @Deprecated
    public static CompletableFuture<MerkleDataImpl> fromData(ByteBuffer data) {
        return MerkleDataImpl.fromData(data);
    }

    /**
     * Loads a MerkleRef from an existing .mref file.
     * 
     * @deprecated Use {@link MerkleRef#load(Path)} for .mref files or {@link MerkleState#load(Path)} for .mrkl files
     * @param path The path to the .mref file
     * @return A loaded MerkleDataImpl
     * @throws IOException If an I/O error occurs during loading
     */
    @Deprecated
    public static MerkleDataImpl load(Path path) throws IOException {
        if (!Files.exists(path)) {
            throw new IOException("File does not exist: " + path);
        }
        
        long fileSize = Files.size(path);
        if (fileSize == 0) {
            throw new IOException("File is empty: " + path);
        }
        
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            // Read footer from the end of the file
            Merklev2Footer footer = Merklev2Footer.readFromChannel(channel, 
                fileSize - Merklev2Footer.FIXED_FOOTER_SIZE);
            
            // Create shape from footer
            MerkleShape shape = new BaseMerkleShape(footer.totalContentSize(), footer.chunkSize());
            
            // Calculate data region size
            long dataRegionSize = (long) shape.getNodeCount() * 32; // 32 bytes per SHA-256 hash
            
            // Verify file size consistency
            long expectedFileSize = dataRegionSize + footer.bitSetSize() + footer.footerLength();
            if (fileSize != expectedFileSize) {
                throw new IOException("File size mismatch. Expected: " + expectedFileSize + 
                    ", actual: " + fileSize);
            }
            
            // Read hash data
            byte[][] hashes = new byte[shape.getNodeCount()][];
            long currentPosition = 0;
            
            // Read leaf hashes
            for (int i = 0; i < shape.getLeafCount(); i++) {
                byte[] hash = new byte[32];
                ByteBuffer buffer = ByteBuffer.wrap(hash);
                long leafPosition = (long) (shape.getOffset() + i) * 32;
                channel.read(buffer, leafPosition);
                int hashIndex = shape.getOffset() + i;
                hashes[hashIndex] = hash;
            }
            
            // Note: Skipping padded leaves - they contain zero hashes
            
            // Read internal node hashes
            for (int i = 0; i < shape.getOffset(); i++) {
                byte[] hash = new byte[32];
                ByteBuffer buffer = ByteBuffer.wrap(hash);
                long internalPosition = (long) i * 32;
                channel.read(buffer, internalPosition);
                hashes[i] = hash;
            }
            
            // Read and validate BitSet (though for reference trees, all bits should be set)
            ByteBuffer bitSetBuffer = ByteBuffer.allocate(footer.bitSetSize());
            long bitSetPosition = dataRegionSize;
            channel.read(bitSetBuffer, bitSetPosition);
            bitSetBuffer.flip();
            
            byte[] bitSetData = new byte[footer.bitSetSize()];
            bitSetBuffer.get(bitSetData);
            BitSet validBits = BitSet.valueOf(bitSetData);
            
            // For reference trees, we expect all bits to be set, but we don't enforce this
            // as the file might have been created with partial data
            
            // Create a new MerkleDataImpl from the loaded hashes using the static factory method
            // We need to use the in-memory constructor since we have the hashes loaded
            return MerkleDataImpl.createFromHashesAndBitSet(shape, hashes, validBits);
        }
    }

    /**
     * Creates an empty MerkleRef for the given content size.
     * All hashes will be null/empty.
     * 
     * @deprecated Use {@link MerkleRef#createEmpty(long)} instead
     * @param contentSize The total size of the content
     * @return An empty MerkleDataImpl
     */
    @Deprecated
    public static MerkleDataImpl createEmpty(long contentSize) {
        return MerkleDataImpl.createEmpty(contentSize);
    }
}