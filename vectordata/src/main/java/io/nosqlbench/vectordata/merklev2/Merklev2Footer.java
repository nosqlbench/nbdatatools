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
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

/**
 * A class for the footer of a merkle tree file in the merklev2 package.
 * <p>
 * The footer contains metadata about the merkle tree.
 */
public class Merklev2Footer {
    /// the size of each chunk in bytes
    private final long chunkSize;
    /// the total size of the data in bytes
    private final long totalContentSize;
    /// the total number of chunks
    private final int totalChunks;
    /// the number of leaf nodes in the merkle tree
    private final int leafCount;
    /// the capacity for leaf nodes (next power of 2 >= leafCount)
    private final int capLeaf;
    /// the total number of nodes in the merkle tree
    private final int nodeCount;
    /// the offset where leaf nodes start in the merkle tree array
    private final int offset;
    /// the number of internal nodes in the merkle tree
    private final int internalNodeCount;
    /// the size of the BitSet in bytes
    private final int bitSetSize;
    /// the length of the footer in bytes
    private final byte footerLength;
    
    public Merklev2Footer(long chunkSize, long totalContentSize, int totalChunks, int leafCount, int capLeaf, int nodeCount, int offset, int internalNodeCount, int bitSetSize, byte footerLength) {
        this.chunkSize = chunkSize;
        this.totalContentSize = totalContentSize;
        this.totalChunks = totalChunks;
        this.leafCount = leafCount;
        this.capLeaf = capLeaf;
        this.nodeCount = nodeCount;
        this.offset = offset;
        this.internalNodeCount = internalNodeCount;
        this.bitSetSize = bitSetSize;
        this.footerLength = footerLength;
    }
    
    /// @return the size of each chunk in bytes
    public long chunkSize() {
        return chunkSize;
    }
    
    /// @return the total size of the data in bytes
    public long totalContentSize() {
        return totalContentSize;
    }
    
    /// @return the total number of chunks
    public int totalChunks() {
        return totalChunks;
    }
    
    /// @return the number of leaf nodes in the merkle tree
    public int leafCount() {
        return leafCount;
    }
    
    /// @return the capacity for leaf nodes (next power of 2 >= leafCount)
    public int capLeaf() {
        return capLeaf;
    }
    
    /// @return the total number of nodes in the merkle tree
    public int nodeCount() {
        return nodeCount;
    }
    
    /// @return the offset where leaf nodes start in the merkle tree array
    public int offset() {
        return offset;
    }
    
    /// @return the number of internal nodes in the merkle tree
    public int internalNodeCount() {
        return internalNodeCount;
    }
    
    /// @return the size of the BitSet in bytes
    public int bitSetSize() {
        return bitSetSize;
    }
    
    /// @return the length of the footer in bytes
    public byte footerLength() {
        return footerLength;
    }

    /**
     * The fixed size of the footer in bytes
     * 8 bytes for chunkSize + 8 bytes for totalContentSize + 4 bytes for totalChunks + 
     * 4 bytes for leafCount + 4 bytes for capLeaf + 4 bytes for nodeCount + 
     * 4 bytes for offset + 4 bytes for internalNodeCount + 4 bytes for bitSetSize + 
     * 1 byte for footerLength
     */
    public static final int FIXED_FOOTER_SIZE = Long.BYTES * 2 + Integer.BYTES * 7 + Byte.BYTES;

    /**
     * Creates a new Merklev2Footer with the given parameters and calculates the footer length.
     * @param chunkSize the size of each chunk in bytes
     * @param totalContentSize the total size of the data in bytes
     * @param totalChunks the total number of chunks
     * @param leafCount the number of leaf nodes in the merkle tree
     * @param capLeaf the capacity for leaf nodes
     * @param nodeCount the total number of nodes in the merkle tree
     * @param offset the offset where leaf nodes start in the merkle tree array
     * @param internalNodeCount the number of internal nodes in the merkle tree
     * @param bitSetSize the size of the BitSet in bytes
     * @return a new Merklev2Footer instance
     */
    public static Merklev2Footer create(
        long chunkSize,
        long totalContentSize,
        int totalChunks,
        int leafCount,
        int capLeaf,
        int nodeCount,
        int offset,
        int internalNodeCount,
        int bitSetSize
    ) {
        // Calculate the footer length: fixed size
        byte footerLength = (byte) (FIXED_FOOTER_SIZE);
        return new Merklev2Footer(chunkSize, totalContentSize, totalChunks, leafCount, capLeaf, nodeCount, offset, internalNodeCount, bitSetSize, footerLength);
    }

    /**
     * Creates a new Merklev2Footer from a MerkleShape instance.
     * @param shape the MerkleShape instance
     * @return a new Merklev2Footer instance
     */
    public static Merklev2Footer create(MerkleShape shape) {
        return create(
            shape.getChunkSize(),
            shape.getTotalContentSize(),
            shape.getTotalChunks(),
            shape.getLeafCount(),
            shape.getCapLeaf(),
            shape.getNodeCount(),
            shape.getOffset(),
            shape.getInternalNodeCount(),
            1 // Default bitSetSize
        );
    }

    /**
     * Creates a new Merklev2Footer from a MerkleShape instance.
     * @param shape the MerkleShape instance
     * @return a new Merklev2Footer instance
     */
    public static Merklev2Footer fromMerkleShape(MerkleShape shape) {
        return create(shape);
    }

    /**
     * Serializes this footer to a ByteBuffer.
     * Uses BIG_ENDIAN byte order for consistent serialization across platforms.
     * @return a ByteBuffer containing the serialized footer
     */
    public ByteBuffer toByteBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(footerLength);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putLong(chunkSize);
        buffer.putLong(totalContentSize);
        buffer.putInt(totalChunks);
        buffer.putInt(leafCount);
        buffer.putInt(capLeaf);
        buffer.putInt(nodeCount);
        buffer.putInt(offset);
        buffer.putInt(internalNodeCount);
        buffer.putInt(bitSetSize);
        buffer.put(footerLength);
        buffer.flip();
        return buffer;
    }

    /**
     * Deserializes a Merklev2Footer from a ByteBuffer.
     * Uses BIG_ENDIAN byte order for consistent deserialization across platforms.
     * @param buffer the ByteBuffer containing the serialized footer
     * @return a new Merklev2Footer instance
     */
    public static Merklev2Footer fromByteBuffer(ByteBuffer buffer) {
        // Expect a full footer
        int expected = FIXED_FOOTER_SIZE;
        if (buffer == null) {
            throw new IllegalArgumentException("Merkle footer buffer is null");
        }

        // Create a duplicate to avoid modifying the original buffer's position and order
        ByteBuffer duplicate = buffer.duplicate();
        duplicate.order(ByteOrder.BIG_ENDIAN);

        // Check if we have enough bytes for the full footer
        if (duplicate.remaining() < expected) {
            throw new IllegalArgumentException(
                "Invalid Merkle footer buffer size: " + buffer.remaining()
                + ", expected at least " + expected);
        }

        // Read the full footer
        long chunkSize = duplicate.getLong();
        long totalContentSize = duplicate.getLong();
        int totalChunks = duplicate.getInt();
        int leafCount = duplicate.getInt();
        int capLeaf = duplicate.getInt();
        int nodeCount = duplicate.getInt();
        int offset = duplicate.getInt();
        int internalNodeCount = duplicate.getInt();
        int bitSetSize = duplicate.getInt();
        byte footerLength = duplicate.get();

        return new Merklev2Footer(chunkSize, totalContentSize, totalChunks, leafCount, capLeaf, nodeCount, offset, internalNodeCount, bitSetSize, footerLength);
    }

    /**
     * Writes this footer to a FileChannel at the specified position.
     * @param channel the FileChannel to write to
     * @param position the position in the channel to write the footer
     * @throws IOException if an I/O error occurs
     */
    public void writeToChannel(FileChannel channel, long position) throws IOException {
        ByteBuffer buffer = toByteBuffer();
        channel.write(buffer, position);
    }

    /**
     * Reads a Merklev2Footer from a FileChannel at the specified position.
     * @param channel the FileChannel to read from
     * @param position the position in the channel to read the footer from
     * @return a new Merklev2Footer instance
     * @throws IOException if an I/O error occurs
     */
    public static Merklev2Footer readFromChannel(FileChannel channel, long position) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(FIXED_FOOTER_SIZE);
        channel.read(buffer, position);
        buffer.flip();
        return fromByteBuffer(buffer);
    }

    /**
     * Creates a BaseMerkleShape from this footer.
     * @return a new BaseMerkleShape instance
     */
    public BaseMerkleShape toMerkleShape() {
        return new BaseMerkleShape(totalContentSize, chunkSize);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Merklev2Footer))
            return false;
        
        Merklev2Footer that = (Merklev2Footer) o;

        return chunkSize == that.chunkSize && 
               totalContentSize == that.totalContentSize && 
               totalChunks == that.totalChunks &&
               leafCount == that.leafCount &&
               capLeaf == that.capLeaf &&
               nodeCount == that.nodeCount &&
               offset == that.offset &&
               internalNodeCount == that.internalNodeCount &&
               bitSetSize == that.bitSetSize &&
               footerLength == that.footerLength;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(chunkSize);
        result = 31 * result + Long.hashCode(totalContentSize);
        result = 31 * result + Integer.hashCode(totalChunks);
        result = 31 * result + Integer.hashCode(leafCount);
        result = 31 * result + Integer.hashCode(capLeaf);
        result = 31 * result + Integer.hashCode(nodeCount);
        result = 31 * result + Integer.hashCode(offset);
        result = 31 * result + Integer.hashCode(internalNodeCount);
        result = 31 * result + Integer.hashCode(bitSetSize);
        result = 31 * result + footerLength;
        return result;
    }
}
