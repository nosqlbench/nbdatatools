package io.nosqlbench.vectordata.download.merkle;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Provides windowed access to a file along with its associated Merkle tree.
 * Maintains an open random access channel to the file for efficient reading.
 */
public class MerklePane implements Closeable {
    private final Path filePath;
    private final FileChannel channel;
    private final MerkleTree merkleTree;
    private final long fileSize;

    /**
     * Creates a new MerkleWindow for the given file and its associated Merkle tree
     * @param filePath Path to the data file
     * @param merklePath Path to the Merkle tree file
     * @throws IOException If there's an error opening the files
     */
    public MerklePane(Path filePath, Path merklePath) throws IOException {
        this.filePath = filePath;
        this.channel = FileChannel.open(filePath, StandardOpenOption.READ);
        this.fileSize = channel.size();
        this.merkleTree = MerkleTree.load(merklePath);
        
        // Validate that the Merkle tree matches the file size
        if (merkleTree.totalSize() != fileSize) {
            close();
            throw new IOException(String.format(
                "Merkle tree size (%d) does not match file size (%d)",
                merkleTree.totalSize(), fileSize
            ));
        }
    }

    /**
     * Gets the Merkle tree associated with this window
     * @return The MerkleTree instance
     */
    public MerkleTree merkleTree() {
        return merkleTree;
    }

    /**
     * Gets the total size of the file
     * @return The file size in bytes
     */
    public long fileSize() {
        return fileSize;
    }

    /**
     * Gets the path to the file
     * @return The file path
     */
    public Path filePath() {
        return filePath;
    }

    /**
     * Reads a chunk of data from the file
     * @param chunkIndex The index of the chunk to read
     * @return ByteBuffer containing the chunk data
     * @throws IOException If there's an error reading the file
     * @throws IllegalArgumentException If the chunk index is invalid
     */
    public ByteBuffer readChunk(int chunkIndex) throws IOException {
        MerkleTree.NodeBoundary bounds = merkleTree.getBoundariesForLeaf(chunkIndex);
        long chunkSize = bounds.end() - bounds.start();
        
        ByteBuffer buffer = ByteBuffer.allocate((int)chunkSize);
        channel.position(bounds.start());
        channel.read(buffer);
        buffer.flip();
        
        return buffer;
    }

    /**
     * Reads a range of bytes from the file
     * @param start Starting byte offset
     * @param length Number of bytes to read
     * @return ByteBuffer containing the requested data
     * @throws IOException If there's an error reading the file
     * @throws IllegalArgumentException If the range is invalid
     */
    public ByteBuffer readRange(long start, int length) throws IOException {
        if (start < 0 || start + length > fileSize) {
            throw new IllegalArgumentException(String.format(
                "Invalid range [%d, %d] for file of size %d",
                start, start + length, fileSize
            ));
        }

        ByteBuffer buffer = ByteBuffer.allocate(length);
        channel.position(start);
        channel.read(buffer);
        buffer.flip();
        
        return buffer;
    }

    /**
     * Verifies a specific chunk against the Merkle tree
     * @param chunkIndex The index of the chunk to verify
     * @return true if the chunk matches the Merkle tree, false otherwise
     * @throws IOException If there's an error reading the file
     */
    public boolean verifyChunk(int chunkIndex) throws IOException {
        ByteBuffer chunkData = readChunk(chunkIndex);
        
        // Create a single-chunk Merkle tree from the data
        MerkleRange chunkRange = new MerkleRange(
            chunkIndex * merkleTree.chunkSize(),
            Math.min(fileSize, (chunkIndex + 1) * merkleTree.chunkSize())
        );
        
        MerkleTree chunkTree = MerkleTree.fromData(
            chunkData,
            merkleTree.chunkSize(),
            chunkRange
        );

        // Compare the chunk's hash with the original tree
        return merkleTree.findMismatchedChunks(chunkTree).isEmpty();
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}