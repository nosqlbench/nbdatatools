package io.nosqlbench.vectordata.download.merkle;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the MerkleFooter comparison functionality.
 * This test verifies that we can correctly read and compare footers from merkle tree files.
 */
class MerkleFooterComparisonTest {

    @TempDir
    Path tempDir;

    /**
     * Creates a test merkle tree file with a known footer.
     */
    private Path createTestMerkleFile(String name, long chunkSize, long totalSize, byte[] digest) throws IOException {
        // Create a simple merkle tree file
        Path merklePath = tempDir.resolve(name);
        
        // Create some test data (leaf hashes)
        byte[] leafData = new byte[32 * 10]; // 10 leaf hashes of 32 bytes each
        Arrays.fill(leafData, (byte) 1);
        
        // Create a footer
        MerkleFooter footer = MerkleFooter.create(chunkSize, totalSize, digest);
        ByteBuffer footerBuffer = footer.toByteBuffer();
        
        // Write the file
        try (FileChannel channel = FileChannel.open(merklePath, 
                StandardOpenOption.CREATE, 
                StandardOpenOption.WRITE, 
                StandardOpenOption.TRUNCATE_EXISTING)) {
            // Write the leaf data
            channel.write(ByteBuffer.wrap(leafData));
            
            // Write the footer
            channel.write(footerBuffer);
        }
        
        return merklePath;
    }
    
    /**
     * Reads the footer from a merkle tree file.
     */
    private MerkleFooter readFooter(Path merklePath) throws IOException {
        // Get the file size
        long fileSize = Files.size(merklePath);
        
        // Read the footer length (last byte)
        byte footerLength;
        try (FileChannel channel = FileChannel.open(merklePath, StandardOpenOption.READ)) {
            ByteBuffer lengthBuffer = ByteBuffer.allocate(1);
            channel.position(fileSize - 1);
            channel.read(lengthBuffer);
            lengthBuffer.flip();
            footerLength = lengthBuffer.get();
        }
        
        // Read the entire footer
        ByteBuffer footerBuffer = ByteBuffer.allocate(footerLength);
        try (FileChannel channel = FileChannel.open(merklePath, StandardOpenOption.READ)) {
            channel.position(fileSize - footerLength);
            channel.read(footerBuffer);
            footerBuffer.flip();
        }
        
        // Parse the footer
        return MerkleFooter.fromByteBuffer(footerBuffer);
    }
    
    /**
     * Test that verifies we can correctly read and compare footers from merkle tree files.
     */
    @Test
    void testFooterComparison() throws IOException {
        // Create test digests
        byte[] digest1 = new byte[MerkleFooter.DIGEST_SIZE];
        Arrays.fill(digest1, (byte) 2);
        
        byte[] digest2 = new byte[MerkleFooter.DIGEST_SIZE];
        Arrays.fill(digest2, (byte) 3);
        
        // Create test merkle files with different footers
        Path file1 = createTestMerkleFile("test1.mrkl", 4096, 40960, digest1);
        Path file2 = createTestMerkleFile("test2.mrkl", 4096, 40960, digest2);
        Path file3 = createTestMerkleFile("test3.mrkl", 8192, 40960, digest1);
        Path file4 = createTestMerkleFile("test4.mrkl", 4096, 81920, digest1);
        Path file5 = createTestMerkleFile("test5.mrkl", 4096, 40960, digest1);
        
        // Read the footers
        MerkleFooter footer1 = readFooter(file1);
        MerkleFooter footer2 = readFooter(file2);
        MerkleFooter footer3 = readFooter(file3);
        MerkleFooter footer4 = readFooter(file4);
        MerkleFooter footer5 = readFooter(file5);
        
        // Verify the footers were read correctly
        assertEquals(4096, footer1.chunkSize());
        assertEquals(40960, footer1.totalSize());
        assertArrayEquals(digest1, footer1.digest());
        
        assertEquals(4096, footer2.chunkSize());
        assertEquals(40960, footer2.totalSize());
        assertArrayEquals(digest2, footer2.digest());
        
        assertEquals(8192, footer3.chunkSize());
        assertEquals(40960, footer3.totalSize());
        assertArrayEquals(digest1, footer3.digest());
        
        assertEquals(4096, footer4.chunkSize());
        assertEquals(81920, footer4.totalSize());
        assertArrayEquals(digest1, footer4.digest());
        
        assertEquals(4096, footer5.chunkSize());
        assertEquals(40960, footer5.totalSize());
        assertArrayEquals(digest1, footer5.digest());
        
        // Test footer comparison
        // Different digest
        assertFalse(Arrays.equals(footer1.digest(), footer2.digest()));
        
        // Different chunk size
        assertNotEquals(footer1.chunkSize(), footer3.chunkSize());
        
        // Different total size
        assertNotEquals(footer1.totalSize(), footer4.totalSize());
        
        // Same footer
        assertEquals(footer1.chunkSize(), footer5.chunkSize());
        assertEquals(footer1.totalSize(), footer5.totalSize());
        assertArrayEquals(footer1.digest(), footer5.digest());
    }
}
