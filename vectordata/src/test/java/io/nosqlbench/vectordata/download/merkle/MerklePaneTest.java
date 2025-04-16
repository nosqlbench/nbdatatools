package io.nosqlbench.vectordata.download.merkle;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class MerklePaneTest {
    @TempDir
    Path tempDir;

    private Path createTestFile(byte[] data) throws IOException {
        Path file = tempDir.resolve("test.dat");
        Files.write(file, data);
        return file;
    }

    private Path createMerkleFile(Path dataFile) throws IOException {
        Path merkleFile = tempDir.resolve("test.dat.merkle");
        
        // Create and save Merkle tree
        ByteBuffer buffer = ByteBuffer.wrap(Files.readAllBytes(dataFile));
        MerkleTree tree = MerkleTree.fromData(
            buffer,
            10, // chunk size
            new MerkleRange(0, buffer.capacity())
        );
        tree.save(merkleFile);
        
        return merkleFile;
    }

    @Test
    void testBasicOperations() throws IOException {
        // Create test data
        byte[] data = new byte[100];
        Arrays.fill(data, (byte)1);
        
        // Create test files
        Path dataFile = createTestFile(data);
        Path merkleFile = createMerkleFile(dataFile);

        // Test MerkleWindow operations
        try (MerklePane window = new MerklePane(dataFile, merkleFile)) {
            assertEquals(100, window.fileSize());
            assertEquals(dataFile, window.filePath());
            
            // Test chunk reading
            ByteBuffer chunk = window.readChunk(0);
            assertEquals(10, chunk.remaining()); // First chunk should be 10 bytes
            assertEquals(1, chunk.get(0)); // Should contain our test data
            
            // Test range reading
            ByteBuffer range = window.readRange(5, 20);
            assertEquals(20, range.remaining());
            
            // Test chunk verification
            assertTrue(window.verifyChunk(0));
        }
    }

    @Test
    void testCorruptedData() throws IOException {
        // Create initial test data
        byte[] data = new byte[100];
        Arrays.fill(data, (byte)1);
        
        // Create test files
        Path dataFile = createTestFile(data);
        Path merkleFile = createMerkleFile(dataFile);

        // Corrupt the data file
        data[15] = 2; // Modify second chunk
        Files.write(dataFile, data);

        // Test corruption detection
        try (MerklePane window = new MerklePane(dataFile, merkleFile)) {
            assertTrue(window.verifyChunk(0)); // First chunk should be intact
            assertFalse(window.verifyChunk(1)); // Second chunk should fail verification
        }
    }

    @Test
    void testInvalidOperations() throws IOException {
        byte[] data = new byte[100];
        Path dataFile = createTestFile(data);
        Path merkleFile = createMerkleFile(dataFile);

        try (MerklePane window = new MerklePane(dataFile, merkleFile)) {
            // Test invalid chunk index
            assertThrows(IllegalArgumentException.class, () -> window.readChunk(-1));
            assertThrows(IllegalArgumentException.class, () -> window.readChunk(100));

            // Test invalid range
            assertThrows(IllegalArgumentException.class, () -> window.readRange(-1, 10));
            assertThrows(IllegalArgumentException.class, () -> window.readRange(95, 10));
        }
    }
}