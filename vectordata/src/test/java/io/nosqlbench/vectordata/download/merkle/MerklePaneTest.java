package io.nosqlbench.vectordata.download.merkle;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MerklePaneTest {
    @TempDir
    Path tempDir;

    private static final int CHUNK_SIZE = 16; // Power of two
    private static final int TEST_FILE_SIZE = 128; // Multiple of chunk size for cleaner tests
    private static final byte TEST_DATA_VALUE = (byte)1;

    private Path createTestFile(byte[] data) throws IOException {
        Path file = tempDir.resolve("test.dat");
        Files.write(file, data);
        return file;
    }

    private byte[] createTestData(int size, byte value) {
        byte[] data = new byte[size];
        Arrays.fill(data, value);
        return data;
    }

    private Path createMerkleFile(Path dataFile, int chunkSize) throws IOException {
        Path merkleFile = tempDir.resolve("test.dat" + MerklePane.MRKL);
        
        // Create and save Merkle tree
        ByteBuffer buffer = ByteBuffer.wrap(Files.readAllBytes(dataFile));
        MerkleTree tree = MerkleTree.fromData(
            buffer,
            chunkSize,
            new MerkleRange(0, buffer.capacity())
        );
        tree.save(merkleFile);
        
        return merkleFile;
    }

    @Test
    void testConstructorWithSinglePath() throws IOException {
        byte[] data = createTestData(TEST_FILE_SIZE, TEST_DATA_VALUE);
        Path dataFile = createTestFile(data);
        Path merkleFile = createMerkleFile(dataFile, CHUNK_SIZE);

        try (MerklePane window = new MerklePane(dataFile)) {
            assertEquals(TEST_FILE_SIZE, window.fileSize());
            assertEquals(dataFile, window.filePath());
        }
    }

    @Test
    void testBasicOperations() throws IOException {
        byte[] data = createTestData(TEST_FILE_SIZE, TEST_DATA_VALUE);
        Path dataFile = createTestFile(data);
        Path merkleFile = createMerkleFile(dataFile, CHUNK_SIZE);

        try (MerklePane window = new MerklePane(dataFile, merkleFile)) {
            assertEquals(TEST_FILE_SIZE, window.fileSize());
            assertEquals(dataFile, window.filePath());
            
            // Test chunk reading
            ByteBuffer chunk = window.readChunk(0);
            assertEquals(CHUNK_SIZE, chunk.remaining());
            assertEquals(TEST_DATA_VALUE, chunk.get(0));
            
            // Test last chunk reading
            int lastChunkIndex = (TEST_FILE_SIZE / CHUNK_SIZE) - 1;
            ByteBuffer lastChunk = window.readChunk(lastChunkIndex);
            assertEquals(CHUNK_SIZE, lastChunk.remaining());
            assertEquals(TEST_DATA_VALUE, lastChunk.get(0));
            
            // Test range reading
            int rangeStart = 5;
            int rangeLength = 20;
            ByteBuffer range = window.readRange(rangeStart, rangeLength);
            assertEquals(rangeLength, range.remaining());
            assertEquals(TEST_DATA_VALUE, range.get(0));
            
            // Test chunk verification
            assertTrue(window.verifyChunk(0), "First chunk should verify successfully");
            assertTrue(window.verifyChunk(lastChunkIndex), "Last chunk should verify successfully");
        }
    }

    @Test
    void testVerifyChunkWithCorruption() throws IOException {
        byte[] data = createTestData(TEST_FILE_SIZE, TEST_DATA_VALUE);
        Path dataFile = createTestFile(data);
        Path merkleFile = createMerkleFile(dataFile, CHUNK_SIZE);

        // Corrupt specific chunks in the data file
        data[0] = 2; // Corrupt first chunk
        data[CHUNK_SIZE + 1] = 2; // Corrupt second chunk
        Files.write(dataFile, data);

        try (MerklePane window = new MerklePane(dataFile, merkleFile)) {
            assertFalse(window.verifyChunk(0), "First chunk should fail verification");
            assertFalse(window.verifyChunk(1), "Second chunk should fail verification");
            assertTrue(window.verifyChunk(2), "Third chunk should verify successfully");
        }
    }

    @Test
    void testInvalidOperations() throws IOException {
        byte[] data = createTestData(TEST_FILE_SIZE, TEST_DATA_VALUE);
        Path dataFile = createTestFile(data);
        Path merkleFile = createMerkleFile(dataFile, CHUNK_SIZE);

        try (MerklePane window = new MerklePane(dataFile, merkleFile)) {
            // Test invalid chunk indices
            assertThrows(IllegalArgumentException.class, 
                () -> window.readChunk(-1),
                "Negative chunk index should throw exception"
            );
            
            int invalidChunkIndex = TEST_FILE_SIZE / CHUNK_SIZE;
            assertThrows(IllegalArgumentException.class, 
                () -> window.readChunk(invalidChunkIndex),
                "Chunk index beyond file size should throw exception"
            );

            // Test invalid ranges
            assertThrows(IllegalArgumentException.class, 
                () -> window.readRange(-1, CHUNK_SIZE),
                "Negative range start should throw exception"
            );
            
            assertThrows(IllegalArgumentException.class, 
                () -> window.readRange(0, -1),
                "Negative range length should throw exception"
            );
            
            assertThrows(IllegalArgumentException.class, 
                () -> window.readRange(TEST_FILE_SIZE - 5, 10),
                "Range extending beyond file size should throw exception"
            );
        }
    }

    @Test
    void testMissingOrInvalidFiles() {
        Path nonExistentFile = tempDir.resolve("nonexistent.dat");
        
        assertThrows(RuntimeException.class, 
            () -> new MerklePane(nonExistentFile),
            "Constructor should throw exception for non-existent file"
        );
    }

    @Test
    void testResourceLeaks() throws IOException {
        byte[] data = createTestData(TEST_FILE_SIZE, TEST_DATA_VALUE);
        Path dataFile = createTestFile(data);
        Path merkleFile = createMerkleFile(dataFile, CHUNK_SIZE);

        MerklePane window = new MerklePane(dataFile, merkleFile);
        window.close();

        assertThrows(IOException.class, 
            () -> window.readChunk(0),
            "Operations after close should throw exception"
        );
    }
}
