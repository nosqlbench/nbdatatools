package io.nosqlbench.nbvectors.commands.merkle;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the MockCMD_merkle class.
 */
public class MockCMD_merkleTest {

    @TempDir
    Path tempDir;

    private static final int CHUNK_SIZE = 1048576; // 1MB chunk size
    private static final int TEST_FILE_SIZE = CHUNK_SIZE * 3; // 3MB test file

    /**
     * Creates a test file with specified size and content
     */
    private Path createTestFile(int size) throws IOException {
        Path testFile = tempDir.resolve("test-file.dat");
        byte[] data = new byte[size];
        // Fill with some pattern data
        for (int i = 0; i < size; i++) {
            data[i] = (byte)(i % 256);
        }
        Files.write(testFile, data);
        return testFile;
    }

    @Test
    public void testCreateMerkleFile() throws Exception {
        // Create a test file
        Path testFile = createTestFile(TEST_FILE_SIZE);
        
        // Create the Merkle file using our mock implementation
        MockCMD_merkle cmd = new MockCMD_merkle();
        cmd.createMerkleFile(testFile, CHUNK_SIZE);
        
        // Verify the Merkle file was created
        Path merkleFile = testFile.resolveSibling(testFile.getFileName() + MockCMD_merkle.MRKL);
        assertTrue(Files.exists(merkleFile), "Merkle file should be created");
        
        // Verify the Merkle file has content
        long fileSize = Files.size(merkleFile);
        assertTrue(fileSize > 0, "Merkle file should have content");
    }

    @Test
    public void testVerifyMerkleFile() throws Exception {
        // Create a test file
        Path testFile = createTestFile(TEST_FILE_SIZE);
        
        // Create the Merkle file using our mock implementation
        MockCMD_merkle cmd = new MockCMD_merkle();
        cmd.createMerkleFile(testFile, CHUNK_SIZE);
        
        // Get the Merkle file path
        Path merkleFile = testFile.resolveSibling(testFile.getFileName() + MockCMD_merkle.MRKL);
        
        // Verify the file against its Merkle tree - should succeed
        try {
            cmd.verifyFile(testFile, merkleFile, CHUNK_SIZE);
            // If we get here, verification succeeded
            assertTrue(true, "File should verify successfully against its Merkle tree");
        } catch (Exception e) {
            fail("Verification should succeed for unmodified file: " + e.getMessage());
        }
        
        // Corrupt the file and verify it fails verification
        byte[] fileData = Files.readAllBytes(testFile);
        fileData[CHUNK_SIZE + 10] = (byte)(fileData[CHUNK_SIZE + 10] + 1); // Corrupt a byte in the second chunk
        Files.write(testFile, fileData);
        
        // Verification should now fail with an exception
        Exception exception = assertThrows(RuntimeException.class, () -> {
            cmd.verifyFile(testFile, merkleFile, CHUNK_SIZE);
        }, "Corrupted file should fail verification");
        
        assertTrue(exception.getMessage().contains("verification failed"), 
                   "Exception message should indicate verification failure");
    }
}
