package io.nosqlbench.vectordata.download.merkle;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class MerkleRAFTest {

    @TempDir
    Path tempDir;

    /**
     * Test that MerkleRAF can create its own MerklePainter instance.
     * This test will:
     * 1. Use the new constructor that takes a localPath and remoteUrl
     * 2. Verify that MerkleRAF creates and manages its own MerklePainter
     * 3. Test that reading and writing work correctly
     *
     * Note: This test is tagged as "integration" since it requires internet access
     * and downloads real data, which may take longer than a typical unit test.
     */
    @Test
    @Tag("integration")
    void testMerkleRAFWithInternalPainter() throws IOException {
        // Define the remote URL for the dataset
        String remoteUrl = "https://jvector-datasets-shared.s3.us-east-1.amazonaws.com/faed719b5520a075f2281efb8c820834/ANN_SIFT1B/bigann_query.bvecs";

        // Create a local file path for the data
        Path localPath = tempDir.resolve("bigann_query_internal.bvecs");

        try {
            // Create a MerkleRAF instance with its own internal MerklePainter
            // Use deleteOnExit=true for clean test execution
            try (MerkleRAF merkleRAF = new MerkleRAF(localPath, remoteUrl, true)) {
                // Enable test mode to prevent downloading chunks
                merkleRAF.setTestMode(true);
                // Verify that the file exists
                assertTrue(Files.exists(localPath), "Local file should exist");

                // Verify that the merkle file exists
                Path merklePath = localPath.resolveSibling(localPath.getFileName().toString() + ".mrkl");
                assertTrue(Files.exists(merklePath), "Merkle file should exist");

                // Verify that the reference merkle file exists
                Path referenceTreePath = localPath.resolveSibling(localPath.getFileName().toString() + ".mref");
                assertTrue(Files.exists(referenceTreePath), "Reference merkle file should exist");

                // Test writing and reading back
                merkleRAF.seek(0);
                byte[] writeData = new byte[100];
                for (int i = 0; i < writeData.length; i++) {
                    writeData[i] = (byte) (i + 10);
                }
                merkleRAF.write(writeData);

                // Read back what we wrote
                merkleRAF.seek(0);
                byte[] readBack = new byte[100];
                int bytesRead = merkleRAF.read(readBack);
                assertEquals(100, bytesRead, "Should read the requested number of bytes");
                assertArrayEquals(writeData, readBack, "Should read back what was written");

                // Print some information about the file
                System.out.println("Successfully tested MerkleRAF with internal MerklePainter");
                System.out.println("File size: " + merkleRAF.length() + " bytes");
            }
            // The MerklePainter should be automatically closed when MerkleRAF is closed
        } catch (java.io.FileNotFoundException e) {
            // This might happen if the remote file doesn't exist
            System.out.println("Remote file not found: " + e.getMessage());
            // Skip the test rather than fail it
            assumeTrue(false, "Remote file not available");
        } catch (java.io.IOException e) {
            System.out.println("Error during test: " + e.getMessage());
            throw e;
        }
    }
}
