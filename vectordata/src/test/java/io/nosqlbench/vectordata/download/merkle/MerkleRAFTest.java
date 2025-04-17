package io.nosqlbench.vectordata.download.merkle;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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
                // No need to set test mode anymore
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

    /**
     * Test that MerkleRAF can prebuffer data asynchronously.
     * This test will:
     * 1. Create a MerkleRAF instance
     * 2. Call prebuffer to asynchronously download a range of data
     * 3. Verify that the prebuffer operation completes successfully
     * 4. Verify that the data can be read without additional downloads
     *
     * Note: This test is tagged as "integration" since it requires internet access
     * and downloads real data, which may take longer than a typical unit test.
     */
    @Test
    @Tag("integration")
    void testPrebuffer() throws Exception {
        // Define the remote URL for the dataset
        String remoteUrl = "https://jvector-datasets-shared.s3.us-east-1.amazonaws.com/faed719b5520a075f2281efb8c820834/ANN_SIFT1B/bigann_query.bvecs";

        // Create a local file path for the data
        Path localPath = tempDir.resolve("bigann_query_prebuffer.bvecs");

        try {
            // Create a MerkleRAF instance with its own internal MerklePainter
            // Use deleteOnExit=true for clean test execution
            try (MerkleRAF merkleRAF = new MerkleRAF(localPath, remoteUrl, true)) {
                // Verify that the file exists
                assertTrue(Files.exists(localPath), "Local file should exist");

                // Define a range to prebuffer
                long startPosition = 1024;
                long length = 2048; // 2KB

                // Call prebuffer to asynchronously download the range
                CompletableFuture<Void> future = merkleRAF.prebuffer(startPosition, length);

                // Wait for the prebuffer operation to complete
                future.get(30, TimeUnit.SECONDS);

                // Now read from the prebuffered range - this should not trigger any downloads
                merkleRAF.seek(startPosition);
                byte[] buffer = new byte[(int)length];
                int bytesRead = merkleRAF.read(buffer);

                // Verify that we read the expected number of bytes
                assertEquals(length, bytesRead, "Should read the requested number of bytes");

                // Verify that the buffer contains data (not all zeros)
                boolean hasNonZeroData = false;
                for (byte b : buffer) {
                    if (b != 0) {
                        hasNonZeroData = true;
                        break;
                    }
                }
                assertTrue(hasNonZeroData, "Buffer should contain non-zero data");

                System.out.println("Successfully tested MerkleRAF.prebuffer");
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
