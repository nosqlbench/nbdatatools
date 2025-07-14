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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for verifying the integrity of Merkle tree files.
 * This class contains the verification logic that was previously in the MerkleTree class.
 */
public class MerkleTreeFileVerificationTest {

    @TempDir
    Path tempDir;

    /**
     * Tests that a valid Merkle tree file can be verified successfully.
     */
    @Test
    void testVerifyValidMerkleFile() throws IOException {
        // Create a simple Merkle tree
        byte[] data = new byte[1024]; // 1KB of data
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);
        MerkleTree tree = MerkleTree.fromData(buffer);

        // Save the tree to a file
        Path merkleFile = tempDir.resolve("valid_merkle.mrkl");
        tree.save(merkleFile);

        // Verify the file
        assertDoesNotThrow(() -> verifyWrittenMerkleFile(merkleFile, null));
    }

    /**
     * Tests that verification fails for an empty file.
     */
    @Test
    void testVerifyEmptyFile() throws IOException {
        // Create an empty file
        Path emptyFile = tempDir.resolve("empty.mrkl");
        Files.createFile(emptyFile);

        // Verify the file
        IOException exception = assertThrows(IOException.class, 
            () -> verifyWrittenMerkleFile(emptyFile, null));
        assertEquals("File is empty", exception.getMessage());
    }

    /**
     * Tests that verification fails for a file with invalid footer.
     */
    @Test
    void testVerifyInvalidFooter() throws IOException {
        // Create a file with invalid content
        Path invalidFile = tempDir.resolve("invalid.mrkl");
        Files.write(invalidFile, new byte[100]); // Just some random bytes

        // Verify the file
        IOException exception = assertThrows(IOException.class, 
            () -> verifyWrittenMerkleFile(invalidFile, null));
        assertTrue(exception.getMessage().contains("Invalid merkle tree file format"));
    }

    /**
     * Verifies the basic integrity of a written Merkle tree file by checking its footer and data.
     * This optimized version avoids reading the entire file into memory for large files.
     * 
     * @param path The path to the merkle tree file
     * @param fileChannel Optional file channel to use for reading. If null, a new channel will be opened.
     * @throws IOException If the file is empty, truncated, or the data is invalid.
     */
    private void verifyWrittenMerkleFile(Path path, FileChannel fileChannel) throws IOException {
        // Ensure file exists and is non-empty
        long fileSize = Files.size(path);
        if (fileSize == 0) {
            throw new IOException("File is empty");
        }

        boolean closeChannel = false;
        FileChannel ch;

        if (fileChannel != null) {
            ch = fileChannel;
        } else {
            ch = FileChannel.open(path, StandardOpenOption.READ);
            closeChannel = true;
        }

        try {
            // Read footer buffer using the provided or newly opened channel
            ByteBuffer footerBuf;
            try {
                footerBuf = readFooterBuffer(path, fileSize, ch);
            } catch (IOException | RuntimeException e) {
                throw new IOException("Verification failed: Invalid merkle tree file format", e);
            }

            MerkleFooter footer;
            try {
                footer = MerkleFooter.fromByteBuffer(footerBuf);
            } catch (IllegalArgumentException e) {
                throw new IOException("Verification failed: Invalid merkle tree footer", e);
            }

            // Check if data size is valid
            int fl = footer.footerLength();
            int bitSetSize = footer.bitSetSize();
            long totalDataSize = fileSize - fl;
            if (totalDataSize < 0) {
                throw new IOException("Verification failed: Invalid data size " + totalDataSize);
            }

            // Handle empty files (just a footer, no data)
            if (totalDataSize == 0) {
                System.out.println("Merkle tree file " + path + " has no data (only footer). This might be an empty tree.");
                return; // Skip further validation for empty files
            }

            // Calculate the hash data size (excluding BitSet data)
            long hashDataSize;

            // Check if this is a file created with the old format (bitSetSize = 0)
            // or with the new format (bitSetSize = 1 but no actual bitset)
            // If the total data size is a multiple of MerkleTree.HASH_SIZE,
            // then there's likely no bitset data in the file
            if (bitSetSize == 1 && totalDataSize % MerkleTree.HASH_SIZE == 0) {
                // This is likely a file created with the old format or with the new format but no actual bitset
                // Adjust the calculation to not subtract the bitSetSize
                hashDataSize = totalDataSize;
                System.out.println("Detected file with no actual bitset data during verification. Adjusted hash data size: " + hashDataSize);
            } else {
                // Normal case - subtract the bitSetSize from the total data size
                hashDataSize = totalDataSize - bitSetSize;
            }

            // Check if hash data size is a multiple of hash size
            if (hashDataSize % MerkleTree.HASH_SIZE != 0) {
                throw new IOException("Verification failed: Hash data size is not a multiple of hash size");
            }

            // For large files, we'll verify the file structure without reading the entire file
            // We'll check the first and last hash blocks to ensure they're readable
            if (totalDataSize > 100 * 1024 * 1024) { // For files larger than 100MB
                System.out.println("Large file detected during verification. Using optimized verification.");

                // Check first hash block
                ByteBuffer firstHashBlock = ByteBuffer.allocateDirect(MerkleTree.HASH_SIZE);
                int bytesRead = ch.read(firstHashBlock, 0);
                if (bytesRead != MerkleTree.HASH_SIZE) {
                    throw new IOException("Verification failed: Could not read first hash block");
                }

                // Check last hash block in the hash data region
                long lastHashPosition = hashDataSize - MerkleTree.HASH_SIZE;
                if (lastHashPosition > 0) {
                    ByteBuffer lastHashBlock = ByteBuffer.allocateDirect(MerkleTree.HASH_SIZE);
                    bytesRead = ch.read(lastHashBlock, lastHashPosition);
                    if (bytesRead != MerkleTree.HASH_SIZE) {
                        throw new IOException("Verification failed: Could not read last hash block");
                    }
                }
            } else {
                // For smaller files, we can still read the entire data region
                // but use memory mapping for better performance
                try {
                    // Map the data region into memory
                    MappedByteBuffer mappedBuffer = ch.map(FileChannel.MapMode.READ_ONLY, 0, totalDataSize);

                    // Just accessing the buffer is enough to verify it's readable
                    // We don't need to read all the bytes
                    mappedBuffer.load();

                    // We don't need to explicitly clean up the mapped buffer
                    // The garbage collector will handle it when the buffer is no longer referenced
                    // Just ensure we've accessed the buffer to verify it's readable
                    mappedBuffer.force();
                } catch (Exception e) {
                    throw new IOException("Verification failed: Could not map data region into memory", e);
                }
            }
        } finally {
            // Close the channel if we opened it
            if (closeChannel) {
                ch.close();
            }
        }
    }

    /**
     * Reads the footer buffer from a merkle tree file.
     * Uses BIG_ENDIAN byte order for consistent reading across platforms.
     * This optimized version reduces I/O operations by reading the footer in a single operation when possible.
     * 
     * @param path The path to the merkle tree file
     * @param fileSize The size of the file
     * @param fileChannel The file channel to use for reading
     * @return The footer buffer
     * @throws IOException If an I/O error occurs
     */
    private ByteBuffer readFooterBuffer(Path path, long fileSize, FileChannel fileChannel) throws IOException {
        // Read the footer length byte from the end of the file
        ByteBuffer len = ByteBuffer.allocateDirect(1);
        len.order(java.nio.ByteOrder.BIG_ENDIAN);

        // Use absolute positioning for thread safety
        int bytesRead = fileChannel.read(len, fileSize - 1);
        if (bytesRead != 1) {
            throw new IOException("Failed to read footer length byte from file: " + path);
        }

        len.flip();
        byte fl = len.get();

        // Validate footer length
        if (fl <= 0 || fl > fileSize) {
            throw new IOException("Invalid footer length: " + fl);
        }

        // Read the full footer in a single operation if possible
        ByteBuffer footerBuffer = ByteBuffer.allocateDirect(fl);
        footerBuffer.order(java.nio.ByteOrder.BIG_ENDIAN);

        // Position at the start of the footer
        long footerPosition = fileSize - fl;

        // Try to read the entire footer in one operation
        bytesRead = fileChannel.read(footerBuffer, footerPosition);

        // If we couldn't read the entire footer in one go, read it incrementally
        if (bytesRead < fl) {
            int totalBytesRead = bytesRead;
            while (footerBuffer.hasRemaining()) {
                int read = fileChannel.read(footerBuffer, footerPosition + totalBytesRead);
                if (read <= 0) {
                    throw new IOException("Unexpected end of file reading merkle footer, expected " + 
                                         footerBuffer.remaining() + " more bytes");
                }
                totalBytesRead += read;
            }
        }

        // Prepare the buffer for reading
        footerBuffer.flip();

        return footerBuffer;
    }
}
