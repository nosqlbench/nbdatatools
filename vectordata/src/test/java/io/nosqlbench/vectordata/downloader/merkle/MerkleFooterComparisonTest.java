package io.nosqlbench.vectordata.downloader.merkle;

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


import io.nosqlbench.vectordata.merkle.MerkleFooter;
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
    private Path createTestMerkleFile(String name, long chunkSize, long totalSize) throws IOException {
        // Create a simple merkle tree file
        Path merklePath = tempDir.resolve(name);

        // Create some test data (leaf hashes)
        byte[] leafData = new byte[32 * 10]; // 10 leaf hashes of 32 bytes each
        Arrays.fill(leafData, (byte) 1);

        // Create a footer
        MerkleFooter footer = MerkleFooter.create(chunkSize, totalSize);
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
        // Create test merkle files with different footers
        Path file1 = createTestMerkleFile("test1.mrkl", 4096, 40960);
        Path file2 = createTestMerkleFile("test2.mrkl", 4096, 40960);
        Path file3 = createTestMerkleFile("test3.mrkl", 8192, 40960);
        Path file4 = createTestMerkleFile("test4.mrkl", 4096, 81920);
        Path file5 = createTestMerkleFile("test5.mrkl", 4096, 40960);

        // Read the footers
        MerkleFooter footer1 = readFooter(file1);
        MerkleFooter footer2 = readFooter(file2);
        MerkleFooter footer3 = readFooter(file3);
        MerkleFooter footer4 = readFooter(file4);
        MerkleFooter footer5 = readFooter(file5);

        // Verify the footers were read correctly
        assertEquals(4096, footer1.chunkSize());
        assertEquals(40960, footer1.totalSize());

        assertEquals(4096, footer2.chunkSize());
        assertEquals(40960, footer2.totalSize());

        assertEquals(8192, footer3.chunkSize());
        assertEquals(40960, footer3.totalSize());

        assertEquals(4096, footer4.chunkSize());
        assertEquals(81920, footer4.totalSize());

        assertEquals(4096, footer5.chunkSize());
        assertEquals(40960, footer5.totalSize());

        // Different chunk size
        assertNotEquals(footer1.chunkSize(), footer3.chunkSize());

        // Different total size
        assertNotEquals(footer1.totalSize(), footer4.totalSize());

        // Same footer
        assertEquals(footer1.chunkSize(), footer5.chunkSize());
        assertEquals(footer1.totalSize(), footer5.totalSize());
    }
}
