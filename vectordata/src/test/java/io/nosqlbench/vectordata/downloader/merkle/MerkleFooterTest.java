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

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class MerkleFooterTest {

    @Test
    void testCreateFooter() {
        // Create test data
        long chunkSize = 4096;
        long totalSize = 1024 * 1024;
        byte[] digest = new byte[MerkleFooter.DIGEST_SIZE];
        Arrays.fill(digest, (byte) 1);

        // Create footer
        MerkleFooter footer = MerkleFooter.create(chunkSize, totalSize, digest);

        // Verify properties
        assertEquals(chunkSize, footer.chunkSize());
        assertEquals(totalSize, footer.totalSize());
        assertArrayEquals(digest, footer.digest());
        assertEquals(MerkleFooter.FIXED_FOOTER_SIZE + MerkleFooter.DIGEST_SIZE, footer.footerLength());
    }

    @Test
    void testSerializeDeserialize() {
        // Create test data
        long chunkSize = 4096;
        long totalSize = 1024 * 1024;
        byte[] digest = new byte[MerkleFooter.DIGEST_SIZE];
        Arrays.fill(digest, (byte) 1);

        // Create footer
        MerkleFooter original = MerkleFooter.create(chunkSize, totalSize, digest);

        // Serialize
        ByteBuffer buffer = original.toByteBuffer();

        // Deserialize
        MerkleFooter deserialized = MerkleFooter.fromByteBuffer(buffer);

        // Verify properties
        assertEquals(original.chunkSize(), deserialized.chunkSize());
        assertEquals(original.totalSize(), deserialized.totalSize());
        assertArrayEquals(original.digest(), deserialized.digest());
        assertEquals(original.footerLength(), deserialized.footerLength());
    }

    @Test
    void testCalculateDigest() {
        // Create test data
        ByteBuffer data = ByteBuffer.allocate(100);
        for (int i = 0; i < data.capacity(); i++) {
            data.put((byte) i);
        }
        data.flip();

        // Calculate digest
        byte[] digest = MerkleFooter.calculateDigest(data);

        // Verify digest is not null and has the correct length
        assertNotNull(digest);
        assertEquals(MerkleFooter.DIGEST_SIZE, digest.length);

        // Verify digest is consistent
        byte[] digest2 = MerkleFooter.calculateDigest(data);
        assertArrayEquals(digest, digest2);

        // Create a modified copy of the data
        ByteBuffer modifiedData = ByteBuffer.allocate(100);
        for (int i = 0; i < modifiedData.capacity(); i++) {
            modifiedData.put((byte) i);
        }
        modifiedData.flip();
        modifiedData.put(0, (byte) 99); // Change the first byte

        // Verify digest changes when data changes
        byte[] digest3 = MerkleFooter.calculateDigest(modifiedData);
        assertFalse(Arrays.equals(digest, digest3));
    }

    @Test
    void testVerifyDigest() {
        // Create test data
        ByteBuffer data = ByteBuffer.allocate(100);
        for (int i = 0; i < data.capacity(); i++) {
            data.put((byte) i);
        }
        data.flip();

        // Calculate digest
        byte[] digest = MerkleFooter.calculateDigest(data);

        // Create footer
        MerkleFooter footer = MerkleFooter.create(4096, 1024 * 1024, digest);

        // Verify digest
        assertTrue(footer.verifyDigest(data));

        // Create a modified copy of the data
        ByteBuffer modifiedData = ByteBuffer.allocate(100);
        for (int i = 0; i < modifiedData.capacity(); i++) {
            modifiedData.put((byte) i);
        }
        modifiedData.flip();
        modifiedData.put(0, (byte) 99); // Change the first byte

        // Verify digest fails with modified data
        assertFalse(footer.verifyDigest(modifiedData));
    }
}
