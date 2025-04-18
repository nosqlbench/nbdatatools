package io.nosqlbench.vectordata.download.merkle;

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
