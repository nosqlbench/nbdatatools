package io.nosqlbench.vectordata.download.merkle;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * A record type for the footer of a merkle tree file.
 * <p>
 * The footer contains metadata about the merkle tree, including:
 * - chunkSize: the size of each chunk in bytes
 * - totalSize: the total size of the data in bytes
 * - digest: a hash of the merkle tree data for integrity verification
 * - footerLength: the length of the footer in bytes
 */
public record MerkleFooter(long chunkSize, long totalSize, byte[] digest, byte footerLength) {

    /**
     * The size of the digest in bytes (SHA-256 = 32 bytes)
     */
    public static final int DIGEST_SIZE = 32;

    /**
     * The fixed size of the footer in bytes (excluding the variable-length digest)
     * 8 bytes for chunkSize + 8 bytes for totalSize + 1 byte for footerLength
     */
    public static final int FIXED_FOOTER_SIZE = Long.BYTES * 2 + Byte.BYTES;

    /**
     * Creates a new MerkleFooter with the given parameters and calculates the footer length.
     *
     * @param chunkSize the chunk size in bytes
     * @param totalSize the total size of the data in bytes
     * @param digest    the digest of the merkle tree data
     * @return a new MerkleFooter instance
     */
    public static MerkleFooter create(long chunkSize, long totalSize, byte[] digest) {
        // Calculate the footer length: fixed size + digest size
        byte footerLength = (byte) (FIXED_FOOTER_SIZE + DIGEST_SIZE);
        return new MerkleFooter(chunkSize, totalSize, digest, footerLength);
    }

    /**
     * Calculates the digest of the merkle tree data.
     *
     * @param treeData the merkle tree data to digest
     * @return the digest as a byte array
     */
    public static byte[] calculateDigest(ByteBuffer treeData) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            // Create a duplicate to avoid modifying the original buffer
            ByteBuffer buffer = treeData.duplicate();
            buffer.position(0); // Reset position to beginning

            // Create a byte array from the buffer
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);

            // Update the digest with the bytes
            digest.update(bytes);
            return digest.digest();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

    /**
     * Serializes this footer to a ByteBuffer.
     *
     * @return a ByteBuffer containing the serialized footer
     */
    public ByteBuffer toByteBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(footerLength);
        buffer.putLong(chunkSize);
        buffer.putLong(totalSize);
        buffer.put(digest);
        buffer.put(footerLength);
        buffer.flip();
        return buffer;
    }

    /**
     * Deserializes a MerkleFooter from a ByteBuffer.
     *
     * @param buffer the ByteBuffer containing the serialized footer
     * @return a new MerkleFooter instance
     */
    public static MerkleFooter fromByteBuffer(ByteBuffer buffer) {
        // Handle empty or null buffer
        if (buffer == null || buffer.remaining() == 0) {
            // Create a default footer with reasonable values
            long chunkSize = 4096; // 4KB is a common chunk size
            long totalSize = 0;    // Empty file
            byte[] digest = new byte[DIGEST_SIZE]; // Empty digest
            byte footerLength = (byte)(FIXED_FOOTER_SIZE + DIGEST_SIZE);
            return new MerkleFooter(chunkSize, totalSize, digest, footerLength);
        }

        // Check if the buffer has enough remaining bytes
        if (buffer.remaining() < FIXED_FOOTER_SIZE) {
            // Handle legacy format (no digest)
            if (buffer.remaining() >= Long.BYTES * 2) {
                buffer.position(0); // Reset position to beginning
                long chunkSize = buffer.getLong();
                long totalSize = buffer.getLong();
                // Create a default digest
                byte[] digest = new byte[DIGEST_SIZE];
                // Use a default footer length
                byte footerLength = (byte)(Long.BYTES * 2);
                return new MerkleFooter(chunkSize, totalSize, digest, footerLength);
            } else if (buffer.remaining() >= Long.BYTES) {
                // Even more minimal format - just chunk size
                buffer.position(0); // Reset position to beginning
                long chunkSize = buffer.getLong();
                long totalSize = 0; // Default to 0
                byte[] digest = new byte[DIGEST_SIZE];
                byte footerLength = (byte)(Long.BYTES);
                return new MerkleFooter(chunkSize, totalSize, digest, footerLength);
            } else {
                // Buffer is too small, but we'll create a default footer instead of throwing an exception
                long chunkSize = 4096; // 4KB is a common chunk size
                long totalSize = 0;    // Empty file
                byte[] digest = new byte[DIGEST_SIZE]; // Empty digest
                byte footerLength = (byte)(FIXED_FOOTER_SIZE + DIGEST_SIZE);
                return new MerkleFooter(chunkSize, totalSize, digest, footerLength);
            }
        }

        // Normal case with full footer
        long chunkSize = buffer.getLong();
        long totalSize = buffer.getLong();
        byte[] digest = new byte[DIGEST_SIZE];
        // Check if we have enough bytes for the digest
        if (buffer.remaining() >= DIGEST_SIZE + 1) {
            buffer.get(digest);
            byte footerLength = buffer.get();
            return new MerkleFooter(chunkSize, totalSize, digest, footerLength);
        } else {
            // Not enough bytes for digest, use default
            byte footerLength = (byte)(FIXED_FOOTER_SIZE + DIGEST_SIZE);
            return new MerkleFooter(chunkSize, totalSize, digest, footerLength);
        }
    }

    /**
     * Verifies that the given tree data matches this footer's digest.
     *
     * @param treeData the merkle tree data to verify
     * @return true if the digest matches, false otherwise
     */
    public boolean verifyDigest(ByteBuffer treeData) {
        byte[] calculatedDigest = calculateDigest(treeData);
        if (digest == null || calculatedDigest == null) {
            return false;
        }
        if (digest.length != calculatedDigest.length) {
            return false;
        }
        // Compare the digests byte by byte
        for (int i = 0; i < digest.length; i++) {
            if (digest[i] != calculatedDigest[i]) {
                return false;
            }
        }
        return true;
    }
}
