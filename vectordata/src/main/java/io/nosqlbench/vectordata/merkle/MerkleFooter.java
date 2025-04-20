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


import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
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

    /**
     * Reads a MerkleFooter from a remote URL.
     *
     * @param url The URL to read from
     * @return The MerkleFooter
     * @throws IOException If there's an error reading the footer
     */
    public static MerkleFooter fromRemoteUrl(URL url) throws IOException {

        // Step 1: Get the size of the remote file using a HEAD request
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("HEAD");
        connection.setConnectTimeout(5000); // 5 second timeout
        connection.setReadTimeout(5000);    // 5 second timeout
        connection.connect();

        // Check if the request was successful
        int responseCode = connection.getResponseCode();
        if (responseCode != HttpURLConnection.HTTP_OK) {
            throw new IOException("Failed to get file size: HTTP " + responseCode);
        }

        // Get the file size
        long remoteFileSize = connection.getContentLengthLong();
        if (remoteFileSize <= 0) {
            throw new IOException("Invalid file size: " + remoteFileSize);
        }

        // Step 2: Read the last 1KB of the file (or the entire file if it's smaller than 1KB)
        int readSize = (int) Math.min(1024, remoteFileSize);
        long startPosition = remoteFileSize - readSize;

        // Set up the connection for a ranged GET request
        connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Range", "bytes=" + startPosition + "-" + (remoteFileSize - 1));
        connection.setConnectTimeout(5000); // 5 second timeout
        connection.setReadTimeout(5000);    // 5 second timeout
        connection.connect();

        // Check if the server supports range requests
        responseCode = connection.getResponseCode();
        ByteBuffer buffer;

        if (responseCode == HttpURLConnection.HTTP_PARTIAL) {
            // Range request succeeded
            try (InputStream inputStream = connection.getInputStream()) {
                buffer = ByteBuffer.allocate(readSize);
                byte[] bytes = new byte[readSize];
                int bytesRead = inputStream.read(bytes);
                if (bytesRead > 0) {
                    buffer.put(bytes, 0, bytesRead);
                }
                buffer.flip();
            }
        } else {
            // Range request failed, read the entire file
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(5000); // 5 second timeout
            connection.setReadTimeout(5000);    // 5 second timeout
            connection.connect();

            responseCode = connection.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                throw new IOException("Failed to read file: HTTP " + responseCode);
            }

            try (InputStream inputStream = connection.getInputStream()) {
                // Read the entire file into memory
                byte[] bytes = inputStream.readAllBytes();
                // If the file is larger than 1KB, only keep the last 1KB
                if (bytes.length > 1024) {
                    buffer = ByteBuffer.allocate(1024);
                    buffer.put(bytes, bytes.length - 1024, 1024);
                } else {
                    buffer = ByteBuffer.allocate(bytes.length);
                    buffer.put(bytes);
                }
                buffer.flip();
            }
        }

        // Step 3: Read the footer length from the last byte
        if (buffer.remaining() == 0) {
            throw new IOException("Empty buffer read from URL");
        }

        // Get the last byte which contains the footer length
        byte footerLength = buffer.get(buffer.limit() - 1);

        // Step 4: Extract the footer data based on the footer length
        if (footerLength <= 0 || footerLength > buffer.remaining()) {
            // Invalid footer length, try to create a default footer
            return fromByteBuffer(buffer);
        }

        // Create a new buffer with just the footer data
        ByteBuffer footerBuffer = ByteBuffer.allocate(footerLength);
        int startPos = buffer.limit() - footerLength;
        buffer.position(startPos);
        footerBuffer.put(buffer);
        footerBuffer.flip();

        // Step 5: Create a new MerkleFooter object from the extracted data
        return fromByteBuffer(footerBuffer);
    }
}
