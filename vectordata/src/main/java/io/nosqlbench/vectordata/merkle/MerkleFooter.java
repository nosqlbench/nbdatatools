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


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;

/**
 * A record type for the footer of a merkle tree file.
 * <p>
 * The footer contains metadata about the merkle tree, including:
 * @param chunkSize: the size of each chunk in bytes
 * @param totalSize: the total size of the data in bytes
 * @param footerLength: the length of the footer in bytes
 */
public record MerkleFooter(long chunkSize, long totalSize, byte footerLength) {

    /**
     The fixed size of the footer in bytes
     8 bytes for chunkSize + 8 bytes for totalSize + 1 byte for footerLength
     */
    public static final int FIXED_FOOTER_SIZE = Long.BYTES * 2 + Byte.BYTES;
    private static final Logger logger = LogManager.getLogger(MerkleFooter.class);

    /**
     Creates a new MerkleFooter with the given parameters and calculates the footer length.
     @param chunkSize
     the chunk size in bytes
     @param totalSize
     the total size of the data in bytes
     @return a new MerkleFooter instance
     */
    public static MerkleFooter create(long chunkSize, long totalSize) {
        // Calculate the footer length: fixed size
        byte footerLength = (byte) (FIXED_FOOTER_SIZE);
        return new MerkleFooter(chunkSize, totalSize, footerLength);
    }

    /**
     Serializes this footer to a ByteBuffer.
     @return a ByteBuffer containing the serialized footer
     */
    public ByteBuffer toByteBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(footerLength);
        buffer.putLong(chunkSize);
        buffer.putLong(totalSize);
        buffer.put(footerLength);
        buffer.flip();
        return buffer;
    }

    /**
     Deserializes a MerkleFooter from a ByteBuffer.
     @param buffer
     the ByteBuffer containing the serialized footer
     @return a new MerkleFooter instance
     */
    public static MerkleFooter fromByteBuffer(ByteBuffer buffer) {
        // Expect a full footer: chunkSize (8) + totalSize (8) + footerLength (1)
        int expected = FIXED_FOOTER_SIZE;
        if (buffer == null || buffer.remaining() < expected) {
            throw new IllegalArgumentException(
                "Invalid Merkle footer buffer size: " + (buffer == null ? 0 : buffer.remaining())
                + ", expected at least " + expected);
        }
        long chunkSize = buffer.getLong();
        long totalSize = buffer.getLong();
        byte footerLength = buffer.get();
        return new MerkleFooter(chunkSize, totalSize, footerLength);
    }

    /**
     * This method is a placeholder that always returns true.
     * The digest verification functionality has been removed.
     * @param treeData the merkle tree data (ignored)
     * @return always returns true
     */
    public boolean verifyDigest(ByteBuffer treeData) {
        return true;
    }

    /**
     Reads a MerkleFooter from a remote URL.
     @param url
     The URL to read from
     @return The MerkleFooter
     @throws IOException
     If there's an error reading the footer
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
        connection.setRequestProperty(
            "Range",
            "bytes=" + startPosition + "-" + (remoteFileSize - 1)
        );
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

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MerkleFooter that))
            return false;

      return chunkSize == that.chunkSize && totalSize == that.totalSize
               && footerLength == that.footerLength;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(chunkSize);
        result = 31 * result + Long.hashCode(totalSize);
        result = 31 * result + footerLength;
        return result;
    }
}
