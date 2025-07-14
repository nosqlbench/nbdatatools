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

/// Exception thrown when there is an error downloading or verifying a chunk in the MerklePainter.
/// This exception is used to provide more specific information about the failure than a boolean return value.
public class ChunkDownloadException extends RuntimeException {
    /// The starting chunk index of the range that failed to download or verify.
    private final int startChunkIndex;

    /// The ending chunk index of the range that failed to download or verify.
    private final int endChunkIndex;

    /// The expected hash value for the chunk, if a hash verification failure occurred.
    /// This will be null if the failure was not related to hash verification.
    private final byte[] expectedHash;

    /// The actual hash value calculated for the downloaded chunk, if a hash verification failure occurred.
    /// This will be null if the failure was not related to hash verification.
    private final byte[] actualHash;

    /// Creates a new ChunkDownloadException with the specified message and chunk range.
    /// @param message The error message
    /// @param startChunkIndex The starting chunk index of the range that failed
    /// @param endChunkIndex The ending chunk index of the range that failed
    public ChunkDownloadException(String message, int startChunkIndex, int endChunkIndex) {
        super(message);
        this.startChunkIndex = startChunkIndex;
        this.endChunkIndex = endChunkIndex;
        this.expectedHash = null;
        this.actualHash = null;
    }

    /// Creates a new ChunkDownloadException with the specified message, cause, and chunk range.
    /// @param message The error message
    /// @param cause The cause of the exception
    /// @param startChunkIndex The starting chunk index of the range that failed
    /// @param endChunkIndex The ending chunk index of the range that failed
    public ChunkDownloadException(String message, Throwable cause, int startChunkIndex, int endChunkIndex) {
        super(message, cause);
        this.startChunkIndex = startChunkIndex;
        this.endChunkIndex = endChunkIndex;
        this.expectedHash = null;
        this.actualHash = null;
    }

    /// Creates a new ChunkDownloadException with the specified message, chunk range, and hash values.
    /// @param message The error message
    /// @param startChunkIndex The starting chunk index of the range that failed
    /// @param endChunkIndex The ending chunk index of the range that failed
    /// @param expectedHash The expected hash value
    /// @param actualHash The actual hash value
    public ChunkDownloadException(String message, int startChunkIndex, int endChunkIndex, byte[] expectedHash, byte[] actualHash) {
        super(formatMessage(message, expectedHash, actualHash));
        this.startChunkIndex = startChunkIndex;
        this.endChunkIndex = endChunkIndex;
        this.expectedHash = expectedHash;
        this.actualHash = actualHash;
    }

    /// Creates a new ChunkDownloadException with the specified message, cause, chunk range, and hash values.
    /// @param message The error message
    /// @param cause The cause of the exception
    /// @param startChunkIndex The starting chunk index of the range that failed
    /// @param endChunkIndex The ending chunk index of the range that failed
    /// @param expectedHash The expected hash value
    /// @param actualHash The actual hash value
    public ChunkDownloadException(String message, Throwable cause, int startChunkIndex, int endChunkIndex, byte[] expectedHash, byte[] actualHash) {
        super(formatMessage(message, expectedHash, actualHash), cause);
        this.startChunkIndex = startChunkIndex;
        this.endChunkIndex = endChunkIndex;
        this.expectedHash = expectedHash;
        this.actualHash = actualHash;
    }

    /// Gets the starting chunk index of the range that failed.
    /// @return The starting chunk index
    public int getStartChunkIndex() {
        return startChunkIndex;
    }

    /// Gets the ending chunk index of the range that failed.
    /// @return The ending chunk index
    public int getEndChunkIndex() {
        return endChunkIndex;
    }

    /// Gets the expected hash value.
    /// @return The expected hash value
    public byte[] getExpectedHash() {
        return expectedHash;
    }

    /// Gets the actual hash value.
    /// @return The actual hash value
    public byte[] getActualHash() {
        return actualHash;
    }

    /// Formats the error message to include hex representations of the hashes.
    /// @param message The base error message
    /// @param expectedHash The expected hash value
    /// @param actualHash The actual hash value
    /// @return The formatted error message
    private static String formatMessage(String message, byte[] expectedHash, byte[] actualHash) {
        if (expectedHash == null || actualHash == null) {
            return message;
        }
        return message + " (expected hash: " + bytesToHex(expectedHash) + ", actual hash: " + bytesToHex(actualHash) + ")";
    }

    /// Converts a byte array to a hexadecimal string representation.
    /// @param bytes The byte array to convert
    /// @return The hexadecimal string representation of the byte array
    private static String bytesToHex(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }
        StringBuilder hexString = new StringBuilder(2 * bytes.length);
        for (byte b : bytes) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }
}
