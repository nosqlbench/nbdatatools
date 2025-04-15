package io.nosqlbench.vectordata.download.merkle;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;

/// Represents the footer structure of a Merkle tree file format.
/// This footer provides metadata about the Merkle tree's structure, hash algorithm,
/// and data organization.
///
/// The footer has a fixed size specified by the footerLength field and contains
/// essential information for parsing and validating the Merkle tree data that precedes it.
///
/// @param version Format version number for compatibility management
/// @param flags Reserved flags for format options (endianness, compression, etc.)
/// @param hashAlgorithmId Identifier for the hash algorithm used (e.g., 0x01=SHA-256, 0x02=SHA-512)
/// @param hashDigestLength Length in bytes of individual hash digests
/// @param totalDataSize Size in bytes of the full data that the Merkle tree indexes
/// @param numberOfLeaves Total count of leaf sections in the Merkle tree
/// @param leafBoundaryTableOffset File offset where the leaf boundary table begins
/// @param leafBoundaryTableLength Size in bytes of the leaf boundary table
/// @param fileDigest SHA-256 hash of the entire Merkle tree file for integrity checking
/// @param footerLength Total length of this footer in bytes
public record MerkleTreeFooter(
    byte version,             // 1 byte
    byte flags,               // 1 byte
    byte hashAlgorithmId,     // 1 byte
    byte hashDigestLength,    // 1 byte
    long totalDataSize,       // 8 bytes
    int numberOfLeaves,       // 4 bytes
    int leafBoundaryTableOffset, // 4 bytes
    int leafBoundaryTableLength, // 4 bytes
    byte[] fileDigest,        // 32 bytes
    short footerLength        // 2 bytes
) {
    /// Current version of the format
    public static final byte CURRENT_VERSION = 0x01;

    /// Supported hash algorithms
    public enum HashAlgorithm {
        /// SHA-256 hash algorithm
        SHA_256(32),
        /// SHA-512 hash algorithm
        SHA_512(64);

        private final int digestLength;

        HashAlgorithm(int digestLength) {
            this.digestLength = digestLength;
        }

        /// Get the byte value for this algorithm
        public byte getValue() {
            return (byte) ordinal();
        }

        /// Get the digest length in bytes for this algorithm
        public byte getDigestLength() {
            return (byte) digestLength;
        }

        /// Get the algorithm for a given byte value
        public static HashAlgorithm fromValue(byte value) {
            if (value < 0 || value >= values().length) {
                throw new IllegalArgumentException("Invalid hash algorithm ID: " + value);
            }
            return values()[value];
        }
    }

    /// Flag bits for format options
    public static class Flags {
        /// Flag indicating little-endian byte order
        public static final byte LITTLE_ENDIAN = 0x01;
        /// Flag indicating compressed data
        public static final byte COMPRESSED = 0x02;
    }

    /// Encodes this footer into a ByteBuffer
    ///
    /// @return a new ByteBuffer containing the encoded footer
    public ByteBuffer encode() {
        ByteBuffer buffer = ByteBuffer.allocate(size());
        return encodeTo(buffer);
    }

    /// Encodes this footer into the provided ByteBuffer
    ///
    /// @param buffer the buffer to write to
    /// @return the buffer, for method chaining
    public ByteBuffer encodeTo(ByteBuffer buffer) {
        buffer.put(version)
              .put(flags)
              .put(hashAlgorithmId)
              .put(hashDigestLength)
              .putLong(totalDataSize)
              .putInt(numberOfLeaves)
              .putInt(leafBoundaryTableOffset)
              .putInt(leafBoundaryTableLength)
              .put(fileDigest)
              .putShort(footerLength);
        return buffer;
    }

    /// Decodes a footer from a ByteBuffer
    ///
    /// @param buffer the buffer to read from
    /// @return the decoded MerkleTreeFooter
    /// @throws IllegalArgumentException if the buffer contains invalid data
    public static MerkleTreeFooter decode(ByteBuffer buffer) {
        byte version = buffer.get();
        byte flags = buffer.get();
        byte hashAlgorithmId = buffer.get();
        byte hashDigestLength = buffer.get();
        long totalDataSize = buffer.getLong();
        int numberOfLeaves = buffer.getInt();
        int leafBoundaryTableOffset = buffer.getInt();
        int leafBoundaryTableLength = buffer.getInt();
        
        HashAlgorithm algorithm = HashAlgorithm.fromValue(hashAlgorithmId);
        byte[] fileDigest = new byte[algorithm.getDigestLength()];
        buffer.get(fileDigest);
        
        short footerLength = buffer.getShort();

        MerkleTreeFooter footer = new MerkleTreeFooter(
            version,
            flags,
            hashAlgorithmId,
            hashDigestLength,
            totalDataSize,
            numberOfLeaves,
            leafBoundaryTableOffset,
            leafBoundaryTableLength,
            fileDigest,
            footerLength
        );

        footer.validate();
        return footer;
    }

    /// Validates that this footer has the correct version
    ///
    /// @throws IllegalStateException if the footer is invalid
    public void validate() {
        if (version != CURRENT_VERSION) {
            throw new IllegalStateException(
                "Unsupported version: 0x" + 
                Integer.toHexString(version)
            );
        }

        HashAlgorithm algorithm = HashAlgorithm.fromValue(hashAlgorithmId);
        if (hashDigestLength != algorithm.getDigestLength()) {
            throw new IllegalStateException(
                "Invalid digest length for " + algorithm + ": expected " + 
                algorithm.getDigestLength() + ", got " + hashDigestLength
            );
        }

        if (fileDigest.length != algorithm.getDigestLength()) {
            throw new IllegalStateException(
                "Invalid file digest length: expected " + 
                algorithm.getDigestLength() + ", got " + fileDigest.length
            );
        }
    }

    @Override
    public String toString() {
        StringBuilder hexDigest = new StringBuilder("0x");
        for (byte b : fileDigest) {
            hexDigest.append(String.format("%02x", b & 0xFF));
        }

        return new StringJoiner(", ", MerkleTreeFooter.class.getSimpleName() + "[", "]")
            .add("version=" + version)
            .add("flags=" + flags)
            .add("hashAlgorithmId=" + hashAlgorithmId)
            .add("hashDigestLength=" + hashDigestLength)
            .add("totalDataSize=" + totalDataSize)
            .add("numberOfLeaves=" + numberOfLeaves)
            .add("leafBoundaryTableOffset=" + leafBoundaryTableOffset)
            .add("leafBoundaryTableLength=" + leafBoundaryTableLength)
            .add("fileDigest=" + hexDigest)
            .add("footerLength=" + footerLength)
            .toString();
    }

    /// Returns the total size of the footer in bytes
    ///
    /// @return total footer size in bytes (58)
    public static int size() {
        return 58; // Sum of all field sizes
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MerkleTreeFooter that = (MerkleTreeFooter) o;
        return version == that.version &&
               flags == that.flags &&
               hashAlgorithmId == that.hashAlgorithmId &&
               hashDigestLength == that.hashDigestLength &&
               totalDataSize == that.totalDataSize &&
               numberOfLeaves == that.numberOfLeaves &&
               leafBoundaryTableOffset == that.leafBoundaryTableOffset &&
               leafBoundaryTableLength == that.leafBoundaryTableLength &&
               footerLength == that.footerLength &&
               Arrays.equals(fileDigest, that.fileDigest);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(
            version,
            flags,
            hashAlgorithmId,
            hashDigestLength,
            totalDataSize,
            numberOfLeaves,
            leafBoundaryTableOffset,
            leafBoundaryTableLength,
            footerLength
        );
        result = 31 * result + Arrays.hashCode(fileDigest);
        return result;
    }
}
