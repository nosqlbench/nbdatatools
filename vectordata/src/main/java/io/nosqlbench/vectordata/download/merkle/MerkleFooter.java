package io.nosqlbench.vectordata.download.merkle;

/// a record type for the footer of a merkle tree file
/// @param chunkSize the chunk size in bytes
/// @param totalSize the total size of the data in bytes
public record MerkleFooter(long chunkSize, long totalSize, byte[] digest, byte footerLength) {
}
