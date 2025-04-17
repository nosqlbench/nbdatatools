package io.nosqlbench.vectordata.download.merkle;

/// Represents a mismatched chunk in the Merkle tree comparison
/// @param chunkIndex The index of the mismatched chunk
/// @param start Starting byte offset of the chunk
/// @param length Length of the chunk in bytes
public record MerkleMismatch(int chunkIndex, long start, long length) {}