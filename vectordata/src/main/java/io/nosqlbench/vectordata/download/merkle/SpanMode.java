package io.nosqlbench.vectordata.download.merkle;

/// Defines the download span mode for Merkle tree-based downloads.
///
/// This enum specifies whether to download the full file or a specific portion
/// up to a target size.
///
public enum SpanMode {
    /// Use the full span available in the merkle tree
    FULL_SPAN,

    /// Use a specific target span size
    TARGET_SPAN
}