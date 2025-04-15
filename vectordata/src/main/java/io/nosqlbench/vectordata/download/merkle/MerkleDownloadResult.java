package io.nosqlbench.vectordata.download.merkle;

import java.nio.file.Path;

/// Represents the result of a Merkle tree-based download operation.
///
/// This record contains information about the outcome of a download operation,
/// including the downloaded file path, bytes downloaded, elapsed time, and success status.
///
public record MerkleDownloadResult(
    /// Path to the downloaded file
    Path downloadedFile,
    /// Number of bytes downloaded
    long bytesDownloaded,
    /// Time elapsed in milliseconds
    long timeElapsedMs,
    /// Whether the download completed successfully
    boolean successful,
    /// Error that occurred during the download, or null if successful
    Throwable error
) {}