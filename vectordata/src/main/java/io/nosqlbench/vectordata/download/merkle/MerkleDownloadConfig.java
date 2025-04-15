package io.nosqlbench.vectordata.download.merkle;

import java.net.URL;
import java.nio.file.Path;
import java.time.Duration;
import java.util.function.Consumer;

/// Configuration record for Merkle tree-based file downloads.
///
/// This record contains all parameters needed to configure a download operation
/// that uses Merkle trees for efficient verification and partial downloads.
/// It supports both full downloads and partial downloads up to a target size.
///
public record MerkleDownloadConfig(
    URL sourceUrl,
    URL merkleUrl,
    Path targetPath,
    SpanMode spanMode,
    long targetSpanSize,  // only used when spanMode is TARGET_SPAN
    int retryCount,
    Duration timeout,
    Duration retryDelay,
    Consumer<ReconciliationProgress> progressCallback,
    Consumer<ReconciliationComplete> completionCallback,
    boolean useChunkedDownloader,
    int chunkSize,
    int maxConcurrentChunks
) {
    public MerkleDownloadConfig {
        if (sourceUrl == null) throw new IllegalArgumentException("Source URL is required");
        if (targetPath == null) throw new IllegalArgumentException("Target path is required");
        if (merkleUrl == null) throw new IllegalArgumentException("Merkle URL is required");
        if (spanMode == null) throw new IllegalArgumentException("Span mode is required");
        if (spanMode == SpanMode.TARGET_SPAN && targetSpanSize <= 0) {
            throw new IllegalArgumentException("Target span size must be positive when using TARGET_SPAN mode");
        }
        if (timeout == null) throw new IllegalArgumentException("Timeout is required");
        if (retryDelay == null) throw new IllegalArgumentException("Retry delay is required");
        if (chunkSize <= 0) throw new IllegalArgumentException("Chunk size must be positive");
        if (maxConcurrentChunks <= 0) throw new IllegalArgumentException("Max concurrent chunks must be positive");
    }
}