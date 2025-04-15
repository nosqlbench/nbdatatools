package io.nosqlbench.vectordata.download.merkle;

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
