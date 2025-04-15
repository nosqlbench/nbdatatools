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
