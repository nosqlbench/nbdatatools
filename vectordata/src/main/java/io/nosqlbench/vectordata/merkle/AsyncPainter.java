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

import io.nosqlbench.vectordata.downloader.DownloadProgress;

/**
 * Interface for asynchronous chunk painting operations in the Merkle tree system.
 * 
 * Implementations of this interface provide asynchronous downloading and verification
 * of data chunks based on Merkle tree validation. The paintAsync method returns a
 * DownloadProgress object that can be used to track the status of the download
 * operation or wait for its completion.
 */
public interface AsyncPainter {
    
    /**
     * Uses the underlying pane to determine what chunks need to be fetched, and then 
     * submits them to the pane for reconciliation. This call returns a DownloadProgress 
     * that can be used to check the status of an active download or otherwise 
     * synchronously wait for the result.
     * 
     * This method first checks if all chunks in the range are already valid.
     * If they are, it returns a completed DownloadProgress without scheduling any downloads.
     *
     * Implementations may automatically select download sizes that align with chunk boundaries,
     * respecting any minimum and maximum download size constraints. They may also ensure 
     * that downloads are only scheduled for chunks that don't already have pending downloads.
     *
     * Implementations may also implement auto-buffering behavior for read-ahead optimization.
     *
     * @param startIncl the start value inclusive
     * @param endExcl the end value exclusive
     * @return a DownloadProgress that can be used to track the download progress
     */
    DownloadProgress paintAsync(long startIncl, long endExcl);
}