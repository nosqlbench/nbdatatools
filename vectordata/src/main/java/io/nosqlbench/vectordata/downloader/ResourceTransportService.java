package io.nosqlbench.vectordata.downloader;

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

import io.nosqlbench.vectordata.status.EventSink;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

/**
 * Service interface for transporting resources (files) with support for
 * metadata querying and efficient downloads.
 */
public interface ResourceTransportService {
    
    /**
     * Gets metadata about a resource without downloading it.
     * 
     * @param url The URL of the resource
     * @return A CompletableFuture that completes with resource metadata
     */
    CompletableFuture<ResourceMetadata> getResourceMetadata(URL url);
    
    /**
     * Downloads a resource from the specified URL to a local path.
     * 
     * @param url The URL to download from
     * @param targetPath The local path to download to
     * @param force Whether to overwrite existing files
     * @return A DownloadProgress that tracks the download
     */
    DownloadProgress downloadResource(URL url, Path targetPath, boolean force);
    
    /**
     * Downloads a resource with custom settings.
     * 
     * @param url The URL to download from
     * @param targetPath The local path to download to
     * @param force Whether to overwrite existing files
     * @param chunkSize Custom chunk size for parallel downloads (0 for default)
     * @param parallelism Custom parallelism level (0 for default)
     * @param eventSink Event sink for progress tracking
     * @return A DownloadProgress that tracks the download
     */
    DownloadProgress downloadResource(URL url, Path targetPath, boolean force, 
                                    long chunkSize, int parallelism, EventSink eventSink);
    
    /**
     * Checks if a resource exists at the given URL.
     * 
     * @param url The URL to check
     * @return A CompletableFuture that completes with true if the resource exists
     */
    CompletableFuture<Boolean> resourceExists(URL url);
    
    /**
     * Compares a local file with a remote resource to see if they match.
     * This is useful for determining if a download is necessary.
     * 
     * @param localPath The local file path
     * @param remoteUrl The remote resource URL
     * @return A CompletableFuture that completes with true if they match
     */
    CompletableFuture<Boolean> localMatchesRemote(Path localPath, URL remoteUrl);
}