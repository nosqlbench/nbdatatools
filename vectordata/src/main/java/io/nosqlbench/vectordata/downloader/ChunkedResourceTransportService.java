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
import io.nosqlbench.vectordata.status.NoOpDownloadEventSink;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

/**
 * Implementation of ResourceTransportService using ChunkedDownloader.
 * This service provides consistent transport for all resource operations.
 */
public class ChunkedResourceTransportService implements ResourceTransportService {
    
    private final long defaultChunkSize;
    private final int defaultParallelism;
    private final EventSink defaultEventSink;
    private final Executor executor;
    private final OkHttpClient httpClient;
    
    /**
     * Creates a ChunkedResourceTransportService with default settings.
     * 
     * Uses 1MB chunk size, 4-way parallelism, no-op event sink, and common fork-join pool.
     */
    public ChunkedResourceTransportService() {
        this(1024 * 1024, 4, new NoOpDownloadEventSink(), ForkJoinPool.commonPool());
    }
    
    /**
     * Creates a ChunkedResourceTransportService with custom settings.
     * 
     * @param defaultChunkSize Default chunk size for downloads in bytes
     * @param defaultParallelism Default number of parallel download threads
     * @param defaultEventSink Default event sink for download progress notifications
     * @param executor Executor service for asynchronous operations
     */
    public ChunkedResourceTransportService(long defaultChunkSize, int defaultParallelism, 
                                         EventSink defaultEventSink, Executor executor) {
        this.defaultChunkSize = defaultChunkSize;
        this.defaultParallelism = defaultParallelism;
        this.defaultEventSink = defaultEventSink;
        this.executor = executor;
        this.httpClient = new OkHttpClient.Builder().build();
    }
    
    @Override
    public CompletableFuture<ResourceMetadata> getResourceMetadata(URL url) {
        return CompletableFuture.supplyAsync(() -> {
            if ("file".equalsIgnoreCase(url.getProtocol())) {
                return getFileMetadata(url);
            } else {
                return getHttpMetadata(url);
            }
        }, executor);
    }
    
    @Override
    public DownloadProgress downloadResource(URL url, Path targetPath, boolean force) {
        return downloadResource(url, targetPath, force, defaultChunkSize, defaultParallelism, defaultEventSink);
    }
    
    @Override
    public DownloadProgress downloadResource(URL url, Path targetPath, boolean force,
                                           long chunkSize, int parallelism, EventSink eventSink) {
        // Use ChunkedDownloader for the actual download
        ChunkedDownloader downloader = new ChunkedDownloader(
            url,
            targetPath.getFileName().toString(),
            chunkSize > 0 ? chunkSize : defaultChunkSize,
            parallelism > 0 ? parallelism : defaultParallelism,
            eventSink != null ? eventSink : defaultEventSink
        );
        
        return downloader.download(targetPath, force);
    }
    
    @Override
    public CompletableFuture<Boolean> resourceExists(URL url) {
        return getResourceMetadata(url).thenApply(ResourceMetadata::exists);
    }
    
    @Override
    public CompletableFuture<Boolean> localMatchesRemote(Path localPath, URL remoteUrl) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                if (!Files.exists(localPath)) {
                    return false;
                }
                
                // Get remote metadata
                ResourceMetadata remoteMetadata = getResourceMetadata(remoteUrl).join();
                if (!remoteMetadata.exists()) {
                    return false;
                }
                
                // Compare sizes first (quick check)
                long localSize = Files.size(localPath);
                if (remoteMetadata.hasValidSize() && localSize != remoteMetadata.size()) {
                    return false;
                }
                
                // If we have an ETag, use that for comparison
                if (remoteMetadata.etag() != null) {
                    // For now, we'll consider ETag comparison as a future enhancement
                    // and fall back to size comparison
                    return true;
                }
                
                // If we have last modified time, compare that
                if (remoteMetadata.lastModified() != null) {
                    // For now, we'll consider timestamp comparison as a future enhancement
                    // and fall back to size comparison
                    return true;
                }
                
                // As a basic check, if sizes match, consider them the same
                // This could be enhanced with content hashing in the future
                return true;
                
            } catch (Exception e) {
                defaultEventSink.debug("Error comparing local and remote files: {}", e.getMessage());
                return false;
            }
        }, executor);
    }
    
    private ResourceMetadata getFileMetadata(URL url) {
        try {
            Path filePath = Path.of(url.getPath());
            if (!Files.exists(filePath)) {
                return ResourceMetadata.notFound();
            }
            
            long size = Files.size(filePath);
            // File protocol supports ranges (we can seek in files)
            return ResourceMetadata.basic(size, true);
            
        } catch (Exception e) {
            defaultEventSink.debug("Error getting file metadata for {}: {}", url, e.getMessage());
            return ResourceMetadata.notFound();
        }
    }
    
    private ResourceMetadata getHttpMetadata(URL url) {
        Request request = new Request.Builder().url(url).head().build();
        
        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                return ResourceMetadata.notFound();
            }
            
            long contentLength = parseContentLength(response.header("Content-Length"));
            boolean supportsRanges = "bytes".equals(response.header("Accept-Ranges"));
            String contentType = response.header("Content-Type");
            String lastModified = response.header("Last-Modified");
            String etag = response.header("ETag");
            
            return ResourceMetadata.full(contentLength, supportsRanges, contentType, lastModified, etag);
            
        } catch (IOException e) {
            defaultEventSink.debug("Error getting HTTP metadata for {}: {}", url, e.getMessage());
            return ResourceMetadata.notFound();
        }
    }
    
    private long parseContentLength(String contentLengthHeader) {
        if (contentLengthHeader == null || contentLengthHeader.trim().isEmpty()) {
            return -1;
        }
        try {
            return Long.parseLong(contentLengthHeader.trim());
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}