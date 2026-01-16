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

import io.nosqlbench.nbdatatools.api.transport.ChunkedTransportClient;
import io.nosqlbench.nbdatatools.api.transport.ChunkedTransportIO;
import io.nosqlbench.nbdatatools.api.transport.FetchResult;
import io.nosqlbench.vectordata.events.EventSink;
import io.nosqlbench.vectordata.events.NoOpEventSink;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implementation of ResourceTransportService using ChunkedTransportIO.
 * This service delegates all transport operations to ChunkedTransportClient
 * to avoid duplication of HTTP client and metadata logic.
 */
public class ChunkedResourceTransportService implements ResourceTransportService {
    
    private final long defaultChunkSize;
    private final int defaultParallelism;
    private final EventSink defaultEventSink;
    private final Executor executor;
    
    /**
     * Creates a ChunkedResourceTransportService with default settings.
     * 
     * Uses 1MB chunk size, 4-way parallelism, no-op event sink, and common fork-join pool.
     */
    public ChunkedResourceTransportService() {
        this(1024 * 1024, 4, NoOpEventSink.INSTANCE, ForkJoinPool.commonPool());
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
    }
    
    @Override
    public CompletableFuture<ResourceMetadata> getResourceMetadata(URL url) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // For file URLs, we need to handle them specially since ChunkedTransportIO
                // may not handle file:// URLs correctly
                String urlString = url.toString();
                if (url.getProtocol().equalsIgnoreCase("file")) {
                    // Use the path component directly for file URLs
                    urlString = url.getPath();
                }
                
                try (ChunkedTransportClient client = ChunkedTransportIO.create(urlString)) {
                    long size = client.getSize().join();
                    boolean supportsRanges = client.supportsRangeRequests();
                    
                    // Build metadata based on what ChunkedTransportClient provides
                    if (size >= 0) {
                        return ResourceMetadata.basic(size, supportsRanges);
                    } else {
                        return ResourceMetadata.notFound();
                    }
                }
            } catch (Exception e) {
                defaultEventSink.debug("Error getting resource metadata for {}: {}", url, e.getMessage());
                return ResourceMetadata.notFound();
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
        AtomicLong bytesDownloaded = new AtomicLong(0);
        
        CompletableFuture<DownloadResult> downloadFuture = CompletableFuture.supplyAsync(() -> {
            try {
                // Handle file URLs specially
                String urlString = url.toString();
                if (url.getProtocol().equalsIgnoreCase("file")) {
                    urlString = url.getPath();
                }
                
                try (ChunkedTransportClient client = ChunkedTransportIO.create(urlString)) {
                    // Check if file already exists and force parameter
                    if (Files.exists(targetPath) && !force) {
                        long existingSize = Files.size(targetPath);
                        eventSink.debug("File already exists: {} (size: {})", targetPath, existingSize);
                        return DownloadResult.skipped(targetPath, existingSize);
                    }
                    
                    // Create parent directories if needed
                    Path parentDir = targetPath.getParent();
                    try {
                        Files.createDirectories(parentDir);
                    } catch (java.nio.file.FileAlreadyExistsException e) {
                        // Handle race condition or broken symlink
                        Path problemPath = java.nio.file.Path.of(e.getFile());
                        if (Files.isSymbolicLink(problemPath)) {
                            try {
                                Path target = Files.readSymbolicLink(problemPath);
                                if (!target.isAbsolute()) {
                                    target = problemPath.getParent().resolve(target);
                                }
                                Files.createDirectories(target);
                            } catch (IOException targetEx) {
                                Files.delete(problemPath);
                            }
                            Files.createDirectories(parentDir);
                        } else if (!Files.isDirectory(parentDir)) {
                            throw e;
                        }
                    }
                    
                    // Get total size for progress tracking
                    long totalSize = client.getSize().join();
                    if (totalSize < 0) {
                        throw new IOException("Unable to determine resource size");
                    }
                    
                    eventSink.debug("Starting download of {} bytes from {} to {}", totalSize, url, targetPath);
                    
                    // Download in chunks for better progress tracking and parallelism
                    try (FileChannel fileChannel = FileChannel.open(targetPath, 
                            StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                        
                        // Calculate total chunks for progress tracking
                        int totalChunks = (int) ((totalSize + chunkSize - 1) / chunkSize);
                        
                        // Wrap the client with progress tracking if event sink is provided
                        ChunkedTransportClient trackingClient = (eventSink != null && !(eventSink instanceof NoOpEventSink)) 
                            ? new ProgressTrackingTransportClient(client, totalSize, totalChunks, eventSink)
                            : client;
                        
                        if (client.supportsRangeRequests() && totalSize > chunkSize) {
                            // Use parallel chunked download
                            downloadInParallel(trackingClient, fileChannel, totalSize, chunkSize, parallelism, 
                                             bytesDownloaded, eventSink);
                        } else {
                            // Use sequential download
                            downloadSequentially(trackingClient, fileChannel, totalSize, chunkSize, 
                                                bytesDownloaded, eventSink);
                        }
                    }
                    
                    eventSink.debug("Download completed: {} bytes written to {}", bytesDownloaded.get(), targetPath);
                    return DownloadResult.downloaded(targetPath, bytesDownloaded.get());
                }
            } catch (Exception e) {
                eventSink.debug("Download failed for {}: {}", url, e.getMessage());
                throw new RuntimeException("Download failed: " + e.getMessage(), e);
            }
        }, executor);
        
        return new DownloadProgress(targetPath, -1, bytesDownloaded, downloadFuture);
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
                
                // Get remote size using ChunkedTransportClient
                String urlString = remoteUrl.toString();
                if (remoteUrl.getProtocol().equalsIgnoreCase("file")) {
                    urlString = remoteUrl.getPath();
                }
                
                try (ChunkedTransportClient client = ChunkedTransportIO.create(urlString)) {
                    long remoteSize = client.getSize().join();
                    long localSize = Files.size(localPath);
                    
                    if (remoteSize < 0) {
                        defaultEventSink.debug("Unable to determine remote size for {}", remoteUrl);
                        return false;
                    }
                    
                    // Compare sizes as basic check
                    boolean match = localSize == remoteSize;
                    defaultEventSink.debug("Size comparison: local={}, remote={}, match={}", 
                                         localSize, remoteSize, match);
                    return match;
                }
                
            } catch (Exception e) {
                defaultEventSink.debug("Error comparing local and remote files: {}", e.getMessage());
                return false;
            }
        }, executor);
    }
    
    /**
     * Downloads a resource in parallel chunks.
     */
    private void downloadInParallel(ChunkedTransportClient client, FileChannel fileChannel, 
                                   long totalSize, long chunkSize, int parallelism,
                                   AtomicLong bytesDownloaded, EventSink eventSink) throws Exception {
        
        // Calculate chunk boundaries
        long numChunks = (totalSize + chunkSize - 1) / chunkSize;
        
        // Download chunks in parallel with controlled parallelism
        CompletableFuture<?>[] chunkFutures = new CompletableFuture[(int) Math.min(numChunks, parallelism)];
        
        for (int i = 0; i < chunkFutures.length; i++) {
            final int chunkIndex = i;
            chunkFutures[i] = CompletableFuture.runAsync(() -> {
                try {
                    long startOffset = chunkIndex * chunkSize;
                    long endOffset = Math.min(startOffset + chunkSize, totalSize);
                    int chunkLength = (int) (endOffset - startOffset);
                    
                    if (chunkLength <= 0) return;
                    
                    FetchResult<?> result = client.fetchRange(startOffset, chunkLength).join();
                    ByteBuffer chunkData = result.getData();
                    
                    synchronized (fileChannel) {
                        fileChannel.write(chunkData, startOffset);
                    }
                    
                    // Update bytes downloaded counter
                    long actualLength = result.getActualLength();
                    bytesDownloaded.addAndGet(actualLength);
                    
                    // Progress tracking is now handled by ProgressTrackingTransportClient if enabled
                    if (!(client instanceof ProgressTrackingTransportClient)) {
                        eventSink.debug("Downloaded chunk {}: {} bytes (total: {}/{})", 
                                       chunkIndex, actualLength, bytesDownloaded.get(), totalSize);
                    }
                    
                } catch (Exception e) {
                    throw new RuntimeException("Failed to download chunk " + chunkIndex, e);
                }
            }, executor);
        }
        
        // Wait for all chunks to complete
        CompletableFuture.allOf(chunkFutures).join();
        
        // Download remaining chunks if any
        if (numChunks > parallelism) {
            for (long i = parallelism; i < numChunks; i++) {
                long startOffset = i * chunkSize;
                long endOffset = Math.min(startOffset + chunkSize, totalSize);
                int chunkLength = (int) (endOffset - startOffset);
                
                if (chunkLength <= 0) break;
                
                FetchResult<?> result = client.fetchRange(startOffset, chunkLength).join();
                ByteBuffer chunkData = result.getData();
                fileChannel.write(chunkData, startOffset);
                
                // Update bytes downloaded counter
                long actualLength = result.getActualLength();
                bytesDownloaded.addAndGet(actualLength);
                
                // Progress tracking is now handled by ProgressTrackingTransportClient if enabled
                if (!(client instanceof ProgressTrackingTransportClient)) {
                    eventSink.debug("Downloaded remaining chunk {}: {} bytes (total: {}/{})", 
                                   i, actualLength, bytesDownloaded.get(), totalSize);
                }
            }
        }
    }
    
    /**
     * Downloads a resource sequentially.
     */
    private void downloadSequentially(ChunkedTransportClient client, FileChannel fileChannel, 
                                    long totalSize, long chunkSize, AtomicLong bytesDownloaded, 
                                    EventSink eventSink) throws Exception {
        
        long offset = 0;
        while (offset < totalSize) {
            long remainingSize = totalSize - offset;
            int currentChunkSize = (int) Math.min(chunkSize, remainingSize);
            
            FetchResult<?> result = client.fetchRange(offset, currentChunkSize).join();
            ByteBuffer chunkData = result.getData();
            fileChannel.write(chunkData, offset);
            
            offset += currentChunkSize;
            
            // Update bytes downloaded counter
            long actualLength = result.getActualLength();
            bytesDownloaded.addAndGet(actualLength);
            
            // Progress tracking is now handled by ProgressTrackingTransportClient if enabled
            if (!(client instanceof ProgressTrackingTransportClient)) {
                eventSink.debug("Downloaded sequential chunk: {} bytes (total: {}/{})", 
                               actualLength, bytesDownloaded.get(), totalSize);
            }
        }
    }
}