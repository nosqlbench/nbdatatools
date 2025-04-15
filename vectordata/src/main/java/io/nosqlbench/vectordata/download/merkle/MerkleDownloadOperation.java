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



import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/// Handles the execution of Merkle tree-based file downloads.
///
/// This class implements the core download logic for files using Merkle trees
/// for verification. It supports both full file downloads and partial downloads
/// up to a specified size, with optional chunked downloading for large files.
///
public class MerkleDownloadOperation {
    private final MerkleDownloadConfig config;
    private final ExecutorService executor;

    public MerkleDownloadOperation(MerkleDownloadConfig config) {
        this.config = config;
        this.executor = Executors.newFixedThreadPool(config.maxConcurrentChunks());
    }

    /// Executes the download operation asynchronously.
    ///
    /// @return a CompletableFuture that will complete with the download result
    public CompletableFuture<MerkleDownloadResult> execute() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                MerkleTreeFile merkleTree = downloadMerkleTree();
                List<Long> boundaries = merkleTree.getLeafBoundaries();

                long targetSpan = switch (config.spanMode()) {
                    case FULL_SPAN -> boundaries.get(boundaries.size() - 1);
                    case TARGET_SPAN -> config.targetSpanSize();
                };

                long downloadBoundary = boundaries.stream()
                    .filter(b -> b >= targetSpan)
                    .findFirst()
                    .orElse(boundaries.get(boundaries.size() - 1));

                return config.useChunkedDownloader()
                    ? downloadWithChunks(downloadBoundary)
                    : downloadDirect(downloadBoundary);
            } catch (Exception e) {
                return new MerkleDownloadResult(
                    config.targetPath(),
                    0L,
                    0L,
                    false,
                    e
                );
            } finally {
                executor.shutdown();
                try {
                    executor.awaitTermination(config.timeout().toMillis(), TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
    }

    /// Downloads the Merkle tree file from the configured URL.
    ///
    /// @return the downloaded and parsed Merkle tree file
    /// @throws IOException if there is an error downloading or parsing the file
    private MerkleTreeFile downloadMerkleTree() throws IOException {
        HttpURLConnection conn = (HttpURLConnection) config.merkleUrl().openConnection();
        conn.setConnectTimeout((int) config.timeout().toMillis());
        conn.setReadTimeout((int) config.timeout().toMillis());

        Path tempMerklePath = Files.createTempFile("merkle", ".tmp");
        try (InputStream in = conn.getInputStream();
             FileChannel out = FileChannel.open(tempMerklePath,
                 StandardOpenOption.WRITE,
                 StandardOpenOption.CREATE)) {

            byte[] buffer = new byte[8192];
            int read;
            while ((read = in.read(buffer)) != -1) {
                out.write(ByteBuffer.wrap(buffer, 0, read));
            }
        }

        return MerkleTreeFile.load(tempMerklePath);
    }

    /// Downloads the file in chunks up to the specified boundary.
    ///
    /// @param boundary the maximum file offset to download
    /// @return the result of the download operation
    /// @throws IOException if there is an error during download
    private MerkleDownloadResult downloadWithChunks(long boundary) throws IOException {
        long startTime = System.currentTimeMillis();

        List<DownloadChunk> chunks = createChunks(boundary);

        AtomicLong bytesProcessed = new AtomicLong(0);
        int totalChunks = chunks.size();

        List<CompletableFuture<Void>> futures = chunks.stream()
            .map(chunk -> CompletableFuture.runAsync(() -> {
                downloadChunk(chunk);
                updateProgress(bytesProcessed.addAndGet(chunk.size), boundary,
                    chunks.indexOf(chunk) + 1, totalChunks);
            }, executor))
            .collect(Collectors.toList());

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        long timeElapsed = System.currentTimeMillis() - startTime;
        notifyComplete(true, boundary, timeElapsed, null);

        return new MerkleDownloadResult(
            config.targetPath(),
            boundary,
            timeElapsed,
            true,
            null
        );
    }

    /// Downloads the file directly (without chunking) up to the specified boundary.
    ///
    /// @param boundary the maximum file offset to download
    /// @return the result of the download operation
    /// @throws IOException if there is an error during download
    private MerkleDownloadResult downloadDirect(long boundary) throws IOException {
        long startTime = System.currentTimeMillis();

        HttpURLConnection conn = (HttpURLConnection) config.sourceUrl().openConnection();
        conn.setRequestProperty("Range", "bytes=0-" + (boundary - 1));

        try (InputStream in = conn.getInputStream();
             FileChannel out = FileChannel.open(config.targetPath(),
                 StandardOpenOption.WRITE,
                 StandardOpenOption.CREATE)) {

            long bytesProcessed = 0;
            byte[] buffer = new byte[8192];
            int read;

            while ((read = in.read(buffer)) != -1) {
                out.write(ByteBuffer.wrap(buffer, 0, read));
                bytesProcessed += read;
                updateProgress(bytesProcessed, boundary, 1, 1);
            }

            long timeElapsed = System.currentTimeMillis() - startTime;
            notifyComplete(true, bytesProcessed, timeElapsed, null);

            return new MerkleDownloadResult(
                config.targetPath(),
                bytesProcessed,
                timeElapsed,
                true,
                null
            );
        }
    }

    /// Updates the progress callback with the current download progress.
    ///
    /// @param bytesProcessed number of bytes processed so far
    /// @param totalBytes total number of bytes to process
    /// @param completedSections number of sections completed
    /// @param totalSections total number of sections
    private void updateProgress(long bytesProcessed, long totalBytes,
                              int completedSections, int totalSections) {
        if (config.progressCallback() != null) {
            double percent = (bytesProcessed * 100.0) / totalBytes;
            config.progressCallback().accept(new ReconciliationProgress(
                bytesProcessed,
                totalBytes,
                completedSections,
                totalSections,
                percent
            ));
        }
    }

    /// Notifies the completion callback about the download result.
    ///
    /// @param successful whether the download completed successfully
    /// @param downloadedSize number of bytes downloaded
    /// @param timeElapsed time elapsed in milliseconds
    /// @param error error that occurred, or null if successful
    private void notifyComplete(boolean successful, long downloadedSize, long timeElapsed, Throwable error) {
        if (config.completionCallback() != null) {
            config.completionCallback().accept(new ReconciliationComplete(
                successful ? downloadedSize : 0,
                timeElapsed,
                successful,
                error
            ));
        }
    }

    /// Creates a list of download chunks for the specified boundary.
    ///
    /// @param boundary the maximum file offset to download
    /// @return a list of download chunks
    private List<DownloadChunk> createChunks(long boundary) {
        List<DownloadChunk> chunks = new ArrayList<>();
        long remaining = boundary;
        long offset = 0;

        while (remaining > 0) {
            long size = Math.min(remaining, config.chunkSize());
            chunks.add(new DownloadChunk(offset, size));
            offset += size;
            remaining -= size;
        }

        return chunks;
    }

    /// Represents a chunk of a file to download.
    ///
    /// @param offset the starting offset in the file
    /// @param size the size of the chunk in bytes
    private record DownloadChunk(long offset, long size) {}

    /// Downloads a single chunk of the file.
    ///
    /// @param chunk the chunk to download
    private void downloadChunk(DownloadChunk chunk) {
        try {
            HttpURLConnection conn = (HttpURLConnection) config.sourceUrl().openConnection();
            conn.setConnectTimeout((int) config.timeout().toMillis());
            conn.setReadTimeout((int) config.timeout().toMillis());
            conn.setRequestProperty("Range", String.format("bytes=%d-%d", chunk.offset(), chunk.offset() + chunk.size() - 1));

            try (InputStream in = conn.getInputStream();
                 FileChannel out = FileChannel.open(config.targetPath(),
                     StandardOpenOption.WRITE,
                     StandardOpenOption.CREATE)) {

                out.position(chunk.offset());
                byte[] buffer = new byte[8192];
                int read;
                long totalRead = 0;

                while (totalRead < chunk.size() && (read = in.read(buffer, 0, (int) Math.min(buffer.length, chunk.size() - totalRead))) != -1) {
                    out.write(ByteBuffer.wrap(buffer, 0, read));
                    totalRead += read;
                }

                if (totalRead != chunk.size()) {
                    throw new IOException(String.format("Incomplete chunk download: expected %d bytes, got %d bytes",
                        chunk.size(), totalRead));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to download chunk at offset " + chunk.offset(), e);
        }
    }
}
