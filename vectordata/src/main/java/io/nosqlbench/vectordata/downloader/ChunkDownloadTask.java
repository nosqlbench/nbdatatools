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
import okhttp3.*;
import okio.BufferedSource;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.file.Path;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/// A task to download a specific chunk of a file and write it to the correct position in the target file
public class ChunkDownloadTask implements Runnable {
    private static final int BUFFER_SIZE = 8192 * 2;
    private final OkHttpClient client;
    private final URL url;
    private final Path targetFile;
    private final long startByte;
    private final long endByte;
    private final long fileOffset; // Position in the output file
    private final AtomicLong totalBytesDownloaded;
    private final AtomicBoolean downloadFailed;
    private final EventSink eventSink;

    /// create a new chunk download task
    /// @param client the http client to use for downloading
    /// @param url the url to download from
    /// @param targetFile the file to write the downloaded chunk to
    /// @param startByte the start byte of the chunk to download
    /// @param endByte the end byte of the chunk to download
    /// @param fileOffset the position in the output file to write the chunk to
    /// @param totalBytesDownloaded the total number of bytes downloaded so far
    /// @param downloadFailed a flag indicating whether the download has failed
    /// @param eventSink the event sink to use for logging
    public ChunkDownloadTask(OkHttpClient client, URL url, Path targetFile, long startByte, long endByte,
                     long fileOffset, AtomicLong totalBytesDownloaded, AtomicBoolean downloadFailed, EventSink eventSink) {
        this.client = client;
        this.url = url;
        this.targetFile = targetFile;
        this.startByte = startByte;
        this.endByte = endByte;
        this.fileOffset = fileOffset;
        this.totalBytesDownloaded = totalBytesDownloaded;
        this.downloadFailed = downloadFailed;
        this.eventSink = eventSink;
    }

    @Override
    public void run() {
        // Short-circuit if another task has already failed
        if (downloadFailed.get()) {
            eventSink.debug("Skipping chunk {}-{} as download already failed.", startByte, endByte);
            return;
        }

        String rangeHeader = "bytes=" + startByte + "-" + endByte;
        Request request = new Request.Builder()
            .url(url)
            .header("Range", rangeHeader)
            .build();

        eventSink.debug("Requesting chunk: {}", rangeHeader);

        try (Response response = client.newCall(request).execute()) {
            // Check for successful partial content response
            if (response.code() != 206) { // 206 Partial Content is expected
                throw new IOException("Unexpected HTTP status " + response.code() +
                    " for range request " + rangeHeader + ". Body: " + getErrorBody(response));
            }

            ResponseBody body = response.body();
            if (body == null) {
                throw new IOException("No response body for range request " + rangeHeader);
            }

            // Write the received chunk to the correct position in the file
            try (RandomAccessFile raf = new RandomAccessFile(targetFile.toFile(), "rw")) {
                writeChunkToFile(raf, body, fileOffset, totalBytesDownloaded);
            }

        } catch (Exception e) {
            // Log the error and set the shared failure flag
            eventSink.error("Failed to download chunk {}-{}: {}", startByte, endByte, e.getMessage(), e);
            downloadFailed.set(true); // Signal failure to other tasks and the main future
            // Re-throw as a RuntimeException to make CompletableFuture fail
            throw new CompletionException("Chunk download failed (" + startByte + "-" + endByte + ")", e);
        }
    }

    private String getErrorBody(Response response) {
        try (ResponseBody body = response.body()) {
            return body != null ? body.string() : "";
        } catch (Exception ignored) {
            return "";
        }
    }

    private void writeChunkToFile(RandomAccessFile raf, ResponseBody body, long startOffset, AtomicLong totalBytesDownloaded) throws IOException {
        raf.seek(startOffset);
        eventSink.trace("Writing chunk starting at offset {}", startOffset);
        byte[] buffer = new byte[BUFFER_SIZE];
        long chunkBytesWritten = 0;
        try (BufferedSource source = body.source()) {
            while (!source.exhausted()) {
                int bytesRead = source.read(buffer);
                if (bytesRead == -1) break;
                raf.write(buffer, 0, bytesRead);
                chunkBytesWritten += bytesRead;
            }
        }
        totalBytesDownloaded.addAndGet(chunkBytesWritten);
        eventSink.trace("Finished writing chunk at offset {}. Bytes written: {}", startOffset, chunkBytesWritten);
    }
}
