package io.nosqlbench.vectordata.download;

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

// TODO: default download location should use one of the established from nb, jvector, hugging
//  face, but should warn if there are multiple


import okhttp3.*;
import okio.BufferedSource;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public record DatasetEntry(
    String name,
    URL url,
    Map<String, String> attributes,
    Map<String, Map<String, Object>> datasets,
    Map<String, String> tokens,
    Map<String, String> tags
) {
    private static final int BUFFER_SIZE = 8192;

    private FileMetadata getFileMetadata(OkHttpClient client) {
        Request request = new Request.Builder()
            .url(url)
            .head()
            .build();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                return new FileMetadata(-1, false);
            }

            boolean supportsRanges = "bytes".equals(response.header("Accept-Ranges"));
            long contentLength = response.body() != null ? response.body().contentLength() : -1;

            return new FileMetadata(contentLength, supportsRanges);
        } catch (IOException e) {
            return new FileMetadata(-1, false);
        }
    }

    public DownloadProgress download(Path target) {
        return download(target, false);
    }

    public DownloadProgress download(Path target, boolean force) {
        // Determine target file path
        Path targetFile;
        if (hasFileExtension(target)) {
            targetFile = target;
        } else {
            try {
                Files.createDirectories(target);
            } catch (IOException e) {
                CompletableFuture<DownloadResult> future = new CompletableFuture<>();
                future.completeExceptionally(e);
                return new DownloadProgress(target, 0, new AtomicLong(0), future);
            }
            String fileName = name;
            if (!hasFileExtension(Path.of(fileName))) {
                fileName += ".bin";
            }
            targetFile = target.resolve(fileName);
        }

        // Create parent directories if needed
        try {
            Files.createDirectories(targetFile.getParent());
        } catch (IOException e) {
            CompletableFuture<DownloadResult> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return new DownloadProgress(targetFile, 0, new AtomicLong(0), future);
        }

        // Initialize progress tracking
        CompletableFuture<DownloadResult> future = new CompletableFuture<>();
        AtomicLong currentBytes = new AtomicLong(0);

        // Check if file exists and get metadata first
        OkHttpClient metadataClient = new OkHttpClient();
        FileMetadata metadata = getFileMetadata(metadataClient);

        try {
            if (Files.exists(targetFile) && !force) {
                long existingFileSize = Files.size(targetFile);
                if (metadata.totalSize() > 0 && existingFileSize == metadata.totalSize()) {
                    // File exists and has correct size - skip download
                    currentBytes.set(existingFileSize);
                    future.complete(DownloadResult.skipped(targetFile, existingFileSize));
                    return new DownloadProgress(targetFile, existingFileSize, currentBytes, future);
                }
            }
        } catch (IOException e) {
            // If we can't check the existing file, proceed with download
        }

        CompletableFuture.runAsync(() -> {
            OkHttpClient client = new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .build();

            try {
                long existingFileSize = 0;

                if (Files.exists(targetFile)) {
                    if (force) {
                        Files.delete(targetFile);
                    } else if (metadata.supportsRanges()) {
                        existingFileSize = Files.size(targetFile);
                        currentBytes.set(existingFileSize);
                    }
                }

                Request.Builder requestBuilder = new Request.Builder()
                    .url(url);

                // Correctly handle range requests to avoid exceeding the file size
                if (existingFileSize > 0 && metadata.supportsRanges()) {
                    if (existingFileSize < metadata.totalSize()) {
                        requestBuilder.addHeader("Range", "bytes=" + existingFileSize + "-" + (metadata.totalSize() -1));
                    } else {
                        // Local file is larger than or equal to remote file, skip download
                        future.complete(DownloadResult.skipped(targetFile, existingFileSize));
                        return;
                    }
                }

                try (Response response = client.newCall(requestBuilder.build()).execute()) {
                    if (!response.isSuccessful()) {
                        String errorBody = "";
                        try {
                            ResponseBody body = response.body();
                            if (body != null) {
                                errorBody = body.string();
                            }
                        } catch (IOException ignored) {
                            // If we can't read the error body, continue with empty error message
                        }
                        throw new IOException("HTTP " + response.code() +
                                              " error downloading " + url +
                                              (errorBody.isEmpty() ? "" : ": " + errorBody));
                    }

                    ResponseBody body = response.body();
                    if (body == null) {
                        throw new IOException("No response body");
                    }

                    // Get the total size from either metadata or response headers
                    long totalSize = metadata.totalSize();
                    if (totalSize <= 0) {
                        totalSize = body.contentLength();
                    }

                    try (RandomAccessFile file = new RandomAccessFile(targetFile.toFile(), "rw")) {
                        if (existingFileSize > 0 && metadata.supportsRanges()) {
                            file.seek(existingFileSize);
                        }

                        byte[] buffer = new byte[BUFFER_SIZE];
                        try (BufferedSource source = body.source()) {
                            while (!source.exhausted()) {
                                int bytesRead = source.read(buffer);
                                if (bytesRead == -1) break;

                                file.write(buffer, 0, bytesRead);
                                currentBytes.addAndGet(bytesRead);
                            }
                        }
                    }

                    future.complete(DownloadResult.downloaded(targetFile, currentBytes.get()));
                }

            } catch (Exception e) {
                try {
                    if (Files.exists(targetFile)) {
                        Files.delete(targetFile);
                    }
                } catch (IOException ignored) {
                    // Ignore cleanup errors
                }
                future.complete(DownloadResult.failed(targetFile, e));
            }
        });

        return new DownloadProgress(targetFile, metadata.totalSize(), currentBytes, future);
    }

    private boolean hasFileExtension(Path path) {
        String fileName = path.getFileName().toString();
        return fileName.contains(".") && !fileName.endsWith(".");
    }
}