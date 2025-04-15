package io.nosqlbench.vectordata.download.chunker;

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


import io.nosqlbench.vectordata.download.DownloadProgress;
import io.nosqlbench.vectordata.download.DownloadResult;
import io.nosqlbench.vectordata.download.StdoutDownloadEventSink;
import okhttp3.*;
import okio.BufferedSource;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ChunkedDownloader {
  private static final int BUFFER_SIZE = 8192 * 2; // Increased buffer size
  private static final long DEFAULT_CHUNK_SIZE = 10 * 1024 * 1024; // 10 MB default chunk size
  private static final int DEFAULT_PARALLELISM =
      Math.max(8, Runtime.getRuntime().availableProcessors() / 2);

  private final URL url;
  private final String name;
  private final long chunkSize;
  private final int parallelism;
  private final OkHttpClient client;
  private final DownloadEventSink eventSink;

  public ChunkedDownloader(
      URL url,
      String name,
      long chunkSize,
      int parallelism,
      DownloadEventSink eventSink
  )
  {
    this.url = url;
    this.name = name;
    this.chunkSize = chunkSize > 0 ? chunkSize : DEFAULT_CHUNK_SIZE;
    this.parallelism = parallelism > 0 ? parallelism : DEFAULT_PARALLELISM;
    this.eventSink = eventSink;
    this.client = new OkHttpClient.Builder().connectTimeout(60, TimeUnit.SECONDS)
        .readTimeout(5, TimeUnit.MINUTES).writeTimeout(60, TimeUnit.SECONDS).followRedirects(true)
        .build();
  }

  public DownloadProgress download(Path target, boolean force) {
    if ("file".equalsIgnoreCase(url.getProtocol())) {
      return handleLocalFile(target, Path.of(url.getPath()), force);
    }

    Path targetFile;
    try {
      targetFile = determineTargetFile(target);
      Files.createDirectories(targetFile.getParent());
    } catch (IOException | RuntimeException e) {
      return createFailedProgress(target, e);
    }

    FileMetadata metadata = getFileMetadata(client);
    if (metadata.totalSize() < 0 || !metadata.supportsRanges()) {
      return downloadSingleStream(targetFile, force, metadata);
    }

    if (metadata.totalSize() == 0) {
      try {
        if (Files.exists(targetFile) && force) {
          Files.delete(targetFile);
        }
        Files.createFile(targetFile);
        CompletableFuture<DownloadResult> future =
            CompletableFuture.completedFuture(DownloadResult.downloaded(targetFile, 0));
        return new DownloadProgress(targetFile, 0, new AtomicLong(0), future);
      } catch (IOException e) {
        return createFailedProgress(targetFile, e);
      }
    }

    try {
      if (Files.exists(targetFile) && !force) {
        long existingFileSize = Files.size(targetFile);
        if (existingFileSize == metadata.totalSize()) {
          CompletableFuture<DownloadResult> future =
              CompletableFuture.completedFuture(DownloadResult.skipped(
                  targetFile,
                  existingFileSize
              ));
          return new DownloadProgress(
              targetFile,
              existingFileSize,
              new AtomicLong(existingFileSize),
              future
          );
        }
      }
    } catch (IOException ignored) {
    }

    return downloadParallel(targetFile, force, metadata, eventSink);
  }

  private DownloadProgress downloadParallel(
      Path targetFile,
      boolean force,
      FileMetadata metadata,
      DownloadEventSink eventSink
  )
  {
    CompletableFuture<DownloadResult> overallFuture = new CompletableFuture<>();
    AtomicLong totalBytesDownloaded = new AtomicLong(0);
    AtomicBoolean downloadFailed = new AtomicBoolean(false);

    ExecutorService executor = Executors.newFixedThreadPool(parallelism);

    try {
      if (Files.exists(targetFile) && force) {
        Files.delete(targetFile);
      }

      try (RandomAccessFile raf = new RandomAccessFile(targetFile.toFile(), "rw")) {
        raf.setLength(metadata.totalSize());
      } catch (IOException e) {
        executor.shutdown();
        return createFailedProgress(targetFile, e);
      }

      List<CompletableFuture<Void>> chunkFutures = new ArrayList<>();
      for (long startByte = 0; startByte < metadata.totalSize(); startByte += chunkSize) {
        if (downloadFailed.get())
          break;

        long endByte = Math.min(startByte + chunkSize - 1, metadata.totalSize() - 1);
        ChunkDownloadTask task = new ChunkDownloadTask(
            client,
            url,
            targetFile,
            startByte,
            endByte,
            totalBytesDownloaded,
            downloadFailed,
            eventSink
        );
        chunkFutures.add(CompletableFuture.runAsync(task, executor));
      }

      CompletableFuture<Void> allChunksFuture =
          CompletableFuture.allOf(chunkFutures.toArray(new CompletableFuture[0]));

      allChunksFuture.whenComplete((result, throwable) -> {
        if (throwable != null || downloadFailed.get()) {
          Exception failureReason =
              (throwable instanceof CompletionException && throwable.getCause() != null) ?
                  (Exception) throwable.getCause() : (
                  throwable instanceof Exception ? (Exception) throwable :
                      new RuntimeException("Unknown parallel download failure", throwable));

          handleDownloadError(targetFile, failureReason, overallFuture);
        } else {
          if (totalBytesDownloaded.get() != metadata.totalSize()) {
            overallFuture.complete(DownloadResult.downloaded(
                targetFile,
                totalBytesDownloaded.get()
            ));
          } else {
            overallFuture.complete(DownloadResult.downloaded(
                targetFile,
                totalBytesDownloaded.get()
            ));
          }
        }
        executor.shutdown();
      });

    } catch (Exception e) {
      handleDownloadError(targetFile, e, overallFuture);
      executor.shutdownNow();
    }

    return new DownloadProgress(
        targetFile,
        metadata.totalSize(),
        totalBytesDownloaded,
        overallFuture
    );
  }

  private DownloadProgress downloadSingleStream(
      Path targetFile,
      boolean force,
      FileMetadata metadata
  )
  {
    CompletableFuture<DownloadResult> future = new CompletableFuture<>();
    AtomicLong currentBytes = new AtomicLong(0);
    long expectedSize = metadata.totalSize(); // May be -1

    CompletableFuture.runAsync(() -> {
      try {
        long existingFileSize = 0;
        boolean resume = false;

        if (Files.exists(targetFile)) {
          if (force) {
            Files.delete(targetFile);
          } else {
            existingFileSize = Files.size(targetFile);
            if (expectedSize > 0 && existingFileSize == expectedSize) {
              future.complete(DownloadResult.skipped(targetFile, existingFileSize));
              return;
            } else if (expectedSize > 0) {
              Files.delete(targetFile); // Delete inconsistent file
              existingFileSize = 0;
            } else {
              Files.delete(targetFile);
              existingFileSize = 0;
            }
          }
        }

        Request request = new Request.Builder().url(url).get().build(); // Simple GET

        executeSingleStreamDownload(
            client,
            request,
            targetFile,
            expectedSize,
            currentBytes,
            future
        );

      } catch (Exception e) {
        handleDownloadError(targetFile, e, future);
      }
    });

    return new DownloadProgress(targetFile, expectedSize, currentBytes, future);
  }

  private void executeSingleStreamDownload(
      OkHttpClient client,
      Request request,
      Path targetFile,
      long expectedSize,
      AtomicLong currentBytes,
      CompletableFuture<DownloadResult> future
  )
  {
    try (Response response = client.newCall(request).execute()) {
      if (!response.isSuccessful()) {
        throw new IOException("HTTP " + response.code() + " error downloading " + url + ": "
                              + getErrorBody(response));
      }

      ResponseBody body = response.body();
      if (body == null) {
        throw new IOException("No response body for " + url);
      }

      Files.copy(body.byteStream(), targetFile, StandardCopyOption.REPLACE_EXISTING);
      long finalSize = Files.size(targetFile);
      currentBytes.set(finalSize);

      if (expectedSize > 0 && finalSize != expectedSize) {
        eventSink.warn(
            "Single-stream download size ({}) does not match expected size ({}).",
            finalSize,
            expectedSize
        );
      }
      eventSink.info(
          "Single-stream download completed for {}. Total bytes: {}",
          targetFile,
          finalSize
      );
      future.complete(DownloadResult.downloaded(targetFile, finalSize));

    } catch (Exception e) {
      handleDownloadError(targetFile, e, future);
    }
  }


  private FileMetadata getFileMetadata(OkHttpClient client) {
    Request request = new Request.Builder().url(url).head().build();
    eventSink.debug("Requesting HEAD metadata from {}", url);

    try (Response response = client.newCall(request).execute()) {
      if (!response.isSuccessful()) {
        eventSink.warn("HEAD request failed ({}). Trying GET for metadata.", response.code());
        return getFileMetadataWithGet(client);
      }

      long contentLength = parseContentLength(response.header("Content-Length"));
      boolean supportsRanges = "bytes".equals(response.header("Accept-Ranges"));
      eventSink.debug(
          "Metadata via HEAD: Size={}, SupportsRanges={}",
          contentLength,
          supportsRanges
      );
      return new FileMetadata(contentLength, supportsRanges);

    } catch (IOException e) {
      eventSink.warn("HEAD request failed for metadata. Trying GET.", e);
      return getFileMetadataWithGet(client);
    }
  }

  private FileMetadata getFileMetadataWithGet(OkHttpClient client) {
    Request request = new Request.Builder().url(url).get().addHeader("Range", "bytes=0-0").build();
    eventSink.debug("Requesting GET metadata (Range 0-0) from {}", url);

    try (Response response = client.newCall(request).execute()) {
      boolean supportsRanges = response.code() == 206 && response.header("Content-Range") != null;
      long contentLength = -1;

      if (supportsRanges) {
        String contentRange = response.header("Content-Range");
        if (contentRange != null && contentRange.startsWith("bytes ")) {
          int slashPos = contentRange.lastIndexOf('/');
          if (slashPos != -1) {
            try {
              contentLength = Long.parseLong(contentRange.substring(slashPos + 1));
            } catch (NumberFormatException ignored) {
              eventSink.warn("Could not parse total size from Content-Range: {}", contentRange);
            }
          }
        }
      }

      if (contentLength < 0) {
        contentLength = parseContentLength(response.header("Content-Length"));
      }

      if (!response.isSuccessful() && response.code() != 206) {
        eventSink.error("GET request for metadata failed with code {}", response.code());
        return new FileMetadata(-1, false);
      }

      eventSink.debug(
          "Metadata via GET: Size={}, SupportsRanges={}",
          contentLength,
          supportsRanges
      );
      return new FileMetadata(contentLength, supportsRanges);

    } catch (IOException e) {
      eventSink.error("GET request for metadata failed.", e);
      return new FileMetadata(-1, false);
    }
  }

  private long parseContentLength(String headerValue) {
    if (headerValue == null)
      return -1;
    try {
      return Long.parseLong(headerValue);
    } catch (NumberFormatException e) {
      return -1;
    }
  }


  private String getErrorBody(Response response) {
    try (ResponseBody body = response.body()) { // Ensure body is closed
      return body != null ? body.string() : "";
    } catch (Exception ignored) { // Catch broader exceptions during body reading/closing
      return "";
    }
  }

  // Renamed from writeToFile to avoid confusion with parallel writing
  private void writeChunkToFile(
      RandomAccessFile raf,
      ResponseBody body,
      long startOffset,
      AtomicLong totalBytesDownloaded
  ) throws IOException
  {
    raf.seek(startOffset);
    eventSink.trace("Writing chunk starting at offset {}", startOffset);

    byte[] buffer = new byte[BUFFER_SIZE];
    long chunkBytesWritten = 0;
    try (BufferedSource source = body.source()) {
      while (!source.exhausted()) {
        int bytesRead = source.read(buffer);
        if (bytesRead == -1)
          break;

        raf.write(buffer, 0, bytesRead);
        chunkBytesWritten += bytesRead;
      }
    }
    totalBytesDownloaded.addAndGet(chunkBytesWritten);
    eventSink.trace(
        "Finished writing chunk at offset {}. Bytes written: {}",
        startOffset,
        chunkBytesWritten
    );
  }

  private void handleDownloadError(
      Path targetFile,
      Exception e,
      CompletableFuture<DownloadResult> future
  )
  {
    eventSink.error("Download failed for {}: {}", targetFile, e.getMessage(), e);
    try {
      if (targetFile != null && Files.exists(targetFile)) {
        eventSink.debug("Attempting to delete partial file: {}", targetFile);
        Files.delete(targetFile);
      }
    } catch (IOException cleanupEx) {
      eventSink.warn("Failed to cleanup partial download file: {}", targetFile, cleanupEx);
    }
    if (!future.isDone()) {
      future.completeExceptionally(
          e instanceof CompletionException ? e : new CompletionException(e));
    }
  }

  private DownloadProgress createFailedProgress(Path targetFile, Exception e) {
    CompletableFuture<DownloadResult> future = new CompletableFuture<>();
    future.completeExceptionally(e); // Complete exceptionally immediately
    // future.complete(DownloadResult.failed(targetFile, e)); // Alternative
    return new DownloadProgress(targetFile, -1, new AtomicLong(0), future);
  }


  private Path determineTargetFile(Path target) {
    // If target has an extension OR if it doesn't exist yet, assume it's the file path
    if (hasFileExtension(target) || !Files.exists(target)) {
      eventSink.trace("Target '{}' treated as file path.", target);
      return target;
    }
    // If target exists and is a directory, resolve the filename inside it
    if (Files.isDirectory(target)) {
      String fileName = name; // Use the name provided in constructor
      // Ensure filename has an extension if the original URL didn't imply one clearly
      if (!hasFileExtension(Path.of(fileName))) {
        String urlPath = url.getPath();
        int lastSlash = urlPath.lastIndexOf('/');
        String urlFileName = (lastSlash >= 0) ? urlPath.substring(lastSlash + 1) : urlPath;
        if (hasFileExtension(Path.of(urlFileName))) {
          fileName = urlFileName; // Prefer filename from URL if it has extension
        } else if (!fileName.endsWith(".bin")) { // Avoid adding .bin if already there
          eventSink.trace("Appending default extension '.bin' to derived filename '{}'", fileName);
          fileName += ".bin";
        }
      }
      Path resolved = target.resolve(fileName);
      eventSink.trace("Target '{}' is a directory. Resolved file path to '{}'", target, resolved);
      return resolved;
    }
    // If target exists but is not a directory and has no extension, it's ambiguous
    throw new RuntimeException("Target path " + target
                               + " exists but is not a directory and lacks a file extension. Cannot determine target file name.");
  }

  private boolean hasFileExtension(Path path) {
    String fileName = path.getFileName().toString();
    int dotIndex = fileName.lastIndexOf('.');
    // Check if '.' exists, is not the first character, and is not the last character
    return dotIndex > 0 && dotIndex < fileName.length() - 1;
  }

  // --- Local File Handling (Unchanged from original logic, but integrated) ---

  private DownloadProgress handleLocalFile(Path target, Path sourcePath, boolean force) {
    eventSink.info("Handling local file copy/link from {} to {}", sourcePath, target);
    Path targetFile = null; // Initialize to null
    CompletableFuture<DownloadResult> future = new CompletableFuture<>();
    AtomicLong currentBytes = new AtomicLong(0);
    long fileSize = -1;

    try {
      targetFile = determineTargetFile(target); // Determine final target path
      Files.createDirectories(targetFile.getParent());

      if (!Files.exists(sourcePath)) {
        throw new IOException("Source file does not exist: " + sourcePath);
      }

      fileSize = Files.size(sourcePath);
      currentBytes.set(fileSize); // Assume full size for local ops

      if (Files.exists(targetFile)) {
        if (!force) {
          try {
            // Check if it's already the same file (e.g., hard link or previous symlink)
            if (Files.isSameFile(sourcePath, targetFile)) {
              eventSink.info(
                  "Target {} is already the same as source {}. Skipping.",
                  targetFile,
                  sourcePath
              );
              future.complete(DownloadResult.skipped(targetFile, fileSize));
              return new DownloadProgress(targetFile, fileSize, currentBytes, future);
            } else {
              eventSink.warn(
                  "Target file {} exists but is different from source {}. Overwriting (force=false, but implicit overwrite for local).",
                  targetFile,
                  sourcePath
              );
              // Fall through to delete and recreate/link
            }
          } catch (IOException e) {
            eventSink.warn(
                "Could not compare source/target files ({}). Proceeding with overwrite.",
                e.getMessage()
            );
            // Fall through if comparison fails
          }
        }
        eventSink.debug("Deleting existing target file {} before local copy/link.", targetFile);
        Files.delete(targetFile);
      }

      handleLocalFileCopy(targetFile, sourcePath, fileSize, future);

    } catch (Exception e) {
      eventSink.error(
          "Failed during local file handling for target {}: {}",
          target,
          e.getMessage(),
          e
      );
      // Ensure future is completed exceptionally, use determined targetFile if available
      handleDownloadError(targetFile != null ? targetFile : target, e, future);
    }

    // Return progress using determined targetFile and known/estimated size
    return new DownloadProgress(
        targetFile != null ? targetFile : target,
        fileSize,
        currentBytes,
        future
    );
  }

  private void handleLocalFileCopy(
      Path targetFile,
      Path sourcePath,
      long fileSize,
      CompletableFuture<DownloadResult> future
  )
  {
    // Prefer symbolic link for efficiency
    try {
      Path absoluteSource = sourcePath.toAbsolutePath();
      Files.createSymbolicLink(targetFile, absoluteSource);
      eventSink.info("Created symbolic link: {} -> {}", targetFile, absoluteSource);
      future.complete(DownloadResult.downloaded(targetFile, fileSize)); // Treat linking as success
    } catch (IOException |
             UnsupportedOperationException linkError) { // Catch UnsupportedOperationException for systems without symlink support
      eventSink.warn(
          "Failed to create symbolic link ({}). Falling back to file copy.",
          linkError.getMessage()
      );
      try {
        Files.copy(
            sourcePath,
            targetFile,
            StandardCopyOption.REPLACE_EXISTING
        ); // Ensure replace if somehow link failed but file exists
        eventSink.info("Copied local file {} -> {}", sourcePath, targetFile);
        future.complete(DownloadResult.downloaded(targetFile, fileSize));
      } catch (IOException copyError) {
        eventSink.error("Fallback file copy failed {} -> {}", sourcePath, targetFile, copyError);
        // Complete exceptionally using the copy error
        handleDownloadError(targetFile, copyError, future);
      }
    }
  }

  // --- Inner Classes ---

  private record FileMetadata(long totalSize, boolean supportsRanges) {
  }
}
