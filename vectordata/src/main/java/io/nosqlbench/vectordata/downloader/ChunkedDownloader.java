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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/// A high-performance file downloader that supports parallel chunk-based downloading.
///
/// This class provides efficient file downloading with the following features:
/// - Parallel downloading of file chunks for faster downloads
/// - Resume capability for interrupted downloads
/// - Fallback to single-stream downloads when chunking is not supported
/// - Local file handling for file:// URLs
/// - Automatic target path resolution
public class ChunkedDownloader {
  /// Buffer size for reading/writing data (16KB)
  private static final int BUFFER_SIZE = 8192 * 2;
  /// Default chunk size for parallel downloads (10MB)
  private static final long DEFAULT_CHUNK_SIZE = 10 * 1024 * 1024;
  /// Default number of parallel download threads (max of 8 or half available processors)
  private static final int DEFAULT_PARALLELISM =
      Math.max(8, Runtime.getRuntime().availableProcessors() / 2);

  /// The URL to download from
  private final URL url;
  /// The name to use for the downloaded file if not specified in the target path
  private final String name;
  /// The size of each chunk for parallel downloads
  private final long chunkSize;
  /// The number of parallel download threads to use
  private final int parallelism;
  /// HTTP client for making requests
  private final OkHttpClient client;
  /// Event sink for logging and progress reporting
  private final EventSink eventSink;
  /// Optional starting position for range downloads
  private final long rangeStart;
  /// Optional length for range downloads (negative means download to end)
  private final long rangeLength;

  /// Creates a new chunked downloader with the specified parameters.
  ///
  /// @param url The URL to download from
  /// @param name The name to use for the downloaded file if not specified in the target path
  /// @param chunkSize The size of each chunk for parallel downloads (use 0 for default)
  /// @param parallelism The number of parallel download threads to use (use 0 for default)
  /// @param eventSink Event sink for logging and progress reporting
  public ChunkedDownloader(
      URL url,
      String name,
      long chunkSize,
      int parallelism,
      EventSink eventSink
  )
  {
    this(url, name, chunkSize, parallelism, eventSink, 0, -1);
  }

  /// Creates a new chunked downloader with the specified parameters for downloading a specific range.
  ///
  /// @param url The URL to download from
  /// @param name The name to use for the downloaded file if not specified in the target path
  /// @param chunkSize The size of each chunk for parallel downloads (use 0 for default)
  /// @param parallelism The number of parallel download threads to use (use 0 for default)
  /// @param eventSink Event sink for logging and progress reporting
  /// @param rangeStart The starting position for the range download
  /// @param rangeLength The length of the range to download (negative means download to end)
  public ChunkedDownloader(
      URL url,
      String name,
      long chunkSize,
      int parallelism,
      EventSink eventSink,
      long rangeStart,
      long rangeLength
  )
  {
    this.url = url;
    this.name = name;
    this.chunkSize = chunkSize > 0 ? chunkSize : DEFAULT_CHUNK_SIZE;
    this.parallelism = parallelism > 0 ? parallelism : DEFAULT_PARALLELISM;
    this.eventSink = eventSink;
    this.rangeStart = rangeStart;
    this.rangeLength = rangeLength;
    this.client = new OkHttpClient.Builder().connectTimeout(60, TimeUnit.SECONDS)
        .readTimeout(5, TimeUnit.MINUTES).writeTimeout(60, TimeUnit.SECONDS).followRedirects(true)
        .build();
  }

  /// Downloads a file from the configured URL to the specified target path.
  ///
  /// @param target The target path (can be a directory or file path)
  /// @param force Whether to force download even if the file already exists
  /// @return A DownloadProgress object that tracks the download progress and result
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

  /// Downloads a file in parallel chunks.
  ///
  /// This method divides the file into chunks and downloads them in parallel,
  /// which can significantly improve download speed for large files.
  ///
  /// @param targetFile The target file path
  /// @param force Whether to force download even if the file already exists
  /// @param metadata Metadata about the file being downloaded
  /// @param eventSink Event sink for logging and progress reporting
  /// @return A DownloadProgress object that tracks the download progress and result
  private DownloadProgress downloadParallel(
      Path targetFile,
      boolean force,
      FileMetadata metadata,
      EventSink eventSink
  )
  {
    CompletableFuture<DownloadResult> overallFuture = new CompletableFuture<>();
    AtomicLong totalBytesDownloaded = new AtomicLong(0);
    AtomicBoolean downloadFailed = new AtomicBoolean(false);

    ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    try {
      if (Files.exists(targetFile) && force) {
        Files.delete(targetFile);
      }

      // Calculate the actual range to download
      long effectiveRangeStart = rangeStart;
      long effectiveRangeLength = rangeLength;

      // If rangeLength is negative, download to the end of the file
      if (effectiveRangeLength < 0) {
        effectiveRangeLength = metadata.totalSize() - effectiveRangeStart;
      }

      // Ensure we don't try to download beyond the end of the file
      if (effectiveRangeStart + effectiveRangeLength > metadata.totalSize()) {
        effectiveRangeLength = metadata.totalSize() - effectiveRangeStart;
      }

      // Calculate the end position (inclusive)
      long effectiveRangeEnd = effectiveRangeStart + effectiveRangeLength - 1;

      // Log the effective range
      eventSink.debug("Effective download range: bytes={}-{} (length={})",
                    effectiveRangeStart, effectiveRangeEnd, effectiveRangeLength);

      try (RandomAccessFile raf = new RandomAccessFile(targetFile.toFile(), "rw")) {
        // Set the file length to match the range we're downloading
        raf.setLength(effectiveRangeLength);
      } catch (IOException e) {
        executor.shutdown();
        return createFailedProgress(targetFile, e);
      }

      List<CompletableFuture<Void>> chunkFutures = new ArrayList<>();

      // Start from the effective range startInclusive and go to the effective range end
      for (long offset = 0; offset < effectiveRangeLength; offset += chunkSize) {
        if (downloadFailed.get())
          break;

        // Calculate the absolute startInclusive and end positions in the file
        long absoluteStartByte = effectiveRangeStart + offset;
        long absoluteEndByte = Math.min(absoluteStartByte + chunkSize - 1, effectiveRangeEnd);

        // Calculate the relative position in the output file
        long relativeStartByte = offset;
        long relativeEndByte = relativeStartByte + (absoluteEndByte - absoluteStartByte);

        eventSink.debug("Requesting chunk: bytes={}-{}", absoluteStartByte, absoluteEndByte);
        ChunkDownloadTask task = new ChunkDownloadTask(
            client,
            url,
            targetFile,
            absoluteStartByte,  // Absolute position in the source file
            absoluteEndByte,    // Absolute position in the source file
            relativeStartByte,  // Relative position in the output file
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

  /// Downloads a file using a single HTTP stream.
  ///
  /// This method is used as a fallback when the server doesn't support range requests
  /// or when the file size is unknown.
  ///
  /// @param targetFile The target file path
  /// @param force Whether to force download even if the file already exists
  /// @param metadata Metadata about the file being downloaded
  /// @return A DownloadProgress object that tracks the download progress and result
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

  /// Executes a single-stream download operation.
  ///
  /// @param client The HTTP client to use
  /// @param request The HTTP request to execute
  /// @param targetFile The target file path
  /// @param expectedSize The expected size of the file (may be -1 if unknown)
  /// @param currentBytes Atomic counter for tracking downloaded bytes
  /// @param future Future to complete when the download finishes
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


  /// Retrieves metadata about the file to be downloaded using a HEAD request.
  ///
  /// @param client The HTTP client to use
  /// @return Metadata about the file, including size and range support
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

  /// Retrieves metadata about the file using a GET request with Range: bytes=0-0.
  ///
  /// This is a fallback method used when HEAD requests are not supported by the server.
  ///
  /// @param client The HTTP client to use
  /// @return Metadata about the file, including size and range support
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

  /// Parses the Content-Length header value to a long.
  ///
  /// @param headerValue The header value to parse
  /// @return The parsed content length, or -1 if parsing failed
  private long parseContentLength(String headerValue) {
    if (headerValue == null)
      return -1;
    try {
      return Long.parseLong(headerValue);
    } catch (NumberFormatException e) {
      return -1;
    }
  }


  /// Extracts the error message from an HTTP response body.
  ///
  /// @param response The HTTP response
  /// @return The response body as a string, or an empty string if extraction fails
  private String getErrorBody(Response response) {
    try (ResponseBody body = response.body()) { // Ensure body is closed
      return body != null ? body.string() : "";
    } catch (Exception ignored) { // Catch broader exceptions during body reading/closing
      return "";
    }
  }

  /// Writes a chunk of data to a specific position in a file.
  ///
  /// @param raf The random access file to write to
  /// @param body The response body containing the chunk data
  /// @param startOffset The starting offset in the file
  /// @param totalBytesDownloaded Atomic counter for tracking downloaded bytes
  /// @throws IOException If an I/O error occurs
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

  /// Handles errors that occur during download.
  ///
  /// This method logs the error, attempts to delete the partial file,
  /// and completes the future exceptionally.
  ///
  /// @param targetFile The target file path
  /// @param e The exception that occurred
  /// @param future The future to complete exceptionally
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

  /// Creates a DownloadProgress object for a failed download.
  ///
  /// @param targetFile The target file path
  /// @param e The exception that caused the failure
  /// @return A DownloadProgress object with a failed future
  private DownloadProgress createFailedProgress(Path targetFile, Exception e) {
    CompletableFuture<DownloadResult> future = new CompletableFuture<>();
    future.completeExceptionally(e); // Complete exceptionally immediately
    // future.complete(DownloadResult.failed(targetFile, e)); // Alternative
    return new DownloadProgress(targetFile, -1, new AtomicLong(0), future);
  }


  /// Determines the actual file path to download to.
  ///
  /// This method handles cases where the target is a directory or doesn't have a file extension.
  ///
  /// @param target The target path specified by the user
  /// @return The resolved file path to download to
  /// @throws RuntimeException If the target path is ambiguous
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

  /// Checks if a path has a file extension.
  ///
  /// @param path The path to check
  /// @return true if the path has a file extension, false otherwise
  private boolean hasFileExtension(Path path) {
    String fileName = path.getFileName().toString();
    int dotIndex = fileName.lastIndexOf('.');
    // Check if '.' exists, is not the first character, and is not the last character
    return dotIndex > 0 && dotIndex < fileName.length() - 1;
  }

  /// Handles local file:// URLs by creating a symbolic link or copying the file.
  ///
  /// @param target The target path specified by the user
  /// @param sourcePath The source file path from the URL
  /// @param force Whether to force overwrite if the target already exists
  /// @return A DownloadProgress object tracking the operation
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

  /// Creates a symbolic link or copies a local file to the target path.
  ///
  /// This method first attempts to create a symbolic link for efficiency,
  /// and falls back to copying the file if linking fails.
  ///
  /// @param targetFile The target file path
  /// @param sourcePath The source file path
  /// @param fileSize The size of the source file
  /// @param future The future to complete when the operation finishes
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

  /// Metadata about a file to be downloaded.
  ///
  /// @param totalSize The total size of the file in bytes, or -1 if unknown
  /// @param supportsRanges Whether the server supports range requests for parallel downloading
  private record FileMetadata(long totalSize, boolean supportsRanges) {
  }
}
