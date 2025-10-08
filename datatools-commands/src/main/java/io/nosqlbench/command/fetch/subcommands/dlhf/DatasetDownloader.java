package io.nosqlbench.command.fetch.subcommands.dlhf;

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


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.HttpHead;
import org.apache.hc.client5.http.fluent.Request;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.HttpHeaders;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/// Handles downloading of datasets from Hugging Face.
/// Manages authentication, metadata fetching, and actual file downloads.
public class DatasetDownloader implements AutoCloseable {
  private static final String HF_API_URL = "https://huggingface.co/api/datasets/";
  private static final String HF_VIEWER_API =
      "https://datasets-server.huggingface.co/parquet?dataset=";
  private static final String HF_DOWNLOAD_BASE = "https://huggingface.co/datasets/";
  private final String TOKEN;
  private final String dsName;
  private final Path target;
  private final ConsoleDisplay display;
  private final DownloadStats stats;
  private final ConcurrentHashMap<Path, FileProgress> fileProgresses = new ConcurrentHashMap<>();
  private volatile boolean running = true;

  /// parquet file metadata
  public static class ParquetFileData {
    /// the dataset name
    private final String dataset;
    /// the config name
    private final String config;
    /// the split name
    private final String split;
    /// the url to download from
    private final String url;
    /// the filename
    private final String filename;
    /// the file size
    private final long size;
    
    public ParquetFileData(String dataset, String config, String split, String url, String filename, long size) {
      this.dataset = dataset;
      this.config = config;
      this.split = split;
      this.url = url;
      this.filename = filename;
      this.size = size;
    }
    
    public String dataset() { return dataset; }
    public String config() { return config; }
    public String split() { return split; }
    public String url() { return url; }
    public String filename() { return filename; }
    public long size() { return size; }

    /// get the fully qualified path
    /// @return the fully qualified path
    public Path path() {
      return Path.of(config).resolve(filename);
    }
  }

  /// Creates a new dataset downloader
  /// @param token Hugging Face API token
  /// @param dsName Name of dataset to download
  /// @param target Target directory to save downloaded files
  /// @throws IOException If target directory cannot be created
  public DatasetDownloader(String token, String dsName, Path target) throws IOException {
    this.dsName = dsName;
    this.target = target;
    this.TOKEN = token;
    if (TOKEN == null || TOKEN.isEmpty()) {
      throw new RuntimeException("No token provided");
    }
    this.stats = new DownloadStats();
    this.display = new ConsoleDisplay(dsName, target, fileProgresses, stats);
  }

  private long getRemoteFileSize(String url) throws IOException {
    try (CloseableHttpClient client = HttpClients.createDefault()) {
      HttpHead head = new HttpHead(url);
      if (TOKEN != null && !TOKEN.isEmpty()) {
        head.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + TOKEN);
      }
      return client.execute(
          head, response -> {
            if (response.getCode() != 200) {
              String errorBody = "";
              try {
                if (response.getEntity() != null) {
                  // Read the entity content as a string
                  try (var content = response.getEntity().getContent()) {
                    java.util.Scanner s = new java.util.Scanner(content).useDelimiter("\\A");
                    errorBody = s.hasNext() ? s.next() : "";
                  }
                }
              } catch (Exception ignored) {
                // Ignore any errors reading the body
              }
              throw new IOException(
                  "HTTP " + response.getCode() + " error while getting file size for " + url + 
                  (errorBody.isEmpty() ? "" : ": " + errorBody));
            }

            if (response.getEntity() == null) {
              display.log("No entity in response for {}, will download full file", url);
              return -1L;
            }

            return response.getEntity().getContentLength();
          }
      );
    }
  }

  /// download the dataset
  public void download() {
    try {
      display.setStatus("Creating output directory");
      display.setAction("Checking " + target.toAbsolutePath());

      Files.createDirectories(target);
      display.log("Created output directory: %s", target);

      List<ParquetFileData> filesToDownload = fetchFileList(dsName);
      stats.setTotalFiles(filesToDownload.size());
      stats.addToTotalBytes(filesToDownload.stream().mapToLong(f -> f.size).sum());

      display.setStatus("Starting downloads");
      display.setAction("Initializing download threads");

      ExecutorService executor = Executors.newFixedThreadPool(6);
      List<CompletableFuture<Void>> tasks = new ArrayList<>();

      display.startProgressThread();

      for (ParquetFileData file : filesToDownload) {
        CompletableFuture<Void> task = CompletableFuture.runAsync(
            () -> {
              try {
                display.log("Starting download of file: %s", file.filename);
                downloadFile(file);
              } catch (Exception e) {
                display.log("Failed to download file: %s", file.filename);
              }
            }, executor
        );
        tasks.add(task);
      }

      CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0])).join();

      // Display final statistics
      display.setStatus("Download complete");
      display.setAction("");
      display.log("Final Statistics:");
      display.log("Files downloaded: %d, skipped: %d, failed: %d, total: %d",
          stats.getDownloadedFiles(),
          stats.getSkippedFiles(),
          stats.getFailedFiles(),
          stats.getTotalFiles());
      display.updateDisplay();

      // Now stop the display thread
      running = false;
      executor.shutdown();

    } catch (Exception e) {
      display.setStatus("Error: " + e.getMessage());
      // Still show statistics even if there was an error
      display.log("Final Statistics:");
      display.log("Files downloaded: %d, skipped: %d, failed: %d, total: %d",
          stats.getDownloadedFiles(),
          stats.getSkippedFiles(),
          stats.getFailedFiles(),
          stats.getTotalFiles());
      display.updateDisplay();
      throw new RuntimeException(e);
    }
  }

  /// Fetches dataset metadata from Hugging Face API
  /// @param datasetName Name of dataset to fetch metadata for
  /// @return List of parquet file metadata
  /// @throws Exception If API request fails
  private List<ParquetFileData> fetchFileList(String datasetName) throws Exception {
    display.setStatus("Fetching dataset parquet metadata");
    display.setAction("Connecting to Hugging Face Dataset Viewer API");

    String parquetMetadataUrl = HF_VIEWER_API + datasetName;
    display.log("Fetching parquet metadata from URL: %s", parquetMetadataUrl);

    Request request = Request.get(parquetMetadataUrl).addHeader("Accept", "application/json");

    if (TOKEN != null && !TOKEN.isEmpty()) {
//      head.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + TOKEN);

      request.addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + TOKEN);
    }

    display.setAction("Reading parquet metadata");

    String response = request.execute().returnContent().asString();
    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = mapper.readTree(response);

    display.setAction("Analyzing parquet files");

    List<ParquetFileData> parquetFiles = new ArrayList<>();

    JsonNode filesNode = root.path("parquet_files");
    for (JsonNode fileNode : filesNode) {
      String dataset = fileNode.path("dataset").asText();
      String config = fileNode.path("config").asText();
      String split = fileNode.path("split").asText();
      String url = fileNode.path("url").asText();
      String filename = fileNode.path("filename").asText();
      long size = fileNode.path("size").asLong();

      ParquetFileData fileData = new ParquetFileData(dataset, config, split, url, filename, size);
      parquetFiles.add(fileData);
    }

    summarizeParquetMetadata(parquetFiles);

    display.setStatus("Ready to begin download");
    display.setAction("Preparing download threads");

    if (parquetFiles.isEmpty()) {
      throw new RuntimeException("No files found for dataset: " + datasetName);
    }
    return parquetFiles;
  }

  private void summarizeParquetMetadata(List<ParquetFileData> parquetFiles) {
    display.clearPreDownloadInfo();
    display.addPreDownloadInfo("Dataset Structure Summary:");

    // File details
    for (ParquetFileData file : parquetFiles) {
      display.addPreDownloadInfo(java.lang.String.format(
          "File: %-30s Size: %d MB",
          file.filename,
          file.size / (1024 * 1024)
      ));
    }

    display.addPreDownloadInfo(java.lang.String.format("\nDataset Summary:"));
    display.addPreDownloadInfo(java.lang.String.format("  Total Files: %d", parquetFiles.size()));
  }

  private void downloadFile(ParquetFileData fileInfo) throws Exception {
    Path outFile = target.resolve(fileInfo.path());

    Path parentDir = outFile.getParent();
    if (parentDir != null && Files.notExists(parentDir)) {
      display.log("Creating directory: %s", parentDir);
      Files.createDirectories(parentDir);
    }

    // Check if file exists and has correct size
    if (Files.exists(outFile)) {
      long existingSize = Files.size(outFile);
      if (existingSize == fileInfo.size) {
        FileProgress progress = new FileProgress(fileInfo.path(), fileInfo.size);
        progress.currentBytes.set(fileInfo.size);  // Set to full size
        progress.completed = true;
        fileProgresses.put(fileInfo.path(), progress);
        stats.addToDownloadedBytes(fileInfo.size);
        stats.incrementSkippedFiles();  // Use new counter
        display.log("Skipped download of %s - file exists with correct size", fileInfo.path());
        return;
      } else {
        display.log("File %s exists but has wrong size (expected: %d, actual: %d) - redownloading",
            fileInfo.path(), fileInfo.size, existingSize);
        Files.delete(outFile);
      }
    }

    FileProgress progress = new FileProgress(fileInfo.path(), fileInfo.size);
    fileProgresses.put(fileInfo.path(), progress);

    try {
      URL url = new URL(fileInfo.url);
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      if (TOKEN != null && !TOKEN.isEmpty()) {
        conn.setRequestProperty(HttpHeaders.AUTHORIZATION, "Bearer " + TOKEN);
      }

      // Check the HTTP status code before attempting to read the stream
      int statusCode = conn.getResponseCode();
      if (statusCode < 200 || statusCode >= 300) {
        String errorBody = "";
        try {
          // When there's an error, the error stream contains the response body
          try (java.io.InputStream errorStream = conn.getErrorStream()) {
            if (errorStream != null) {
              java.util.Scanner s = new java.util.Scanner(errorStream).useDelimiter("\\A");
              errorBody = s.hasNext() ? s.next() : "";
            }
          }
        } catch (Exception ignored) {
          // Ignore any errors reading the body
        }
        throw new IOException("HTTP " + statusCode + " error while downloading " + fileInfo.path() + 
                             (errorBody.isEmpty() ? "" : ": " + errorBody));
      }

      try (InputStream in = conn.getInputStream();
           FileOutputStream out = new FileOutputStream(outFile.toFile()))
      {
        byte[] buffer = new byte[8192];
        int bytesRead;
        while ((bytesRead = in.read(buffer)) != -1) {
          out.write(buffer, 0, bytesRead);
          progress.currentBytes.addAndGet(bytesRead);
          stats.addToDownloadedBytes(bytesRead);
        }

        progress.completed = true;
        stats.incrementDownloadedFiles();  // Use new counter
        display.log("Finished download of %s", fileInfo.path());
      }

    } catch (Exception e) {
      progress.failed = true;
      progress.error = e.getMessage();
      stats.incrementFailedFiles();  // Add failed counter
      display.log("Failed to download %s: %s", fileInfo.path(), e.getMessage());
      try {
        Files.deleteIfExists(outFile);
        display.log("Cleaned up partial download file: %s", outFile);
      } catch (IOException deleteError) {
        display.log("Failed to clean up partial download file: %s", outFile);
      }
      throw e;
    }
  }

  @Override
  public void close() {
    display.close();
  }
}