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
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpHead;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpEntity;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
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
  private static final int MAX_ERROR_PAYLOAD_CHARS = 800;
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
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
    
    /// Creates a new ParquetFileData instance.
    /// @param dataset the dataset name
    /// @param config the config name
    /// @param split the split name
    /// @param url the download URL
    /// @param filename the filename
    /// @param size the file size in bytes
    public ParquetFileData(String dataset, String config, String split, String url, String filename, long size) {
      this.dataset = dataset;
      this.config = config;
      this.split = split;
      this.url = url;
      this.filename = filename;
      this.size = size;
    }
    
    /// Returns the dataset name.
    /// @return the dataset name
    public String dataset() { return dataset; }
    /// Returns the config name.
    /// @return the config name
    public String config() { return config; }
    /// Returns the split name.
    /// @return the split name
    public String split() { return split; }
    /// Returns the download URL.
    /// @return the download URL
    public String url() { return url; }
    /// Returns the filename.
    /// @return the filename
    public String filename() { return filename; }
    /// Returns the file size in bytes.
    /// @return the file size in bytes
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
              throw new IOException(formatHttpErrorMessage(
                  response.getCode(),
                  response.getReasonPhrase(),
                  "getting file size for",
                  url,
                  readHttpEntity(response.getEntity())
              ));
            }

            if (response.getEntity() == null) {
              display.log("No entity in response for %s, will download full file", url);
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
                display.log("Failed to download file: %s (%s)", file.filename, summarizeThrowable(e));
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

    HttpGet request = new HttpGet(parquetMetadataUrl);
    request.setHeader(HttpHeaders.ACCEPT, "application/json");

    if (TOKEN != null && !TOKEN.isEmpty()) {
      request.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + TOKEN);
    }

    display.setAction("Reading parquet metadata");

    String response;
    try (CloseableHttpClient client = HttpClients.createDefault()) {
      response = client.execute(request, httpResponse -> {
        String responseBody = readHttpEntity(httpResponse.getEntity());
        int statusCode = httpResponse.getCode();
        if (statusCode < 200 || statusCode >= 300) {
          throw new IOException(formatHttpErrorMessage(
              statusCode,
              httpResponse.getReasonPhrase(),
              "fetching parquet metadata from",
              parquetMetadataUrl,
              responseBody
          ));
        }
        return responseBody;
      });
    }
    JsonNode root = JSON_MAPPER.readTree(response);

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
        String errorBody = readHttpConnectionErrorBody(conn);
        throw new IOException(formatHttpErrorMessage(
            statusCode,
            conn.getResponseMessage(),
            "downloading",
            fileInfo.path() + " from " + fileInfo.url(),
            errorBody
        ));
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
      progress.error = summarizeThrowable(e);
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

  static String formatHttpErrorMessage(
      int statusCode,
      String reasonPhrase,
      String operation,
      String resource,
      String payload
  ) {
    StringBuilder message = new StringBuilder();
    message.append("HTTP ").append(statusCode);
    if (reasonPhrase != null && !reasonPhrase.isBlank()) {
      message.append(" ").append(reasonPhrase.trim());
    }
    message.append(" while ").append(operation).append(" ").append(resource);

    String payloadSummary = summarizeErrorPayload(payload);
    if (!payloadSummary.isBlank()) {
      message.append(". Response payload: ").append(payloadSummary);
    }
    return message.toString();
  }

  static String summarizeErrorPayload(String payload) {
    if (payload == null) {
      return "";
    }
    String trimmed = payload.trim();
    if (trimmed.isEmpty()) {
      return "";
    }

    String summarized = summarizeJsonPayload(trimmed);
    if (summarized.isBlank()) {
      summarized = collapseWhitespace(trimmed);
    }
    if (summarized.length() > MAX_ERROR_PAYLOAD_CHARS) {
      summarized = summarized.substring(0, MAX_ERROR_PAYLOAD_CHARS) + "... [truncated]";
    }
    return summarized;
  }

  private static String summarizeJsonPayload(String payload) {
    try {
      JsonNode root = JSON_MAPPER.readTree(payload);
      Set<String> messages = new LinkedHashSet<>();
      collectJsonValue(messages, root.path("error"));
      collectJsonValue(messages, root.path("message"));
      collectJsonValue(messages, root.path("detail"));
      collectJsonValue(messages, root.path("cause"));
      collectJsonValue(messages, root.path("errors"));

      if (messages.isEmpty() && !root.isMissingNode() && !root.isNull()) {
        collectJsonValue(messages, root);
      }
      return collapseWhitespace(String.join(" | ", messages));
    } catch (Exception ignored) {
      return "";
    }
  }

  private static void collectJsonValue(Set<String> messages, JsonNode node) {
    if (node == null || node.isMissingNode() || node.isNull()) {
      return;
    }
    if (node.isTextual() || node.isNumber() || node.isBoolean()) {
      String value = node.asText().trim();
      if (!value.isEmpty()) {
        messages.add(value);
      }
      return;
    }
    if (node.isArray()) {
      for (JsonNode child : node) {
        collectJsonValue(messages, child);
      }
      return;
    }
    if (node.isObject()) {
      if (node.has("message")) {
        collectJsonValue(messages, node.get("message"));
      }
      if (node.has("detail")) {
        collectJsonValue(messages, node.get("detail"));
      }
      if (node.has("error")) {
        collectJsonValue(messages, node.get("error"));
      }
      if (messages.isEmpty()) {
        messages.add(node.toString());
      }
      return;
    }
    String asText = node.asText().trim();
    if (!asText.isEmpty()) {
      messages.add(asText);
    }
  }

  private static String collapseWhitespace(String value) {
    return value.replaceAll("\\s+", " ").trim();
  }

  private static String summarizeThrowable(Throwable throwable) {
    Throwable root = throwable;
    while (root.getCause() != null && root.getCause() != root) {
      root = root.getCause();
    }
    String message = root.getMessage();
    return message == null || message.isBlank() ? root.getClass().getSimpleName() : message;
  }

  private static String readHttpConnectionErrorBody(HttpURLConnection conn) {
    String body = readInputStream(conn.getErrorStream());
    if (body.isBlank()) {
      body = readHttpConnectionBody(conn);
    }
    return body;
  }

  private static String readHttpConnectionBody(HttpURLConnection conn) {
    try {
      return readInputStream(conn.getInputStream());
    } catch (IOException ignored) {
      return "";
    }
  }

  private static String readHttpEntity(HttpEntity entity) {
    if (entity == null) {
      return "";
    }
    try (InputStream input = entity.getContent()) {
      return readInputStream(input);
    } catch (Exception ignored) {
      return "";
    }
  }

  private static String readInputStream(InputStream stream) {
    if (stream == null) {
      return "";
    }
    try (InputStream input = stream) {
      return new String(input.readAllBytes(), StandardCharsets.UTF_8);
    } catch (Exception ignored) {
      return "";
    }
  }

  @Override
  public void close() {
    display.close();
  }
}
