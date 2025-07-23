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

// TODO: default download location should use one of the established from nb, jvector, hugging
//  face, but should warn if there are multiple


import io.nosqlbench.nbdatatools.api.transport.ChunkedTransportClient;
import io.nosqlbench.nbdatatools.api.transport.ChunkedTransportIO;
import io.nosqlbench.vectordata.discovery.ProfileSelector;
import io.nosqlbench.vectordata.utils.SHARED;
import io.nosqlbench.vectordata.layoutv2.DSProfileGroup;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;

/// Represents an entry in a dataset catalog.
///
/// A dataset entry contains metadata about a dataset, including its name, URL,
/// attributes, profiles, and tags. It provides methods for downloading the dataset
/// and selecting profiles for use.
///
/// @param name The name of the dataset
/// @param url The URL where the dataset can be downloaded from
/// @param attributes Additional attributes associated with the dataset
/// @param profiles The profiles available for this dataset
/// @param tags Tags associated with the dataset for categorization
public record DatasetEntry(
    String name,
    URL url,
    Map<String, String> attributes,
    DSProfileGroup profiles,
    Map<String, String> tags
)
{
  /// Creates a DatasetEntry from a data object.
  ///
  /// @param entryObj The object containing dataset entry data
  /// @return A new DatasetEntry instance
  public static DatasetEntry fromData(Object entryObj) {
    Map<String, ?> entry = null;
    if (entryObj instanceof CharSequence cs) {
      entry = SHARED.mapFromJson(entryObj.toString());
    } else if (entryObj instanceof Map<?, ?> mapObj) {
      entry = (Map<String, Object>) entryObj;
    } else {
      throw new RuntimeException("invalid dataset entry format:" + entryObj);
    }

    String name = entry.containsKey("name") ? entry.get("name").toString() : null;

    URL url = null;
    try {
      url = entry.containsKey("url") ? new URL(entry.get("url").toString()) : null;
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }

    Map<String, String> attributes = (Map<String, String>) entry.get("attributes");
    Map<String, String> tokens = (Map<String, String>) entry.get("tokens");
    Map<String, String> tags = (Map<String, String>) entry.get("tags");

    DSProfileGroup profileGroup = new DSProfileGroup();
    if (entry.containsKey("profiles")) {
      profileGroup = DSProfileGroup.fromData(entry.get("profiles"));
    }
    return new DatasetEntry(name, url, attributes, profileGroup, tags);
  }

  /// Downloads the dataset to the specified target path.
  ///
  /// @param target The path where the dataset should be downloaded
  /// @return A DownloadProgress object to track the download
  public DownloadProgress download(Path target) {
    return download(target, false);
  }

  /// Downloads the dataset to the specified target path with an option to force download.
  ///
  /// @param target The path where the dataset should be downloaded
  /// @param force Whether to force download even if the file already exists
  /// @return A DownloadProgress object to track the download
  public DownloadProgress download(Path target, boolean force) {
    try {
      ChunkedTransportClient transportClient = ChunkedTransportIO.create(url.toString());
      
      // Create a simple download wrapper that adapts ChunkedTransportClient to DownloadProgress
      CompletableFuture<DownloadResult> future = CompletableFuture.supplyAsync(() -> {
        try {
          // Check if file already exists and force parameter
          if (Files.exists(target) && !force) {
            long existingSize = Files.size(target);
            return DownloadResult.skipped(target, existingSize);
          }
          
          // Create parent directories if needed
          Files.createDirectories(target.getParent());
          
          // For now, use a simple approach - this would need to be enhanced
          // to properly integrate with ChunkedTransportClient's capabilities
          throw new UnsupportedOperationException("ChunkedTransportClient integration needs full implementation");
          
        } catch (Exception e) {
          throw new RuntimeException("Download failed: " + e.getMessage(), e);
        }
      });
      
      return new DownloadProgress(target, -1, new AtomicLong(0), future);
      
    } catch (IOException e) {
      // Return a failed download progress
      CompletableFuture<DownloadResult> failedFuture = new CompletableFuture<>();
      failedFuture.completeExceptionally(e);
      return new DownloadProgress(target, -1, new AtomicLong(0), failedFuture);
    }
  }

  /// Creates a ProfileSelector for selecting profiles from this dataset.
  ///
  /// @return A ProfileSelector for this dataset
  public ProfileSelector select() {
    return new VirtualProfileSelector(this);
  }
}
