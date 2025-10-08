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

import io.nosqlbench.vectordata.discovery.ProfileSelector;
import io.nosqlbench.vectordata.utils.SHARED;
import io.nosqlbench.vectordata.layoutv2.DSProfileGroup;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Map;

/// Represents an entry in a dataset catalog.
///
/// A dataset entry contains metadata about a dataset, including its name, URL,
/// attributes, profiles, and tags. It provides methods for downloading the dataset
/// and selecting profiles for use.
public class DatasetEntry {
    /// The name of the dataset
    private final String name;
    /// The URL where the dataset can be downloaded from
    private final URL url;
    /// Additional attributes associated with the dataset
    private final Map<String, String> attributes;
    /// The profiles available for this dataset
    private final DSProfileGroup profiles;
    /// Tags associated with the dataset for categorization
    private final Map<String, String> tags;
    
    public DatasetEntry(String name, URL url, Map<String, String> attributes, DSProfileGroup profiles, Map<String, String> tags) {
        this.name = name;
        this.url = url;
        this.attributes = attributes;
        this.profiles = profiles;
        this.tags = tags;
    }
    
    /// @return The name of the dataset
    public String name() {
        return name;
    }
    
    /// @return The URL where the dataset can be downloaded from
    public URL url() {
        return url;
    }
    
    /// @return Additional attributes associated with the dataset
    public Map<String, String> attributes() {
        return attributes;
    }
    
    /// @return The profiles available for this dataset
    public DSProfileGroup profiles() {
        return profiles;
    }
    
    /// @return Tags associated with the dataset for categorization
    public Map<String, String> tags() {
        return tags;
    }
  /// Creates a DatasetEntry from a data object.
  ///
  /// @param entryObj The object containing dataset entry data
  /// @return A new DatasetEntry instance
  public static DatasetEntry fromData(Object entryObj) {
    Map<String, ?> entry = null;
    if (entryObj instanceof CharSequence) {
      CharSequence cs = (CharSequence) entryObj;
      entry = SHARED.mapFromJson(entryObj.toString());
    } else if (entryObj instanceof Map<?, ?>) {
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
    // Use ChunkedResourceTransportService directly
    ChunkedResourceTransportService transportService = new ChunkedResourceTransportService();
    
    // Delegate download to the transport service
    return transportService.downloadResource(url, target, force);
  }

  /// Creates a ProfileSelector for selecting profiles from this dataset.
  ///
  /// @return A ProfileSelector for this dataset
  public ProfileSelector select() {
    return new VirtualProfileSelector(this);
  }
}
