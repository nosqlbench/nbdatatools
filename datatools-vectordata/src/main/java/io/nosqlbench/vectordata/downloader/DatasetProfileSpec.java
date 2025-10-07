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

import java.util.Objects;
import java.util.Optional;

/// Immutable value that captures a dataset identifier and an optional profile name.
/// This is used to normalize callers that want to specify dataset and profile either in
/// separate fluent calls or as a single colon-delimited string, optionally using backslash
/// escaping for literal delimiters.
public final class DatasetProfileSpec {

  private final String dataset;
  private final String profile;

  public DatasetProfileSpec(String dataset, String profile) {
    this.dataset = Objects.requireNonNull(dataset, "dataset");
    this.profile = profile;
  }

  /// @return The dataset name component
  public String dataset() {
    return dataset;
  }

  /// @return An optional profile component if one was provided
  public Optional<String> profile() {
    return Optional.ofNullable(profile);
  }

  /// @return true when a profile component was provided
  public boolean hasProfile() {
    return profile != null && !profile.isEmpty();
  }

  /// Parse the provided specification into dataset and optional profile parts.
  /// Supports escaping the ':' delimiter with a backslash. Leading and trailing
  /// whitespace around each component is trimmed.
  ///
  /// @param spec The raw dataset or dataset:profile string
  /// @return A normalized DatasetProfileSpec
  /// @throws IllegalArgumentException when the spec is null, empty, or malformed
  public static DatasetProfileSpec parse(String spec) {
    if (spec == null) {
      throw new IllegalArgumentException("dataset spec must not be null");
    }
    String trimmed = spec.trim();
    if (trimmed.isEmpty()) {
      throw new IllegalArgumentException("dataset spec must not be empty");
    }

    StringBuilder datasetBuilder = new StringBuilder();
    StringBuilder profileBuilder = null;
    boolean escaping = false;

    for (int i = 0; i < trimmed.length(); i++) {
      char ch = trimmed.charAt(i);
      if (escaping) {
        // Append any escaped character literally
        if (profileBuilder == null) {
          datasetBuilder.append(ch);
        } else {
          profileBuilder.append(ch);
        }
        escaping = false;
        continue;
      }
      if (ch == '\\') {
        escaping = true;
        continue;
      }
      if (ch == ':' && profileBuilder == null) {
        profileBuilder = new StringBuilder();
        continue;
      }
      if (profileBuilder == null) {
        datasetBuilder.append(ch);
      } else {
        profileBuilder.append(ch);
      }
    }

    if (escaping) {
      throw new IllegalArgumentException("dataset spec contains dangling escape: '" + spec + "'");
    }

    String dataset = datasetBuilder.toString().trim();
    if (dataset.isEmpty()) {
      throw new IllegalArgumentException("dataset component must not be empty in spec '" + spec + "'");
    }

    String profile = profileBuilder != null ? profileBuilder.toString().trim() : null;
    if (profile != null && profile.isEmpty()) {
      profile = null;
    }

    return new DatasetProfileSpec(dataset, profile);
  }

  /// Create a spec from discrete dataset and profile values.
  public static DatasetProfileSpec of(String dataset, String profile) {
    if (dataset == null || dataset.trim().isEmpty()) {
      throw new IllegalArgumentException("dataset must not be null or empty");
    }
    return new DatasetProfileSpec(dataset.trim(), profile != null ? profile.trim() : null);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DatasetProfileSpec)) {
      return false;
    }
    DatasetProfileSpec that = (DatasetProfileSpec) o;
    return dataset.equals(that.dataset) && Objects.equals(profile, that.profile);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dataset, profile);
  }

  @Override
  public String toString() {
    if (!hasProfile()) {
      return dataset;
    }
    return dataset + ":" + profile;
  }
}
