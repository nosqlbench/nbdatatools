package io.nosqlbench.nbvectors.commands.export_hdf5;

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


import io.nosqlbench.vectordata.internalapi.datasets.attrs.RootGroupAttributes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/// Config for a single hdf5 file, with some required and some optional components from other files
/// @param base_vectors
///     the base_vectors file to read
/// @param query_vectors
///     the query_vectors file to read
/// @param neighbors
///     the query_neighbors file to read
/// @param distances
///     the query_distances file to read
/// @param query_terms
///     the query_terms file to read
/// @param query_filters
///     the query_filters file to read
/// @param base_content
///     the base_content file to read
/// @param metadata
///     the metadata associated with the overall hdf5 file
/// @param layout
///     the layout file to read
public record DataGroupConfig(
    Optional<Path> base_vectors,
    Optional<Path> query_vectors,
    Optional<Path> neighbors,
    Optional<Path> distances,
    Optional<Path> base_content,
    Optional<Path> query_terms,
    Optional<Path> query_filters,
    Optional<Path> layout,
    RootGroupAttributes metadata
)
{
  /// create a file config
  /// @param base_vectors
  ///     the base_vectors file to read
  /// @param query_vectors
  ///     the query_vectors file to read
  /// @param neighbors
  ///     the query_neighbors file to read
  /// @param distances
  ///     the query_distances file to read
  /// @param query_terms
  ///     the query_terms file to read
  /// @param query_filters
  ///     the query_filters file to read
  /// @param base_content
  ///     the base_content file to read
  /// @param layout
  ///     the layout file to read
  /// @param metadata
  ///     the metadata
  public DataGroupConfig(
      Path base_vectors,
      Path query_vectors,
      Path neighbors,
      Path distances,
      Path base_content,
      Path query_terms,
      Path query_filters,
      Path layout,
      Map<String, String> metadata
  )
  {
    this(
        Optional.ofNullable(base_vectors),
        Optional.ofNullable(query_vectors),
        Optional.ofNullable(neighbors),
        Optional.ofNullable(distances),
        Optional.ofNullable(base_content),
        Optional.ofNullable(query_terms),
        Optional.ofNullable(query_filters),
        Optional.ofNullable(layout),
        RootGroupAttributes.fromMap(metadata)
    );
  }

  /// get the last modified time of the file config
  /// @return the last modified time of the file config
  public Instant getLastModifiedTime() {
    Instant lastModifiedTime = Instant.MIN;
    for (Path path : new Path[]{
        base_vectors.orElse(null), query_vectors.orElse(null), neighbors.orElse(null),
        distances.orElse(null), base_content.orElse(null), query_terms.orElse(null),
        query_filters.orElse(null)
    }) {
      if (path != null) {
        try {
          Instant instant = Files.getLastModifiedTime(path).toInstant();
          lastModifiedTime = lastModifiedTime.isAfter(instant) ? lastModifiedTime : instant;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
    return lastModifiedTime;
  }

  /// create a file config from a config map
  /// @param cfg
  ///     the config map
  /// @return a file config
  public static DataGroupConfig of(Map<String, String> cfg) {

    String base = cfg.remove("base");
    if (base == null) {
      throw new RuntimeException("base is required");
    }

    String query = cfg.remove("query");
    if (query == null) {
      throw new RuntimeException("query is required");
    }

    String indices = cfg.remove("indices");
    if (indices == null) {
      throw new RuntimeException("indices is required");
    }

    return new DataGroupConfig(
        Optional.of(Path.of(base)),
        Optional.of(Path.of(query)),
        Optional.of(Path.of(indices)),
        Optional.ofNullable(cfg.remove("distances")).map(Path::of),
        Optional.ofNullable(cfg.remove("base_content")).map(Path::of),
        Optional.ofNullable(cfg.remove("query_terms")).map(Path::of),
        Optional.ofNullable(cfg.remove("query_filters")).map(Path::of),
        Optional.ofNullable(cfg.remove("layout")).map(Path::of),
        io.nosqlbench.vectordata.internalapi.datasets.attrs.RootGroupAttributes.fromMap(new LinkedHashMap<>(cfg))
    );

  }
}
