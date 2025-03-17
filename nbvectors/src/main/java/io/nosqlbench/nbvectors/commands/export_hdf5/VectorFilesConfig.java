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


import io.nosqlbench.nbvectors.spec.attributes.RootGroupAttributes;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/// Config for a single hdf5 file, with some required and some optional components from other files
///  @param base_vectors
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

public record VectorFilesConfig(
    Path base_vectors,
    Path query_vectors,
    Path neighbors,
    Path distances,
    Optional<Path> base_content,
    Optional<Path> query_terms,
    Optional<Path> query_filters,
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
  /// @param metadata
  ///     the metadata
  public VectorFilesConfig(
      Path base_vectors,
      Path query_vectors,
      Path neighbors,
      Path distances,
      Path base_content,
      Path query_terms,
      Path query_filters,
      Map<String, String> metadata
  )
  {
    this(
        base_vectors,
        query_vectors,
        neighbors,
        distances,
        Optional.ofNullable(base_content),
        Optional.ofNullable(query_terms),
        Optional.ofNullable(query_filters),
        RootGroupAttributes.fromMap(metadata)
    );
  }

  /// create a file config from a config map
  /// @param cfg
  ///     the config map
  /// @return a file config
  public static VectorFilesConfig of(Map<String, String> cfg) {

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

    String distances1 = cfg.remove("distances");
    if (distances1 == null) {
      throw new RuntimeException("distances is required");
    }

    return new VectorFilesConfig(
        Path.of(base),
        Path.of(query),
        Path.of(indices),
        Path.of(distances1),
        Optional.ofNullable(cfg.remove("base_content")).map(Path::of),
        Optional.ofNullable(cfg.remove("query_terms")).map(Path::of),
        Optional.ofNullable(cfg.remove("query_filters")).map(Path::of),
        RootGroupAttributes.fromMap(new LinkedHashMap<>(cfg))
    );

  }
}
