package io.nosqlbench.command.hdf5.subcommands.export_hdf5;

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


import io.nosqlbench.vectordata.spec.attributes.RootGroupAttributes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/// Config for a single hdf5 file, with some required and some optional components from other files
public class DataGroupConfig {
  private final Optional<Path> base_vectors;
  private final Optional<Path> query_vectors;
  private final Optional<Path> neighbors;
  private final Optional<Path> distances;
  private final Optional<Path> base_content;
  private final Optional<Path> query_terms;
  private final Optional<Path> query_filters;
  private final Optional<Path> layout;
  private final RootGroupAttributes metadata;

  public DataGroupConfig(Optional<Path> base_vectors, Optional<Path> query_vectors,
                       Optional<Path> neighbors, Optional<Path> distances,
                       Optional<Path> base_content, Optional<Path> query_terms,
                       Optional<Path> query_filters, Optional<Path> layout,
                       RootGroupAttributes metadata) {
    this.base_vectors = base_vectors;
    this.query_vectors = query_vectors;
    this.neighbors = neighbors;
    this.distances = distances;
    this.base_content = base_content;
    this.query_terms = query_terms;
    this.query_filters = query_filters;
    this.layout = layout;
    this.metadata = metadata;
  }

  public Optional<Path> base_vectors() {
    return base_vectors;
  }

  public Optional<Path> query_vectors() {
    return query_vectors;
  }

  public Optional<Path> neighbors() {
    return neighbors;
  }

  public Optional<Path> distances() {
    return distances;
  }

  public Optional<Path> base_content() {
    return base_content;
  }

  public Optional<Path> query_terms() {
    return query_terms;
  }

  public Optional<Path> query_filters() {
    return query_filters;
  }

  public Optional<Path> layout() {
    return layout;
  }

  public RootGroupAttributes metadata() {
    return metadata;
  }
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
        RootGroupAttributes.fromMap(new LinkedHashMap<>(cfg))
    );

  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DataGroupConfig that = (DataGroupConfig) o;
    return Objects.equals(base_vectors, that.base_vectors) &&
           Objects.equals(query_vectors, that.query_vectors) &&
           Objects.equals(neighbors, that.neighbors) &&
           Objects.equals(distances, that.distances) &&
           Objects.equals(base_content, that.base_content) &&
           Objects.equals(query_terms, that.query_terms) &&
           Objects.equals(query_filters, that.query_filters) &&
           Objects.equals(layout, that.layout) &&
           Objects.equals(metadata, that.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(base_vectors, query_vectors, neighbors, distances,
                       base_content, query_terms, query_filters, layout, metadata);
  }

  @Override
  public String toString() {
    return "DataGroupConfig{" +
           "base_vectors=" + base_vectors +
           ", query_vectors=" + query_vectors +
           ", neighbors=" + neighbors +
           ", distances=" + distances +
           ", base_content=" + base_content +
           ", query_terms=" + query_terms +
           ", query_filters=" + query_filters +
           ", layout=" + layout +
           ", metadata=" + metadata +
           '}';
  }
}
