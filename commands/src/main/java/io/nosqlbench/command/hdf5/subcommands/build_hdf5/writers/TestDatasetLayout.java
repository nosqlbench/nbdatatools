package io.nosqlbench.command.hdf5.subcommands.build_hdf5.writers;

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


import io.nosqlbench.vectordata.spec.datasets.types.TestDataKind;
import org.jetbrains.annotations.NotNull;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

/// This record type captures the source and bounds of data for a single dataset entry
/// in the standard HDF5 KNN answer key format.
/// @param kind
///     the kind of dataset (base, query, neighbor, distance, filter, ...)
/// @param path
///     the path to the source file (dir for parquet, ivec, fvec, bvec, ...)
/// @param config
///     the name of the config to use for the dataset. This is analogous to the config concept
///      from huggingface datasets. If there is only one config, then this should be set to
///     "default"
/// @param start
///     the startInclusive index of the data in the source file (startInclusive inclusive) (set to -1 for all)
/// @param end
///     the end index of the data in the source file (end exclusive) (set to -1 for all)
public record TestDatasetLayout(
    TestDataKind kind, Path path, String config, long start, long end
) implements Comparable<TestDatasetLayout>
{

  @Override
  public int compareTo(@NotNull TestDatasetLayout o) {
    if (this.kind != o.kind) {
      return this.kind.compareTo(o.kind);
    } else if (this.start == o.start) {
      return this.end == o.end ? 0 : Long.compare(this.end, o.end);
    }
    return Long.compare(this.start, o.start);
  }

  /// get the total number of vectors in the dataset
  /// @return the total number of vectors in the dataset
  public long total() {
    return end - start;
  }

  /// create a dataset layout from a map
  /// @param map the map to create the dataset layout from
  /// @return the dataset layout
  public static TestDatasetLayout fromMap(Map<?, ?> map) {
    return new TestDatasetLayout(
        TestDataKind.fromString((String) map.get("kind")),
        Path.of((String) map.get("path")),
        (String) map.get("config"),
        Optional.ofNullable(map.get("startInclusive")).map(String::valueOf).map(Long::parseLong).orElse(-1L),
        Optional.ofNullable(map.get("end")).map(String::valueOf).map(Long::parseLong).orElse(-1L)
    );
  }
}
