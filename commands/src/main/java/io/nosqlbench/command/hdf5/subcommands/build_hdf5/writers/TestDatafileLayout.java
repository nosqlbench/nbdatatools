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
import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/// a group layout is a list of test data layouts, each for a single dataset entry in the group
/// @param layouts
///     the list of test data layouts
public record TestDatafileLayout(List<TestDatasetLayout> layouts) {
  /// load a group layout from a file
  /// @param layout
  ///     the path to the layout file
  /// @return a group layout
  public static TestDatafileLayout load(Path layout) {
    LoadSettings loadSettings = LoadSettings.builder().build();
    Load yaml = new Load(loadSettings);
    String layoutYaml;
    try {
      layoutYaml = Files.readString(layout);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    List<Map<?, ?>> slabs = (List<Map<?, ?>>) yaml.loadFromString(layoutYaml);
    List<TestDatasetLayout> layouts =
        slabs.stream().map(TestDatasetLayout::fromMap).toList();

    return new TestDatafileLayout(layouts);
  }

  /// group the layouts by kind
  /// @return a map of kind to layouts
  public Map<TestDataKind, DatasetLayoutByKind> grouped() {
    Map<TestDataKind, List<TestDatasetLayout>> kindMap =
        layouts.stream().collect(Collectors.groupingBy(TestDatasetLayout::kind));
    Map<TestDataKind, DatasetLayoutByKind> sorted = new LinkedHashMap<>();
    kindMap.forEach((k, v) -> {
      Collections.sort(v);
      sorted.put(k, new DatasetLayoutByKind(k, v));
    });
    return sorted;
  }
}
