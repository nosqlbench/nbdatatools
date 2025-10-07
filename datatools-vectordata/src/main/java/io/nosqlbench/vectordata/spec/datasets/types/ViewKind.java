package io.nosqlbench.vectordata.spec.datasets.types;

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


import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/// Logical dataset view kinds supported by the vectordata tooling.
///
/// A ViewKind defines the canonical name used when referencing a dataset view inside
/// metadata such as `dataset.yaml`. Each kind also lists the set of acceptable aliases
/// that may appear in historical datasets. New descriptors should emit the enum name
/// (e.g. `base`, `query`) for consistency going forward.
public enum ViewKind {

  base(
      TestDataKind.base_vectors,
      Set.of("base_vectors", "train")
  ),

  query(
      TestDataKind.query_vectors,
      Set.of("query_vectors", "queries", "test")
  ),

  indices(
      TestDataKind.neighbor_indices,
      Set.of("neighbor_indices", "ground_truth", "gt")
  ),

  neighbors(
      TestDataKind.neighbor_distances,
      Set.of("neighbor_distances", "distances")
  );

  /// The dataset kind that this view resolves to within the spec.
  private final TestDataKind datasetKind;

  /// All acceptable aliases (including the primary name) in lowercase for easy matching.
  private final Set<String> allNames;

  ViewKind(TestDataKind datasetKind, Set<String> aliases) {
    this.datasetKind = datasetKind;

    LinkedHashSet<String> names = new LinkedHashSet<>();
    names.add(name().toLowerCase(Locale.ROOT));
    names.add(datasetKind.name().toLowerCase(Locale.ROOT));
    names.addAll(datasetKind.getAllNames().stream()
        .map(alias -> alias.toLowerCase(Locale.ROOT))
        .collect(Collectors.toCollection(LinkedHashSet::new))
    );
    names.addAll(aliases.stream().map(alias -> alias.toLowerCase(Locale.ROOT))
        .collect(Collectors.toCollection(LinkedHashSet::new))
    );

    this.allNames = Collections.unmodifiableSet(names);
  }

  /// @return the underlying TestDataKind represented by this view
  public TestDataKind getDatasetKind() {
    return datasetKind;
  }

  /// @return the full set of accepted aliases for this view kind
  public Set<String> getAllNames() {
    return allNames;
  }

  /// Determine if the provided name matches this view kind (case insensitive).
  public boolean matches(String name) {
    if (name == null) {
      return false;
    }
    return allNames.contains(name.toLowerCase(Locale.ROOT));
  }

  /// Parse a name into an optional ViewKind.
  ///
  /// @param name The name to resolve
  /// @return An Optional containing the matching ViewKind if any alias matches
  public static Optional<ViewKind> fromName(String name) {
    if (name == null) {
      return Optional.empty();
    }
    String normalized = name.toLowerCase(Locale.ROOT);
    for (ViewKind viewKind : values()) {
      if (viewKind.allNames.contains(normalized)) {
        return Optional.of(viewKind);
      }
    }
    return Optional.empty();
  }
}
