package io.nosqlbench.vectordata.internalapi.datasets.attrs;

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


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.jhdf.api.Attribute;
import io.jhdf.api.Group;
import io.nosqlbench.vectordata.internalapi.attributes.DistanceFunction;

import java.util.Map;
import java.util.Optional;

/// This record type captures the basic metadata requirements of the vector test data spec
/// Optional fields are not required to be present, and should correspond to the same level of
/// requirements in the spec
/// @param model
///     The name of the model used to generate the data, if any.
/// @param url
///     The URL of the model used to generate the data, if any, hugging face scorecard preferred
/// @param notes
///     Any notes about the data, if any
/// @param distance_function
///     The distance function used to compute distance between vectors
/// @param license
///     The license for the data
/// @param vendor
///     The vendor of the data
/// @param tags
///     Any tags associated with the data
public record RootGroupAttributes(
    String model,
    String url,
    DistanceFunction distance_function,
    Optional<String> notes,
    String license,
    String vendor,
    Map<String, String> tags
)
{

  /// create a metadata object from a group
  /// @param dataset the dataset to read from
  /// @return the metadata for this file
  public static RootGroupAttributes fromGroup(Group dataset) {
    return new RootGroupAttributes(
        dataset.getAttribute("model").getData().toString(),
        dataset.getAttribute("url").getData().toString(),
        DistanceFunction.valueOf(dataset.getAttribute("distance_function").getData().toString()),
        Optional.ofNullable(dataset.getAttribute("notes")).map(Attribute::getData)
            .map(String::valueOf),
        dataset.getAttribute("license").getData().toString(),
        dataset.getAttribute("vendor").getData().toString(),
        Optional.ofNullable(dataset.getAttribute("tags")).map(Attribute::getData)
            .map(String::valueOf).map(t -> new Gson().fromJson(t, Map.class)).orElse(Map.of())
    );
  }

  /// read the metadata from a map
  /// @param data
  ///     the map of metadata
  /// @return the metadata for this file
  public static RootGroupAttributes fromMap(Map<String, String> data) {
    Gson gson = new GsonBuilder().create();
    return new RootGroupAttributes(
        data.get("model"),
        data.get("url"),
        DistanceFunction.valueOf(
            data.get("distance_function") != null ? data.get("distance_function") : "COSINE"),
        Optional.ofNullable(data.get("notes")),
        data.get("license"),
        data.get("vendor"),
        Optional.ofNullable(data.get("tags")).map(t -> gson.fromJson(t, Map.class)).orElse(Map.of())
    );
  }
}
