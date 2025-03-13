package io.nosqlbench.nbvectors.spec.attributes;

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
import io.nosqlbench.nbvectors.verifyknn.options.DistanceFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
public record SpecAttributes(
    String model, String url, DistanceFunction distance_function, Optional<String> notes
)
{
  /// read the metadata from a file
  /// @param metadataFile
  ///     the path to the metadata file
  /// @return the metadata for this file
  public static SpecAttributes fromFile(Path metadataFile) {
    try {

      if (metadataFile.toString().toLowerCase().endsWith(".json")) {
        Gson gson = new GsonBuilder().create();
        try {
          BufferedReader reader = Files.newBufferedReader(metadataFile);
          Map<String, String> data = gson.fromJson(reader, Map.class);
          return SpecAttributes.fromMap(data);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else {
        throw new RuntimeException("unsupported metadata file type: " + metadataFile);
      }
    } catch (Exception e) {
      throw new RuntimeException("While reading from " + metadataFile + ": " + e.getMessage(), e);
    }

  }

  private static SpecAttributes fromMap(Map<String, String> data) {
    return new SpecAttributes(
        data.get("model"),
        data.get("url"),
        DistanceFunction.valueOf(data.get("distance_function")),
        Optional.ofNullable(data.get("notes"))
    );
  }
}
