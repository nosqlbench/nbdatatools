package io.nosqlbench.nbvectors.datasource.parquet.conversion;

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


import org.apache.parquet.example.data.Group;

import java.util.Arrays;
import java.util.function.Function;

/// Enum of available converters
public enum ConverterType {
  /// Hugging Face embeddings format
  HFEMBED(new HFEmbedToFloatAry()),
  /// Hugging Face embeddings format
  EMBEDDINGS_LIST_FLOAT(new EmbeddingsListFloat());

  private final Function<Group, float[]> converter;

  private ConverterType(Function<Group, float[]> converter) {
    this.converter = converter;
  }

  /// get the name of this converter type
  /// @return The name of the converter type
  public String getName() {
    return name().toLowerCase();
  }

  private Function<Group, float[]> getConverter() {
    return converter;
  }

  /// get the converter type from a string
  /// @param name
  ///     The name of the converter type (case-insensitive)
  /// @return The converter type
  /// @throws IllegalArgumentException
  ///     if the converter type name is unknown
  private static ConverterType fromString(String name) {
    for (ConverterType type : values()) {
      if (type.name().equalsIgnoreCase(name)) {
        return type;
      }
    }
    throw new IllegalArgumentException(
        "Unknown converter type: " + name + ". Available types: " + Arrays.toString(values()));
  }

  /// Factory method that creates a Function<Group, float[]> based on the converter type name.
  /// @param name
  ///     The name of the converter type (case-insensitive)
  /// @return The corresponding Function<Group, float[]>
  /// @throws IllegalArgumentException
  ///     if the converter type name is unknown
  public static Function<Group, float[]> createConverter(String name) {
    return fromString(name).getConverter();
  }
}
