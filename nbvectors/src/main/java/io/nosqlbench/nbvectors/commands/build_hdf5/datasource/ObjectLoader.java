package io.nosqlbench.nbvectors.commands.build_hdf5.datasource;

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
import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/// a loader for objects from files
public class ObjectLoader {

  private final static LoadSettings loadSettings = LoadSettings.builder().build();
  private final static Load yaml = new Load(loadSettings);

  /// Create an object from a file containing a map of strings with a conversion function.
  /// @param path the path to the file
  /// @param mapper the function to convert the map to an object
  /// @return the object
  /// @throws RuntimeException if the file cannot be read
  public static <T> Optional<T> load(Path path, Function<Map<String, String>, T> mapper) {
    Map<String, String> data = null;
    try {

      if (path.toString().toLowerCase().endsWith(".yaml")) {
        BufferedReader reader = null;
        reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
        data = (Map<String, String>) yaml.loadFromReader(reader);
      } else if (path.toString().toLowerCase().endsWith(".json")) {
        Gson gson = new GsonBuilder().create();
        BufferedReader reader = Files.newBufferedReader(path);
        data = gson.fromJson(reader, Map.class);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return Optional.ofNullable(data).map(mapper);
  }
}
