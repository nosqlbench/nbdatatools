package io.nosqlbench.command.export_hdf5;

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


import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/// A basic config wrapper for the batch_export command's mapping logic.
/// @param files
///     the files to export
/// @param epochTimestamp
///     the epoch timestamp of the config file
public record BatchConfig(Map<String, DataGroupConfig> files, Instant epochTimestamp) {

  /// create a batch config from a file
  /// @param layoutPath
  ///     the path to the config file
  /// @return a batch config
  public static BatchConfig file(Path layoutPath) {
    BufferedReader reader = null;
    Map<String, String> cfgData = null;
    LoadSettings loadSettings = LoadSettings.builder().build();
    Load yaml = new Load(loadSettings);
    try {
      reader = Files.newBufferedReader(layoutPath, StandardCharsets.UTF_8);
      Map<String, Object> config = (Map<String, Object>) yaml.loadFromReader(reader);
      Map<String, DataGroupConfig> fcfg = new LinkedHashMap<>();
      config.forEach((k, v) -> {
        try {
          fcfg.put(k, DataGroupConfig.of((Map<String, String>) v));
        } catch (Exception e) {
          throw new RuntimeException("while configuring props '" + k + "': " + e);
        }
      });

      return new BatchConfig(fcfg,Files.getLastModifiedTime(layoutPath).toInstant());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}

