package io.nosqlbench.nbvectors.commands.datasets;

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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/// configuration for the datasets command
/// @param locations the locations to search for datasets
public record DataConfig(List<URL> locations) {

  /// load the config from a config directory and a list of catalogs
  /// @param configdir the config directory
  /// @param catalogs the catalogs to search for datasets
  /// @return a data config
  public static DataConfig load(Path configdir, List<String> catalogs) {
    List<URL> clocations = new ArrayList<>();

    catalogs.forEach(catalog -> {
      clocations.add(createUrl(catalog));
    });

    Path catalogCatalog = configdir.resolve("catalogs.yaml");
    if (Files.exists(catalogCatalog)) {
      LoadSettings loadSettings = LoadSettings.builder().build();
      Load yaml = new Load(loadSettings);
      try {
        Object configs = yaml.loadAllFromString(Files.readString(catalogCatalog));
        if (configs instanceof List<?> list) {
          list.forEach(c -> clocations.add(createUrl((String) c)));
        } else {
          throw new RuntimeException("catalogs.yaml must be a list of strings");
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    if (clocations.isEmpty()) {
      throw new RuntimeException("no catalogs specified, and no ");
    }
    return new DataConfig(clocations);
  }

  private static URL createUrl(String catalog) {
    if (catalog.startsWith("http")) {
      try {
        return new URL(catalog);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      try {
        return Path.of(catalog).toUri().toURL();
      } catch (MalformedURLException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
