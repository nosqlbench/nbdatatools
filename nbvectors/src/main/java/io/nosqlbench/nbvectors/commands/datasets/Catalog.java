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


import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/// a catalog of datasets
public record Catalog(List<DatasetEntry> datasets) {

  /// create a catalog from a config
  /// @param config the config to use
  /// @return a catalog
  public static Catalog of(DataConfig config) {
    List<DatasetEntry> entries = new ArrayList<>();
    Gson gson = new Gson();
    Type entryType = new TypeToken<List<Map<?,?>>>() {}.getType();
    for (URL location : config.locations()) {
      URL fileUrl = fileFor(location);
      try {
        InputStream stream = fileUrl.openStream();
        InputStreamReader sr = new InputStreamReader(stream);
        List.of(Map.of());
        List<Map<?,?>> catalogEntries
            = gson.fromJson(sr, entryType);
        int idx = 0;
        for (Map<?, ?> entry : catalogEntries) {
          DatasetEntry datasetEntry = new DatasetEntry(location, idx, entry);
          entries.add(datasetEntry);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return new Catalog(entries);
  };

  private static URL fileFor(URL location) {
    if (location.toString().endsWith("/catalog.yaml")) {
      return location;
    }
    String withFile= location.toString();
    withFile = withFile.endsWith("/") ? withFile : withFile + "/";
    withFile = withFile + "catalog.yaml";
    try {
      return new URL(withFile);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
