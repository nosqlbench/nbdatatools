package io.nosqlbench.nbvectors.commands.catalog_hdf5;

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
import io.nosqlbench.nbvectors.commands.export_json.Hdf5JsonSummarizer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Catalog extends LinkedHashMap<String,String> {
  private final static Logger logger = LogManager.getLogger(Catalog.class);
  private final static Gson gson = new GsonBuilder().create();
  private final static Hdf5JsonSummarizer jsonSummarizer = new Hdf5JsonSummarizer();

  public Catalog(Path path, CatalogMode mode) {
    if (mode==CatalogMode.update) {
      if (Files.exists(path)) {
        try {
          this.putAll(gson.fromJson(Files.newBufferedReader(path), Map.class));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else {
        logger.warn("catalog file does not exist for update mode, will create a new one: " + path);
      }
    }
  }

  public void loadAll(List<Path> hdf5Files) {
    for (Path hdf5File : hdf5Files) {
      if (Files.isDirectory(hdf5File)) {
        loadDirectory(hdf5File);
      } else if (!Files.isRegularFile(hdf5File)) {
        throw new RuntimeException("not a file or directory: " + hdf5File);
      } else {
        loadFile(hdf5File);
      }
    }

  }

  private void loadDirectory(Path hdf5File) {
    try {
      for (Path path : Files.newDirectoryStream(hdf5File, "*.hdf5")) {
        loadFile(path);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void loadFile(Path path) {
    try {
//      String summary = jsonSummarizer.apply(path);
      Map<String, Object> map = jsonSummarizer.describeFile(path);
      gson.toJson(map, System.out);
      //      gson.fromJson(Files.newBufferedReader(path), Entry.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
