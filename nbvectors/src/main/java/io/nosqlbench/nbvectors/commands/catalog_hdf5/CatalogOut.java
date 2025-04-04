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
import org.snakeyaml.engine.v2.api.Dump;
import org.snakeyaml.engine.v2.api.DumpSettings;
import org.snakeyaml.engine.v2.api.DumpSettingsBuilder;
import org.snakeyaml.engine.v2.api.StreamDataWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/// Catalog of data for HDF5 files
public class CatalogOut extends ArrayList<Map<String, Object>> {
  private final static Logger logger = LogManager.getLogger(CatalogOut.class);
  private final static Gson gson = new GsonBuilder().setPrettyPrinting().create();
  private final static Dump dump = new Dump(DumpSettings.builder().build());
  private final static Hdf5JsonSummarizer jsonSummarizer = new Hdf5JsonSummarizer();

  /// create a catalog
  /// @param entries the entries to add
  public CatalogOut(List<Map<String, Object>> entries) {
    super(entries);
    //    if (mode==CatalogMode.update) {
    //      if (Files.exists(path)) {
    //        try {
    //          this.putAll(gson.fromJson(Files.newBufferedReader(path), Map.class));
    //        } catch (IOException e) {
    //          throw new RuntimeException(e);
    //        }
    //      } else {
    //        logger.warn("catalog file does not exist for update mode, will create a new one: " + path);
    //      }
    //    }
  }

  /// load all files and directories into the catalog
  /// @param hdf5Files
  ///     the files and directories to load
  public static CatalogOut loadAll(List<Path> hdf5Files) {
    List<Map<String, Object>> entries = new ArrayList<>();
    for (Path hdf5File : hdf5Files) {
      if (Files.isDirectory(hdf5File)) {
        entries.addAll(loadDirectory(hdf5File));
      } else if (Files.isRegularFile(hdf5File)) {
        entries.add(loadFile(hdf5File));
      } else {
        throw new RuntimeException("not a file or directory: " + hdf5File);
      }
    }
    return new CatalogOut(entries);
  }

  private static List<Map<String, Object>> loadDirectory(Path hdf5File) {
    List<Map<String, Object>> entries = new ArrayList<>();
    try {
      for (Path path : Files.newDirectoryStream(hdf5File, "*.hdf5")) {
        Map<String, Object> map = loadFile(path);
        entries.add(map);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return entries;
  }

  private static Map<String, Object> loadFile(Path path) {
    try {
      //      String summary = jsonSummarizer.apply(path);
      Map<String, Object> map = jsonSummarizer.describeFile(path);
      return map;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /// save the catalog to a file
  /// @param path the path to the file to save to
  public void save(Path path) {
    try {
      Files.writeString(path, gson.toJson(this));
      Path yamlPath =
          path.resolveSibling(path.getFileName().toString().replaceFirst("\\.json$", ".yaml"));
      Files.writeString(yamlPath, dump.dumpToString(this));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
