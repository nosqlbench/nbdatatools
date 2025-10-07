package io.nosqlbench.command.json.subcommands;

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
import io.jhdf.HdfFile;
import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;
import io.nosqlbench.vectordata.discovery.TestDataGroup;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

/// Summarize an HDF5 file as JSON
public class Hdf5JsonSummarizer implements Function<Path, String> {

  @Override
  public String apply(Path path) {
    Map<String, Object> summaryMap = describeFile(path);
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    String json = gson.toJson(summaryMap);
    return json;
  }

  /// describe a file
  /// @param path the path to the file
  /// @return the summary of the file
  public Map<String, Object> describeFile(Path path) {
    try (HdfFile file = new HdfFile(path)) {
      Map<String, Object> filejson = describeGroup(file, 0);
      TestDataGroup data = new TestDataGroup(file);
      filejson.put("tokens",data.getTokens());
      return filejson;
    }
  }

  /// describe a node
  /// @param node the node to describe
  /// @param parentMap the parent map to add the node to
  /// @param level the level of the node
  /// @return a descriptive map
  public Map<String, Object> describeNode(Node node, Map<String, Object> parentMap, int level) {
    if (node instanceof Dataset) {
      Dataset dataset = (Dataset) node;
      Map<String, Object> datasets = (Map<String, Object>) parentMap.computeIfAbsent(
          "datasets",
          k -> new LinkedHashMap<String, Object>()
      );
      datasets.put(dataset.getName(), describeDataset(dataset, level + 1));
    } else if (node instanceof Group) {
      Group group = (Group) node;
      Map<String, Object> groups =
          (Map<String, Object>) parentMap.computeIfAbsent("groups", k -> new LinkedHashMap<>());
      groups.put(group.getName(), describeGroup(group, level));
      //        describeAttrs(group, sb, level);
    } else {
        throw new RuntimeException(
            "unknown type to represent: " + node.getClass().getCanonicalName());
    }
    return parentMap;
  }

  /// describe a group
  /// @param group the group to describe
  /// @param level the level of the group
  /// @return a descriptive map
  private Map<String, Object> describeGroup(Group group, int level) {
    Map<String, Object> groupMap = new LinkedHashMap<>();
    groupMap.put("name", group.getName());
    addAttrs(group, groupMap);

    for (Node childNode : group.getChildren().values()) {
      describeNode(childNode, groupMap, level + 1);
    }
    return groupMap;
  }

  private Map<String, Object> describeDataset(Dataset dataset, int level)
  {
    Map<String, Attribute> attributes = dataset.getAttributes();
    return Map.of(
        "attributes",
        mapAttrs(dataset),
        "dimensions",
        Arrays.toString(dataset.getDimensions()),
        "layout",
        dataset.getDataLayout(),
        "java_type",
        dataset.getJavaType().getCanonicalName(),
        "data_class",
        dataset.getDataType().getDataClass(),
        "data_size",
        dataset.getDataType().getSize(),
        "data_version",
        dataset.getDataType().getVersion()
    );

  }

  private void addAttrs(Node node, Map<String, Object> map) {
    Map<String, Attribute> attributes = node.getAttributes();
    if (attributes != null && attributes.size() > 0) {
      map.put("attributes", mapAttrs(node));
    }
  }

  private Map<String, String> mapAttrs(Node node) {
    Map<String, Attribute> attrs = node.getAttributes();
    LinkedHashMap<String, String> attrvals = new LinkedHashMap<>();
    attrs.forEach((k, v) -> {
      attrvals.put(k, v.getData().toString());
    });
    return attrvals;
  }

}