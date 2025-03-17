package io.nosqlbench.nbvectors.commands.export_json;

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

import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

public class Hdf5JsonSummarizer implements Function<Path, String> {

  @Override
  public String apply(Path path) {
    return describeFile(path).toString();
  }

  private String summarizeHdf5(HdfFile file) {
    StringBuilder sb = new StringBuilder();
    Map<String, Object> groupMap = describeGroup(file, 0);
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    gson.toJson(groupMap, sb);
    String summary = sb.toString();
//    System.out.println(summary);
    return summary;
  }

  public Map<String, Object> describeFile(Path path) {
    try (HdfFile file = new HdfFile(path)) {
      return describeGroup(file, 0);
    }
  }

  public Map<String, Object> describeNode(Node node, Map<String, Object> parentMap, int level) {
    switch (node) {
      case Dataset dataset:
        Map<String, Object> datasets = (Map<String, Object>) parentMap.computeIfAbsent(
            "datasets",
            k -> new LinkedHashMap<String, Object>()
        );
        datasets.put(dataset.getName(), describeDataset(dataset, level + 1));
        break;
      case Group group:
        Map<String, Object> groups =
            (Map<String, Object>) parentMap.computeIfAbsent("groups", k -> new LinkedHashMap<>());
        groups.put(group.getName(), describeGroup(group, level));
        //        describeAttrs(group, sb, level);
        break;
      default:
        throw new RuntimeException(
            "unknown type to represent: " + node.getClass().getCanonicalName());
    }
    return parentMap;
  }

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
