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
import io.nosqlbench.nbvectors.commands.verify_knn.logging.CustomConfigurationFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/// Show details of HDF5 vector data files
@CommandLine.Command(name = "export_json",
    headerHeading = "Usage:%n%n",
    synopsisHeading = "%n",
    descriptionHeading = "%nDescription%n%n",
    parameterListHeading = "%nParameters:%n%",
    optionListHeading = "%nOptions:%n",
    header = "export HDF5 KNN answer-keys to JSON summary files",
    description = """
        TBD
        """,
    exitCodeListHeading = "Exit Codes:%n",
    exitCodeList = {
        "0: no errors",
    })
public class CMD_export_json implements Callable<Integer> {

  private static final Logger logger = LogManager.getLogger(CMD_export_json.class);

  @CommandLine.Parameters(description = "The HDF5 files to export to JSON summaries")
  private List<Path> hdf5Files;

  @CommandLine.Option(names = {"--save"}, description = "Save the JSON summaries to files")
  private boolean save;

  @CommandLine.Option(names = {"--force"}, description = "Force overwrite of existing JSON summaries")
  private boolean force;

  /// run an export_json command
  /// @param args
  ///     command line args
  public static void main(String[] args) {
    System.setProperty("slf4j.internal.verbosity", "ERROR");
    System.setProperty(
        ConfigurationFactory.CONFIGURATION_FACTORY_PROPERTY,
        CustomConfigurationFactory.class.getCanonicalName()
    );

    //    System.setProperty("slf4j.internal.verbosity", "DEBUG");
    CMD_export_json command = new CMD_export_json();
    logger.info("instancing commandline");
    CommandLine commandLine = new CommandLine(command).setCaseInsensitiveEnumValuesAllowed(true)
        .setOptionsCaseInsensitive(true);
    logger.info("executing commandline");
    int exitCode = commandLine.execute(args);
    logger.info("exiting main");
    System.exit(exitCode);
  }

  @Override
  public Integer call() {
    for (Path path : this.hdf5Files) {
      summarize(path);
    }
    return 0;
  }

  private void summarize(Path path) {
    try (HdfFile file = new HdfFile(path)) {
      StringBuilder sb = new StringBuilder();
      Map<String, Object> groupMap = describeGroup(file, 0);
      Gson gson = new GsonBuilder().setPrettyPrinting().create();
      gson.toJson(groupMap, sb);
      System.out.println(sb);
      if (this.save) {
        Path jsonPath = Path.of(path.toString().replaceFirst("\\.hdf5$", ".json"));
        if (Files.exists(jsonPath) && !this.force) {
          throw new RuntimeException("file already exists (use --force to override): " + jsonPath);
        }
        try {
          Files.writeString(jsonPath, sb);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private Map<String,Object> walkHdf(Node node, Map<String, Object> parentMap, int level) {
//
//    parentMap.put("name", node.getName());
//    addAttrs(node,parentMap);

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

  private void addAttrs(Node node, Map<String, Object> map) {
    Map<String, Attribute> attributes = node.getAttributes();
    if (attributes!=null && attributes.size()>0) {
      map.put("attributes", mapAttrs(node));
    }
  }

  //  private void describeAttrs(Node node, StringBuilder sb, int level) {
//    Map<String, Attribute> attributes = node.getAttributes();
//    attributes.forEach((max_k, v) -> {
//      sb.append(" ".repeat(level));
//      sb.append("ATTR ");
//      sb.append(max_k).append(": ").append(v).append("\n");
//    });
//  }

  private Map<String, Object> describeGroup(Group group, int level) {
    Map<String, Object> groupMap = new LinkedHashMap<>();
    groupMap.put("name", group.getName());
    addAttrs(group, groupMap);

    for (Node childNode : group.getChildren().values()) {
      walkHdf(childNode, groupMap, level + 1);
    }
    return groupMap;
  }

  private Map<String, String> mapAttrs(Node node) {
    Map<String, Attribute> attrs = node.getAttributes();
    LinkedHashMap<String, String> attrvals = new LinkedHashMap<>();
    attrs.forEach((k, v) -> {
      attrvals.put(k, v.getData().toString());
    });
    return attrvals;

  }
  private Map<String, Object> describeDataset(
      Dataset dataset,
      int level
  )
  {
    Map<String, Attribute> attributes = dataset.getAttributes();
    return Map.of(
        "attributes", mapAttrs(dataset),
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

}
