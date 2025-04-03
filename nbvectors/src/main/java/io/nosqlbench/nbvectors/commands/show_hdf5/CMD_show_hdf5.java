package io.nosqlbench.nbvectors.commands.show_hdf5;

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


import io.jhdf.HdfFile;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;
import io.nosqlbench.vectordata.local.predicates.PNode;
import io.nosqlbench.nbvectors.commands.verify_knn.logging.CustomConfigurationFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import picocli.CommandLine;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

/// Show details of HDF5 vector data files
@CommandLine.Command(name = "show_hdf5",
    headerHeading = "Usage:%n%n",
    synopsisHeading = "%n",
    descriptionHeading = "%nDescription%n%n",
    parameterListHeading = "%nParameters:%n%",
    optionListHeading = "%nOptions:%n",
    description = "show details of HDF5 KNN answer-keys",
    exitCodeListHeading = "Exit Codes:%n",
    exitCodeList = {
        "0: no errors",
    })
public class CMD_show_hdf5 implements Callable<Integer> {

  private static final Logger logger = LogManager.getLogger(CMD_show_hdf5.class);

  @CommandLine.Parameters(description = "The HDF5 file to view")
  private Path file;

  @CommandLine.Option(names = {"--datasets", "-d"},
      description = "Valid values: ${COMPLETION-CANDIDATES}")
  private List<DatasetNames> decode;

  /// run a show_hdf5 command
  /// @param args command line args
  public static void main(String[] args) {

    System.setProperty("slf4j.internal.verbosity", "ERROR");
    System.setProperty(
        ConfigurationFactory.CONFIGURATION_FACTORY_PROPERTY,
        CustomConfigurationFactory.class.getCanonicalName()
    );

    //    System.setProperty("slf4j.internal.verbosity", "DEBUG");
    CMD_show_hdf5 command = new CMD_show_hdf5();
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
    StringBuilder sb = new StringBuilder();

    try (HdfFile file = new HdfFile(this.file)) {
      if (decode==null || decode.isEmpty()) {
        walkHdf(file, sb, 0);
      } else {
        for (DatasetNames dsname : decode) {
          switch (dsname) {
            case filters -> decodeFilters(sb,file.getDatasetByPath(DatasetNames.filters.name()));
            default -> throw new RuntimeException("unable to show decoded dataset " + dsname.name());
          }
        }
      }

    }


    System.out.println(sb);
    return 0;
  }

  private void decodeFilters(StringBuilder sb, Dataset ds) {
    System.out.println("# predicates from filters dataset:");
    int[] dimensions = ds.getDimensions();
    for (int i = 0; i < dimensions[0]; i++) {
      Object datao = ds.getData(new long[]{i, 0}, new int[]{1, dimensions[1]});
      byte[][] data = (byte[][]) datao;
      byte[] datum = data[0];
      PNode<?> node =
          PNode.fromBuffer(ByteBuffer.wrap(datum));
      System.out.printf("predicate[%d]: %s%n",i, node);
    }

  }

  private void walkHdf(Node node, StringBuilder sb, int level) {
    sb.append(" ".repeat(level)).append(node.getName()).append(" (")
        .append(node.getClass().getSimpleName()).append(")\n");
    switch (node) {
      case Dataset dataset:
        describeDataset(dataset, sb, level + 1);
        break;
      case Group group:
        describeGroup(group, sb, level);
        for (Node childNode : group.getChildren().values()) {
          walkHdf(childNode, sb, level + 1);
        }
        break;
      default:
        throw new RuntimeException(
            "unknown type to represent: " + node.getClass().getCanonicalName());
    }

  }

  private void describeGroup(Group group, StringBuilder sb, int level) {
    //    sb.append(" ".repeat(level)).append("group ").append(group.getName()).append("\n");
  }

  private void describeDataset(Dataset dataset, StringBuilder sb, int level) {

    sb.append(" ".repeat(level));
    sb.append("dimensions: ");
    sb.append(Arrays.toString(dataset.getDimensions()));
    sb.append("\n");

    sb.append(" ".repeat(level));
    sb.append("layout: ");
    sb.append(dataset.getDataLayout());
    sb.append("\n");

    sb.append(" ".repeat(level));
    sb.append("java type: ");
    sb.append(dataset.getJavaType().getCanonicalName());
    sb.append("\n");

    sb.append(" ".repeat(level));
    sb.append("data class: ");
    sb.append(dataset.getDataType().getDataClass());
    sb.append("\n");

    sb.append(" ".repeat(level));
    sb.append("data size: ");
    sb.append(dataset.getDataType().getSize());
    sb.append("\n");

    sb.append(" ".repeat(level));
    sb.append("data version: ");
    sb.append(dataset.getDataType().getVersion());
    sb.append("\n");
  }

}
