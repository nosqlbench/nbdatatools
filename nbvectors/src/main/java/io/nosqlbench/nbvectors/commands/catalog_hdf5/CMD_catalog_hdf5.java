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

import io.jhdf.HdfFile;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;
import io.nosqlbench.nbvectors.commands.build_hdf5.predicates.types.PNode;
import io.nosqlbench.nbvectors.commands.export_json.CMD_export_json;
import io.nosqlbench.nbvectors.commands.export_json.Hdf5JsonSummarizer;
import io.nosqlbench.nbvectors.commands.show_hdf5.DatasetNames;
import io.nosqlbench.nbvectors.commands.verify_knn.logging.CustomConfigurationFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import picocli.CommandLine;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/// Show details of HDF5 vector data files
@CommandLine.Command(name = "catalog_hdf5",
    headerHeading = "Usage:%n%n",
    synopsisHeading = "%n",
    descriptionHeading = "%nDescription%n%n",
    parameterListHeading = "%nParameters:%n%",
    optionListHeading = "%nOptions:%n",
    description = "Create catalog views of HDF5 files",
    exitCodeListHeading = "Exit Codes:%n",
    exitCodeList = {
        "0: no errors",
    })
public class CMD_catalog_hdf5 implements Callable<Integer> {

  private static final Logger logger = LogManager.getLogger(CMD_catalog_hdf5.class);

  @CommandLine.Parameters(description = "Files and/or directories to catalog; All files in the "
                                        + "specified directories and paths are added to the "
                                        + "catalog files", defaultValue = ".")
  private List<Path> hdf5Files;

  @CommandLine.Option(names = {"--basename"},
      description = "The basename to use for the catalog",
      defaultValue = "catalog")
  private String basename;

  @CommandLine.Option(names = {"--mode"},
      description = "The mode to use for the catalog ; Valid values: ${COMPLETION-CANDIDATES}",
      defaultValue = "create")

  private CatalogMode mode;

  /// run a catalog_hdf5 command
  /// @param args
  ///     command line args
  public static void main(String[] args) {

    System.setProperty("slf4j.internal.verbosity", "ERROR");
    System.setProperty(
        ConfigurationFactory.CONFIGURATION_FACTORY_PROPERTY,
        CustomConfigurationFactory.class.getCanonicalName()
    );

    //    System.setProperty("slf4j.internal.verbosity", "DEBUG");
    CMD_catalog_hdf5 command = new CMD_catalog_hdf5();
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
    if (basename.contains(".")) {
      throw new RuntimeException("basename must not contain extension (or dot character)");
    }

    Catalog catalog = new Catalog(Path.of(basename + ".json"), mode);
    catalog.loadAll(hdf5Files);
    System.out.println(catalog);

    return 0;
  }
}
