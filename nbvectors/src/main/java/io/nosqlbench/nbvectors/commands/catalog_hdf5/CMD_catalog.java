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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;

/// Show details of HDF5 vector data files
@CommandLine.Command(name = "catalog",
    headerHeading = "Usage:%n%n",
    synopsisHeading = "%n",
    descriptionHeading = "%nDescription%n%n",
    parameterListHeading = "%nParameters:%n%",
    optionListHeading = "%nOptions:%n",
    description = "Create catalog views of known formats",
    exitCodeListHeading = "Exit Codes:%n",
    exitCodeList = {
        "0: no errors",
    },subcommands = {CommandLine.HelpCommand.class})
public class CMD_catalog implements Callable<Integer> {

  private static final Logger logger = LogManager.getLogger(CMD_catalog.class);

  @CommandLine.Parameters(description = "Files and/or directories to catalog; All files in the "
                                        + "specified directories and paths are added to the "
                                        + "catalog files", defaultValue = ".")
  private List<Path> paths;

  @CommandLine.Option(names = {"--basename"},
      description = "The basename to use for the catalog",
      defaultValue = "catalog")
  private String basename;
  
  /// run a catalog_hdf5 command
  /// @param args
  ///     command line args
  public static void main(String[] args) {

//    System.setProperty("slf4j.internal.verbosity", "ERROR");
//    System.setProperty(
//        ConfigurationFactory.CONFIGURATION_FACTORY_PROPERTY,
//        CustomConfigurationFactory.class.getCanonicalName()
//    );

    //    System.setProperty("slf4j.internal.verbosity", "DEBUG");
    CMD_catalog command = new CMD_catalog();
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
    if (this.paths.isEmpty()) {
      throw new RuntimeException("no files specified");
    }

    if (basename.contains(".")) {
      throw new RuntimeException("basename must not contain extension (or dot character)");
    }
    CatalogOut catalog = CatalogOut.loadAll(this.paths);
    catalog.save(Path.of(basename + ".json"));

    return 0;
  }
}
