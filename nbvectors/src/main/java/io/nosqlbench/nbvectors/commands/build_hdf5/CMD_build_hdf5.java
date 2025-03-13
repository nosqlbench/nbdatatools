package io.nosqlbench.nbvectors.commands.build_hdf5;

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


import io.nosqlbench.nbvectors.commands.jjq.evaluator.JJQInvoker;
import io.nosqlbench.nbvectors.commands.jjq.outputs.BufferOutput;
import io.nosqlbench.nbvectors.commands.verify_knn.logging.CustomConfigurationFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

/// Build HDF datafiles from the provided layout, including source files, jq recipes, analysis
/// functions, and JSON object templates
@CommandLine.Command(name = "build_hdf5",
    headerHeading = "Usage:%n%n",
    synopsisHeading = "%n",
    descriptionHeading = "%nDescription%n%n",
    parameterListHeading = "%nParameters:%n%",
    optionListHeading = "%nOptions:%n",
    header = "build HDF5 KNN test data answer-keys from JSON",
    description = """
        TBD
        """,
    exitCodeListHeading = "Exit Codes:%n",
    exitCodeList = {
        "0: no errors",
    })
public class CMD_build_hdf5 implements Callable<Integer> {

  private static final Logger logger = LogManager.getLogger(CMD_build_hdf5.class);

  @Option(names = {"-o", "--outfile"},
      required = true,
      defaultValue = "out.hdf5",
      description = "The " + "HDF5" + " file to " + "write")
  private Path hdfOutPath;

  @Option(names = {"-l", "--layout"},
      required = true,
      defaultValue = "layout.yaml",
      description = "The yaml file containing the layout " + "instructions.")
  private Path layoutPath;

  /// run a build_hdf5 command
  /// @param args command line args
  @Option(names = {"--_diaglevel", "-_d"}, hidden = true, description = """
      Internal diagnostic level, sends content directly to the console.""", defaultValue = "ERROR")
  public static void main(String[] args) {

    System.setProperty("slf4j.internal.verbosity", "ERROR");
    System.setProperty(
        ConfigurationFactory.CONFIGURATION_FACTORY_PROPERTY,
        CustomConfigurationFactory.class.getCanonicalName()
    );

    //    System.setProperty("slf4j.internal.verbosity", "DEBUG");
    CMD_build_hdf5 command = new CMD_build_hdf5();
    logger.info("instancing commandline");
    CommandLine commandLine = new CommandLine(command).setCaseInsensitiveEnumValuesAllowed(true)
        .setOptionsCaseInsensitive(true);
    logger.info("executing commandline");
    int exitCode = commandLine.execute(args);
    logger.info("exiting main");
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {

    MapperConfig config = MapperConfig.file(layoutPath);

    for (MapperConfig.RemapConfig rmapper : config.getMappers()) {
      System.err.println("running premapping phase " + rmapper.name());
      Supplier<String> input = JJQSupplier.path(rmapper.file());
      String expr = rmapper.expr();
      BufferOutput output = new BufferOutput(5000000);
      try (JJQInvoker invoker = new JJQInvoker(input, expr, output)) {
        invoker.run();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    try (KnnDataWriter kwriter = new KnnDataWriter(hdfOutPath, new JsonLoader(config))) {
      kwriter.writeHdf5();
    }
    return 0;
  }

}
