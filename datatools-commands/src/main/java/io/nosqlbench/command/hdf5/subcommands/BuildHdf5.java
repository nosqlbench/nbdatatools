package io.nosqlbench.command.hdf5.subcommands;

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

import io.nosqlbench.command.hdf5.subcommands.build_hdf5.datasource.JJQSupplier;
import io.nosqlbench.command.hdf5.subcommands.build_hdf5.datasource.MapperConfig;
import io.nosqlbench.command.hdf5.subcommands.build_hdf5.datasource.json.JsonLoader;
import io.nosqlbench.command.hdf5.subcommands.build_hdf5.writers.KnnDataWriter;
import io.nosqlbench.command.json.subcommands.jjq.evaluator.JJQInvoker;
import io.nosqlbench.command.json.subcommands.jjq.outputs.BufferOutput;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

/// Build HDF datafiles from the provided layout, including source files, jq recipes, analysis
/// functions, and JSON object templates
@CommandLine.Command(name = "build",
    headerHeading = "Usage:%n%n",
    synopsisHeading = "%n",
    descriptionHeading = "%nDescription%n%n",
    parameterListHeading = "%nParameters:%n%",
    optionListHeading = "%nOptions:%n",
    header = "build HDF5 KNN test data answer-keys from JSON",
    description = "Build HDF5 KNN test data answer-keys from JSON using the provided layout configuration.",
    exitCodeListHeading = "Exit Codes:%n",
    exitCodeList = {
        "0: no errors",
    }, subcommands = {CommandLine.HelpCommand.class})
public class BuildHdf5 implements Callable<Integer> {

  private static final Logger logger = LogManager.getLogger(BuildHdf5.class);

  @Option(names = {"-o", "--outfile"},
      required = true,
      defaultValue = "out.hdf5",
      description = "The HDF5 file to write")
  private String hdfOutPath;

  @Option(names = {"-l", "--layout"},
      required = true,
      defaultValue = "layout.yaml",
      description = "The yaml file containing the layout instructions.")
  private Path layoutPath;

  /// Create a new BuildHdf5 command
  /// 
  /// Default constructor that initializes a new build subcommand instance.
  public BuildHdf5() {}

  @Override
  public Integer call() throws Exception {
    logger.info("Executing build command with layout: {} and output: {}", layoutPath, hdfOutPath);
    
    MapperConfig config = MapperConfig.file(layoutPath);

    for (MapperConfig.RemapConfig rmapper : config.getMappers()) {
      logger.info("Running premapping phase: {}", rmapper.name());
      Supplier<String> input = JJQSupplier.path(rmapper.file());
      String expr = rmapper.expr();
      BufferOutput output = new BufferOutput(5000000);
      try (JJQInvoker invoker = new JJQInvoker(input, expr, output)) {
        invoker.run();
      } catch (Exception e) {
        logger.error("Error during premapping phase", e);
        throw new RuntimeException(e);
      }
    }

    KnnDataWriter kwriter = new KnnDataWriter(hdfOutPath, new JsonLoader(config));
    kwriter.writeHdf5();

    logger.info("Build command completed successfully");
    return 0;
  }
}