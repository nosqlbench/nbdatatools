package io.nosqlbench.command.compute;

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


import io.nosqlbench.command.compute.subcommands.CMD_compute_knn;
import io.nosqlbench.nbdatatools.api.services.BundledCommand;
import io.nosqlbench.nbdatatools.api.services.Selector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.List;

/// Compute operations on vector data for analysis and evaluation
/// 
/// This command provides utilities for computing operations on vector data
/// that can be used for analysis and evaluation. It includes subcommands for:
/// 
/// - `knn`: Compute k-nearest neighbors ground truth dataset from base and query vectors
/// 
/// The computed results maintain consistent properties and can be
/// reproduced using the same input data and parameters.
@Selector("compute")
@CommandLine.Command(name = "compute",
    headerHeading = "Usage:%n%n",
    synopsisHeading = "%n",
    descriptionHeading = "%nDescription%n%n",
    parameterListHeading = "%nParameters:%n%",
    optionListHeading = "%nOptions:%n",
    header = "Compute operations on vector data for analysis and evaluation",
    description = """
        This provides a set of computational utilities for
        the purposes of analyzing and evaluating vector data. The command includes subcommands
        for computing k-nearest neighbors (knn) to generate ground truth datasets
        from base and query vectors with specified distance metrics.
        """,
    exitCodeListHeading = "Exit Codes:%n",
    exitCodeList = {"0:success", "1:warning", "2:error"},
    subcommands = {CMD_compute_knn.class, CommandLine.HelpCommand.class})

public class CMD_compute implements BundledCommand {
  /// Logger for this class
  private static final Logger logger = LogManager.getLogger(CMD_compute.class);

  /// List of command arguments passed to the compute command
  @CommandLine.Parameters(description = "commands")
  private List<String> commands = new ArrayList<>();

  /// Create the default CMD_compute command
  /// 
  /// Default constructor that initializes a new compute command instance.
  public CMD_compute() {
  }

  /// Run a compute command
  /// 
  /// This method is the entry point for the compute command when run from the command line.
  /// It creates a new command instance, configures the command line parser, and executes
  /// the command with the provided arguments.
  /// 
  /// @param args Command line arguments passed to the compute command
  public static void main(String[] args) {
    logger.info("Creating compute command");
    CMD_compute cmd = new CMD_compute();
    logger.info("Executing command line");
    int exitCode = new CommandLine(cmd)
        .setCaseInsensitiveEnumValuesAllowed(true)
        .setOptionsCaseInsensitive(true)
        .execute(args);
    logger.info("Exiting main with code: {}", exitCode);
    System.exit(exitCode);
  }

}