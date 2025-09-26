package io.nosqlbench.command.generate;

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


import io.nosqlbench.command.generate.subcommands.CMD_generate_dataset;
import io.nosqlbench.command.generate.subcommands.CMD_generate_fvecExtract;
import io.nosqlbench.command.generate.subcommands.CMD_generate_ivecExtract;
import io.nosqlbench.command.generate.subcommands.CMD_generate_ivecShuffle;
import io.nosqlbench.command.generate.subcommands.CMD_generate_mktestdata;
import io.nosqlbench.command.generate.subcommands.CMD_generate_vectors;
import io.nosqlbench.nbdatatools.api.services.BundledCommand;
import io.nosqlbench.nbdatatools.api.services.Selector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.List;

/// Generate deterministic data sequences for test data preparation
///
/// This command provides utilities for generating deterministic data sequences
/// that can be used for test data preparation. It includes subcommands for:
///
/// - `dataset`: Generate a complete example dataset with base, query, indices, distances, and dataset.yaml
/// - `ivec-shuffle`: Generate shuffled integer vectors with deterministic ordering
/// - `ivec-extract`: Extract indices from an ivec file using a range specification
/// - `fvec-extract`: Extract data from floating-point vector files
/// - `mktestdata`: Make basic base, query, and ground truth data from a vector space
/// - `vectors`: Generate vector files with specified types and dimensions
///
/// The generated data maintains consistent statistical properties and can be
/// reproduced using the same seed values.
@Selector("generate")
@CommandLine.Command(name = "generate",
    headerHeading = "Usage:%n%n",
    synopsisHeading = "%n",
    descriptionHeading = "%nDescription%n%n",
    parameterListHeading = "%nParameters:%n%",
    optionListHeading = "%nOptions:%n",
    header = "Generate pseudo-random vector test data in various forms",
    description = "This provides a set of basic procedural generation utilities for\n" +
        "the purposes of preparing test data. The command includes subcommands\n" +
        "for generating complete datasets (dataset), shuffled integer vectors\n" +
        "(ivec-shuffle), extracting indices from integer vector files (ivec-extract),\n" +
        "extracting data from floating-point vector files (fvec-extract), generating\n" +
        "vector files with specified types and dimensions (vectors),\n" +
        "and making basic base, query, and ground truth data (mktestdata).",
    exitCodeListHeading = "Exit Codes:%n",
    exitCodeList = {"0:success", "1:warning", "2:error"},
    subcommands = {CMD_generate_dataset.class, CMD_generate_ivecShuffle.class, CMD_generate_ivecExtract.class, CMD_generate_fvecExtract.class, CMD_generate_vectors.class, CMD_generate_mktestdata.class, CommandLine.HelpCommand.class})

public class CMD_generate implements BundledCommand {
  /// Logger for this class
  private static final Logger logger = LogManager.getLogger(CMD_generate.class);

  /// List of command arguments passed to the generate command
  @CommandLine.Parameters(description = "commands")
  private List<String> commands = new ArrayList<>();

  /// Create the default CMD_generate command
  /// 
  /// Default constructor that initializes a new generate command instance.
  public CMD_generate() {
  }

  /// Run a generate command
  /// 
  /// This method is the entry point for the generate command when run from the command line.
  /// It creates a new command instance, configures the command line parser, and executes
  /// the command with the provided arguments.
  /// 
  /// @param args Command line arguments passed to the generate command
  public static void main(String[] args) {
    logger.info("Creating generate command");
    CMD_generate cmd = new CMD_generate();
    logger.info("Executing command line");
    int exitCode = new CommandLine(cmd)
        .setCaseInsensitiveEnumValuesAllowed(true)
        .setOptionsCaseInsensitive(true)
        .execute(args);
    logger.info("Exiting main with code: {}", exitCode);
    System.exit(exitCode);
  }

}
