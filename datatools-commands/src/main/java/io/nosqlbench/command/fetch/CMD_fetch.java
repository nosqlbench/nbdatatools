package io.nosqlbench.command.fetch;

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

import io.nosqlbench.command.fetch.subcommands.CMD_dlhf;
import io.nosqlbench.nbdatatools.api.services.BundledCommand;
import io.nosqlbench.nbdatatools.api.services.Selector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.List;

/// Hugging Face operations for NoSQLBench
/// 
/// This command provides utilities for working with Hugging Face, including:
/// 
/// - `dl`: Download Hugging Face datasets via API
/// 
/// These commands help manage and manipulate Hugging Face resources for testing and data preparation.
@Selector("fetch")
@CommandLine.Command(name = "fetch",
    header = "Commands to download datasets for processing",
    subcommands = {CMD_dlhf.class, CommandLine.HelpCommand.class})

public class CMD_fetch implements BundledCommand {
  /// Logger for this class
  private static final Logger logger = LogManager.getLogger(CMD_fetch.class);

  /// List of command arguments passed to the huggingface command
  @CommandLine.Parameters(description = "commands")
  private List<String> commands = new ArrayList<>();

  /// Create the default CMD_huggingface command
  /// 
  /// Default constructor that initializes a new huggingface command instance.
  public CMD_fetch() {
  }

  /// Run a huggingface command
  /// 
  /// This method is the entry point for the huggingface command when run from the command line.
  /// It creates a new command instance, configures the command line parser, and executes
  /// the command with the provided arguments.
  /// 
  /// @param args Command line arguments passed to the huggingface command
  public static void main(String[] args) {
    logger.info("Creating huggingface command");
    CMD_fetch cmd = new CMD_fetch();
    logger.info("Executing command line");
    int exitCode = new CommandLine(cmd)
        .setCaseInsensitiveEnumValuesAllowed(true)
        .setOptionsCaseInsensitive(true)
        .execute(args);
    logger.info("Exiting main with code: {}", exitCode);
    System.exit(exitCode);
  }
}