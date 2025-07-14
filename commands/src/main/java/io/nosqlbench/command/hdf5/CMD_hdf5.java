package io.nosqlbench.command.hdf5;

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

import io.nosqlbench.command.hdf5.subcommands.BuildHdf5;
import io.nosqlbench.command.hdf5.subcommands.ExportHdf5;
import io.nosqlbench.command.hdf5.subcommands.ShowHdf5;
import io.nosqlbench.command.hdf5.subcommands.TagHdf5;
import io.nosqlbench.nbdatatools.api.services.BundledCommand;
import io.nosqlbench.nbdatatools.api.services.Selector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.List;

/// HDF5 file operations for NoSQLBench
/// 
/// This command provides utilities for working with HDF5 files, including:
/// 
/// - `tag`: Read or write HDF5 attributes
/// - `show`: Show details of HDF5 KNN test data files
/// - `build`: Build HDF5 KNN test data answer-keys from JSON
/// - `export`: Export HDF5 KNN answer-keys from other formats
/// 
/// These commands help manage and manipulate HDF5 files for testing and data preparation.
@Selector("hdf5")
@CommandLine.Command(name = "hdf5",
    header = "HDF5 file operations for vector test data",
    description = """
        This provides a set of utilities for working with HDF5 files.
        The command includes subcommands for tagging HDF5 files with attributes,
        showing details of HDF5 KNN test data files, building HDF5 KNN test
        data answer-keys from JSON, and exporting HDF5 KNN answer-keys from other formats.
        """,
    exitCodeListHeading = "Exit Codes:%n",
    exitCodeList = {"0:success", "1:warning", "2:error"},
    subcommands = {TagHdf5.class, ShowHdf5.class, BuildHdf5.class, ExportHdf5.class, CommandLine.HelpCommand.class})

public class CMD_hdf5 implements BundledCommand {
  /// Logger for this class
  private static final Logger logger = LogManager.getLogger(CMD_hdf5.class);

  /// List of command arguments passed to the hdf5 command
  @CommandLine.Parameters(description = "commands")
  private List<String> commands = new ArrayList<>();

  /// Create the default CMD_hdf5 command
  /// 
  /// Default constructor that initializes a new hdf5 command instance.
  public CMD_hdf5() {
  }

  /// Run an hdf5 command
  /// 
  /// This method is the entry point for the hdf5 command when run from the command line.
  /// It creates a new command instance, configures the command line parser, and executes
  /// the command with the provided arguments.
  /// 
  /// @param args Command line arguments passed to the hdf5 command
  public static void main(String[] args) {
    logger.info("Creating hdf5 command");
    CMD_hdf5 cmd = new CMD_hdf5();
    logger.info("Executing command line");
    int exitCode = new CommandLine(cmd)
        .setCaseInsensitiveEnumValuesAllowed(true)
        .setOptionsCaseInsensitive(true)
        .execute(args);
    logger.info("Exiting main with code: {}", exitCode);
    System.exit(exitCode);
  }
}
