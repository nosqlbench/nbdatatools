package io.nosqlbench.command.cleanup;

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

import io.nosqlbench.command.cleanup.subcommands.CMD_cleanfvec;
import io.nosqlbench.nbdatatools.api.services.BundledCommand;
import io.nosqlbench.nbdatatools.api.services.Selector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.List;

/// Cleanup operations for vector test data
/// 
/// This command provides utilities for cleaning up various data files, including:
/// 
/// - `cleanfvec`: Clean an input fvec file by removing zero and duplicate vectors
/// 
/// These commands help maintain and optimize data files used in NoSQLBench.
@Selector("cleanup")
@CommandLine.Command(name = "cleanup",
    headerHeading = "Usage:%n%n",
    synopsisHeading = "%n",
    descriptionHeading = "%nDescription%n%n",
    parameterListHeading = "%nParameters:%n%",
    optionListHeading = "%nOptions:%n",
    header = "Cleanup operations for vector test data",
    description = """
        This provides a set of utilities for cleaning up various data files.
        The command includes subcommands for cleaning fvec files by removing
        zero and duplicate vectors, and potentially other cleanup operations
        in the future.
        """,
    exitCodeListHeading = "Exit Codes:%n",
    exitCodeList = {"0:success", "1:warning", "2:error"},
    subcommands = {CMD_cleanfvec.class, CommandLine.HelpCommand.class})

public class CMD_cleanup implements BundledCommand {
    /// Logger for this class
    private static final Logger logger = LogManager.getLogger(CMD_cleanup.class);

    /// List of command arguments passed to the cleanup command
    @CommandLine.Parameters(description = "commands")
    private List<String> commands = new ArrayList<>();

    /// Create the default CMD_cleanup command
    /// 
    /// Default constructor that initializes a new cleanup command instance.
    public CMD_cleanup() {
    }

    /// Run a cleanup command
    /// 
    /// This method is the entry point for the cleanup command when run from the command line.
    /// It creates a new command instance, configures the command line parser, and executes
    /// the command with the provided arguments.
    /// 
    /// @param args Command line arguments passed to the cleanup command
    public static void main(String[] args) {
        logger.info("Creating cleanup command");
        CMD_cleanup cmd = new CMD_cleanup();
        logger.info("Executing command line");
        int exitCode = new CommandLine(cmd)
            .setCaseInsensitiveEnumValuesAllowed(true)
            .setOptionsCaseInsensitive(true)
            .execute(args);
        logger.info("Exiting main with code: {}", exitCode);
        System.exit(exitCode);
    }
}
