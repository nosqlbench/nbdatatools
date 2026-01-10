package io.nosqlbench.command.info;

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

import io.nosqlbench.command.info.subcommands.CMD_info_compute;
import io.nosqlbench.command.info.subcommands.CMD_info_file;
import io.nosqlbench.nbdatatools.api.services.BundledCommand;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.util.concurrent.Callable;

/// The info command contains subcommands to show information about the environment and files.
///
/// This is an umbrella command for various informational subcommands.
@CommandLine.Command(name = "info",
    header = "Show information about environment and files",
    description = "Contains subcommands to show environment capabilities and file details",
    subcommands = {
        CMD_info_compute.class,
        CMD_info_file.class
    })
public class CMD_info implements Callable<Integer>, BundledCommand {
    private static final Logger logger = LogManager.getLogger(CMD_info.class);

    /// Run CMD_info
    ///
    /// @param args Command line arguments
    public static void main(String[] args) {
        System.exit(new CommandLine(new CMD_info()).execute(args));
    }

    /// Execute the info command
    ///
    /// @return 0 for success, 1 for error
    @Override
    public Integer call() {
        // Print help information if no subcommand is specified
        CommandLine.usage(this, System.out);
        return 0;
    }
}
