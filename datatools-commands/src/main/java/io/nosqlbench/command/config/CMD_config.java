package io.nosqlbench.command.config;

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

import io.nosqlbench.command.config.subcommands.CMD_config_init;
import io.nosqlbench.command.config.subcommands.CMD_config_list_mounts;
import io.nosqlbench.command.config.subcommands.CMD_config_show;
import io.nosqlbench.nbdatatools.api.services.BundledCommand;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.util.concurrent.Callable;

/// Manage vectordata configuration settings.
///
/// This command provides subcommands to initialize, view, and manage
/// vectordata configuration, including cache directory settings.
@CommandLine.Command(name = "config",
    header = "Manage vectordata configuration settings",
    description = "Initialize, view, and manage vectordata configuration including cache directory settings",
    subcommands = {
        CMD_config_init.class,
        CMD_config_show.class,
        CMD_config_list_mounts.class,
        CommandLine.HelpCommand.class
    })
public class CMD_config implements Callable<Integer>, BundledCommand {

    private static final Logger logger = LogManager.getLogger(CMD_config.class);

    /// Create the CMD_config command.
    public CMD_config() {}

    /// Run the config command.
    ///
    /// @param args Command line arguments
    public static void main(String[] args) {
        CMD_config command = new CMD_config();
        logger.info("instancing commandline");
        CommandLine commandLine = new CommandLine(command)
            .setCaseInsensitiveEnumValuesAllowed(true)
            .setOptionsCaseInsensitive(true);
        logger.info("executing commandline");
        int exitCode = commandLine.execute(args);
        logger.info("exiting main");
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        // Print help information if no subcommand is specified
        CommandLine.usage(this, System.out);
        return 0;
    }
}
