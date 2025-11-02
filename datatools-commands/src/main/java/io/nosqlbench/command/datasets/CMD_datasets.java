package io.nosqlbench.command.datasets;

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

import io.nosqlbench.command.datasets.subcommands.CMD_datasets_download;
import io.nosqlbench.command.datasets.subcommands.CMD_datasets_list;
import io.nosqlbench.command.datasets.subcommands.CMD_datasets_prebuffer;
import io.nosqlbench.command.datasets.subcommands.CMD_datasets_plan;
import io.nosqlbench.nbdatatools.api.services.BundledCommand;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.util.concurrent.Callable;

/// Browse and download vector testing datasets from accessible catalogs
@CommandLine.Command(name = "datasets",
    header = "Browse and download vector testing datasets from accessible catalogs",
    description = "Contains subcommands to work with vector testing datasets from accessible catalogs",
    subcommands = {
        CMD_datasets_list.class,
        CMD_datasets_download.class,
        CMD_datasets_prebuffer.class,
        CMD_datasets_plan.class,
        CommandLine.HelpCommand.class
    })
public class CMD_datasets implements Callable<Integer>, BundledCommand {

    private static final Logger logger = LogManager.getLogger(CMD_datasets.class);

    /// Create the CMD_datasets command
    public CMD_datasets() {}

    /// Run a datasets command
    /// @param args Command line arguments
    public static void main(String[] args) {
        CMD_datasets command = new CMD_datasets();
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
        // Print help information if no subcommand is specified
        CommandLine.usage(this, System.out);
        return 0;
    }
}
