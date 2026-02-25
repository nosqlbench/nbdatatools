package io.nosqlbench.commands;

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


import io.nosqlbench.command.analyze.CMD_analyze;
import io.nosqlbench.command.catalog.CMD_catalog;
import io.nosqlbench.command.cleanup.CMD_cleanup;
import io.nosqlbench.command.compute.CMD_compute;
import io.nosqlbench.command.config.CMD_config;
import io.nosqlbench.command.convert.CMD_convert;
import io.nosqlbench.command.datasets.CMD_datasets;
import io.nosqlbench.command.fetch.CMD_fetch;
import io.nosqlbench.command.generate.CMD_generate;
import io.nosqlbench.command.info.CMD_info;
import io.nosqlbench.command.merkle.CMD_merkle;
import io.nosqlbench.command.vectordata.CMD_vectordata;
import io.nosqlbench.command.version.CMD_version;
import io.nosqlbench.slabtastic.cli.CMD_slab;
import picocli.CommandLine;

/// A collection of tools for working with vector test data
///
/// This is the top level command which serves as an props point for all sub-commands
@CommandLine.Command(name = "nbvectors", subcommands = {
    CommandLine.HelpCommand.class,
    NbvectorsGenerateCompletion.class,
    NbvectorsComplete.class,
    CMD_analyze.class,
    CMD_catalog.class,
    CMD_cleanup.class,
    CMD_compute.class,
    CMD_config.class,
    CMD_convert.class,
    CMD_datasets.class,
    CMD_fetch.class,
    CMD_generate.class,
    CMD_info.class,
    CMD_merkle.class,
    CMD_vectordata.class,
    CMD_version.class,
    CMD_slab.class
})
public class CommandBundler {

  /// create a command bundler
  public CommandBundler() {}

  /// run a nbv command
  /// @param args
  ///     command line args
  public static void main(String[] args) {
    System.setProperty("log4j2.StatusLogger.level", "OFF");
    CommandBundler command = new CommandBundler();
    CommandLine commandLine = new CommandLine(command).setCaseInsensitiveEnumValuesAllowed(true)
        .setOptionsCaseInsensitive(true);
    int exitCode = commandLine.execute(args);
    System.exit(exitCode);
  }
}
