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


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

/// A collection of tools for working with vector test data
///
/// This is the top level command which serves as an props point for all sub-commands
@CommandLine.Command(name = "nbvectors", subcommands = {
    CommandLine.HelpCommand.class
//    CMD_verify_knn.class, CMD_tag_hdf5.class, CMD_jjq.class,
//    CMD_build_hdf5.class, CMD_show_hdf5.class, CMD_export_hdf5.class, CMD_export_json.class,
//    CMD_catalog.class, CMD_datasets.class, CMD_export_hdf5new.class, CMD_merkle.class,
//    CMD_catalog2.class, CMD_mktestdata.class, CMD_generate.class,
}, modelTransformer = AddBundledCommands.class)
public class CommandBundler {

  /// run a nbv command
  /// @param args
  ///     command line args
  public static void main(String[] args) {
    //    System.setProperty("slf4j.internal.verbosity", "ERROR");
    //    System.setProperty(
    //        ConfigurationFactory.CONFIGURATION_FACTORY_PROPERTY,
    //        CustomConfigurationFactory.class.getCanonicalName()
    //    );
    Logger logger = LogManager.getLogger(CommandBundler.class);

    CommandBundler command = new CommandBundler();
    CommandLine commandLine = new CommandLine(command).setCaseInsensitiveEnumValuesAllowed(true)
        .setOptionsCaseInsensitive(true);
    int exitCode = commandLine.execute(args);
    System.exit(exitCode);
  }
}
