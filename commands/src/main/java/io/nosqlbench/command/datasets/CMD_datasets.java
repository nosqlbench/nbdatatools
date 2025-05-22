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

import com.google.gson.Gson;
import io.nosqlbench.nbvectors.api.commands.BundledCommand;
import io.nosqlbench.vectordata.downloader.Catalog;
import io.nosqlbench.vectordata.discovery.TestDataSources;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

/// Browse and download hdf5 datasets from accessible catalogs
@CommandLine.Command(name = "datasets",
    description = "Browse and download hdf5 datasets from accessible catalogs",
    subcommands = {CommandLine.HelpCommand.class})
public class CMD_datasets implements Callable<Integer>, BundledCommand {

  private static final Logger logger = LogManager.getLogger(CMD_datasets.class);

  @CommandLine.Parameters(description = "Files and/or directories to catalog; All files in the "
                                        + "specified directories and paths are added to the "
                                        + "catalog files")
  private List<String> query = new ArrayList<>();

  @CommandLine.Option(names = {"--catalog"},
      description = "A directory, remote url, or other catalog container",
      defaultValue = "https://jvector-datasets-public.s3.us-east-1.amazonaws.com/")
  private List<String> catalogs = new ArrayList<>();

  @CommandLine.Option(names = {"--configdir"},
      description = "The directory to use for configuration files",
      defaultValue = "~/.config/nbvectors")
  private Path configdir;

  @CommandLine.Option(names = {"--verbose", "-v"}, description = "Show more information")
  private boolean verbose = false;

  /// create the CMD_datasets command
  public CMD_datasets() {}

  /// run a datasets command
  /// @param args
  ///     command line args
  public static void main(String[] args) {

//    System.setProperty("slf4j.internal.verbosity", "ERROR");
//    System.setProperty(
//        ConfigurationFactory.CONFIGURATION_FACTORY_PROPERTY,
//        CustomConfigurationFactory.class.getCanonicalName()
//    );

    //    System.setProperty("slf4j.internal.verbosity", "DEBUG");
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
    this.configdir =
        Path.of(this.configdir.toString().replace("~", System.getProperty("user" + ".home"))
            .replace("${HOME}", System.getProperty("user.home")));
    Gson gson = new Gson();
    TestDataSources config =
        new TestDataSources().configure(this.configdir);
    config=config.addCatalogs(this.catalogs);
    LinkedList<String> commandStream = new LinkedList<>(this.query);

    while (!commandStream.isEmpty()) {
      String verb = commandStream.removeFirst();
      Commands cmd = Commands.valueOf(verb.toLowerCase());
      switch (cmd) {
        case list, ls -> {
          Catalog catalog = Catalog.of(config);
          catalog.datasets().forEach(entry -> {
            entry.profiles().forEach((pr,prv) -> {
              System.out.println(entry.name()+":"+pr);
            });
//            System.out.println(entry.name());
            if (verbose) {
              catalog.datasets().forEach(dataset -> {
                System.out.println(" DATASET:" + dataset.toString());
//                if (v.containsKey("dimensions")) {
//                  System.out.println("   dimensions: " + v.get("dimensions"));
//                }
//                if (v.containsKey("attributes")) {
//                  Map<String, String> attrs = (Map<String, String>) v.get("attributes");
//                  System.out.println("   attributes: ");
//                  for (String an : attrs.keySet()) {
//                    System.out.println("    " + an + ": " + attrs.get(an));
//                  }
//                }
              });
//              d.tokens().forEach((k, v) -> {
//                System.out.println(" " + k + ": " + v);
//              });
              Optional.ofNullable(entry.tags()).ifPresent(tags -> {
                System.out.println(" tags: " + gson.toJson(tags));
              });

            }
          });
        }
        case download -> {
        }
      }
    }
    return 0;
  }
}
