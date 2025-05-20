package io.nosqlbench.command.export_json;

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


import io.nosqlbench.nbvectors.api.commands.BundledCommand;
import io.nosqlbench.nbvectors.api.services.Selector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;

/// Show details of HDF5 vector data files
@Selector("export_json")
@CommandLine.Command(name = "export_json",
    headerHeading = "Usage:%n%n",
    synopsisHeading = "%n",
    descriptionHeading = "%nDescription%n%n",
    parameterListHeading = "%nParameters:%n%",
    optionListHeading = "%nOptions:%n",
    header = "export HDF5 KNN answer-keys to JSON summary files",
    description = """
        TBD
        """,
    exitCodeListHeading = "Exit Codes:%n",
    exitCodeList = {
        "0: no errors",
    },subcommands = {CommandLine.HelpCommand.class})
public class CMD_export_json implements Callable<Integer>, BundledCommand {

  private static final Logger logger = LogManager.getLogger(CMD_export_json.class);

  @CommandLine.Parameters(description = "The HDF5 files to export to JSON summaries")
  private List<Path> hdf5Files;

  @CommandLine.Option(names = {"--save"}, description = "Save the JSON summaries to files")
  private boolean save;

  @CommandLine.Option(names = {"--force"},
      description = "Force overwrite of existing JSON summaries")
  private boolean force;

  @CommandLine.Option(names = {"--mode"},
      description = "The mode to use for the export (default:"
                    + " ${DEFAULT-VALUE} valid values: ${COMPLETION-CANDIDATES})",
      defaultValue = "raw")
  private Mode mode = Mode.raw;

  /// run an export_json command
  /// @param args
  ///     command line args
  public static void main(String[] args) {
    //    System.setProperty("slf4j.internal.verbosity", "ERROR");
    //    System.setProperty(
    //        ConfigurationFactory.CONFIGURATION_FACTORY_PROPERTY,
    //        CustomConfigurationFactory.class.getCanonicalName()
    //    );

    //    System.setProperty("slf4j.internal.verbosity", "DEBUG");
    CMD_export_json command = new CMD_export_json();
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
    Hdf5JsonSummarizer summarizer = new Hdf5JsonSummarizer();

    for (Path path : this.hdf5Files) {
      if (Files.isDirectory(path)) {
        System.err.println("skipping " + path);
        continue;
      }
      String summary = null;
      summary = switch (this.mode) {
        case raw -> summarizer.apply(path);
//        case cooked -> new TestDataGroup(path).toJson();
        default -> throw new RuntimeException("unknown mode: " + mode);
      };
      if (this.save) {
        Path jsonPath = Path.of(path.toString().replaceFirst("\\.hdf5$", ".json"));
        if (Files.exists(jsonPath) && !this.force) {
          throw new RuntimeException("file already exists (use --force to override): " + jsonPath);
        }
        try {
          Files.writeString(jsonPath, summary);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

      }
    }
    return 0;
  }
}
