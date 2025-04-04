package io.nosqlbench.nbvectors.commands.export_json;

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
import com.google.gson.GsonBuilder;
import io.jhdf.HdfFile;
import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;
import io.nosqlbench.nbvectors.commands.verify_knn.logging.CustomConfigurationFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/// Show details of HDF5 vector data files
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
    })
public class CMD_export_json implements Callable<Integer> {

  private static final Logger logger = LogManager.getLogger(CMD_export_json.class);

  @CommandLine.Parameters(description = "The HDF5 files to export to JSON summaries")
  private List<Path> hdf5Files;

  @CommandLine.Option(names = {"--save"}, description = "Save the JSON summaries to files")
  private boolean save;

  @CommandLine.Option(names = {"--force"}, description = "Force overwrite of existing JSON summaries")
  private boolean force;

  /// run an export_json command
  /// @param args
  ///     command line args
  public static void main(String[] args) {
    System.setProperty("slf4j.internal.verbosity", "ERROR");
    System.setProperty(
        ConfigurationFactory.CONFIGURATION_FACTORY_PROPERTY,
        CustomConfigurationFactory.class.getCanonicalName()
    );

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
      String summary = summarizer.apply(path);
      System.out.println(summary);
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
