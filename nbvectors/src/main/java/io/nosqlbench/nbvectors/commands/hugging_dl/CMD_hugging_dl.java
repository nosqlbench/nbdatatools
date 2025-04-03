package io.nosqlbench.nbvectors.commands.hugging_dl;

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


import io.nosqlbench.nbvectors.commands.verify_knn.logging.CustomConfigurationFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.Callable;

/// Show details of HDF5 vector data files
@CommandLine.Command(name = "hugging_dl", header = "Download Huggingface Datasets via API")
public class CMD_hugging_dl implements Callable<Integer> {

  private static final Logger logger = LogManager.getLogger(CMD_hugging_dl.class);

  @CommandLine.Option(names = {
      "--target", "--cache_dir",
  },
      defaultValue = "~/.cache/huggingface",
      description = "The target directory to download " + "to (default: ${DEFAULT-VALUE})")
  private Path target = Path.of("downloads");

  @CommandLine.Parameters(description = "The dataset name")
  private List<String> datasetNames;

  @CommandLine.Option(names = {"--envkey", "-k"},
      description = "The environment variable name for the token (default: ${DEFAULT-VALUE})"
                    + "%n% This is NOT the actual token.")
  private String envKey = "HF_TOKEN";

  @CommandLine.Option(names = {"--token", "-t"}, description = "The token to use for the download")
  private String token;

  /// run a hugging_dl command
  /// @param args
  ///     command line args
  public static void main(String[] args) {
    System.setProperty("slf4j.internal.verbosity", "ERROR");
    System.setProperty(
        ConfigurationFactory.CONFIGURATION_FACTORY_PROPERTY,
        CustomConfigurationFactory.class.getCanonicalName()
    );

    //    System.setProperty("slf4j.internal.verbosity", "DEBUG");
    CMD_hugging_dl command = new CMD_hugging_dl();
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
    target = Path.of(target.toString().replaceAll("~", System.getProperty("user.home")));

    String effectiveToken = findToken(this.token, envKey);

    for (String datasetName : datasetNames) {
      Path datasetDirectory = target.resolve(datasetName);
      DatasetDownloader datasetDownloader = null;
      try {
        datasetDownloader = new DatasetDownloader(effectiveToken, datasetName, datasetDirectory);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      datasetDownloader.download();
    }
    return 0;
  }

  private String findToken(String token, String envKey) {
    token = token != null ? token : System.getenv(envKey);
    Path tokenFallbackPath =
        Paths.get(System.getProperty("user.home"), ".cache", "huggingface", "token");
    try {
      token = token != null ? token :
          Files.exists(tokenFallbackPath) ? Files.readString(tokenFallbackPath) : null;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    if (token == null) {
      throw new RuntimeException(
          "no token found in environment variable " + envKey + " or in file " + tokenFallbackPath);
    }
    return token.trim();
  }
}
