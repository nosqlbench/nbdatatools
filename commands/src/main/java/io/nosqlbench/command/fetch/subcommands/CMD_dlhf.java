package io.nosqlbench.command.fetch.subcommands;

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


import io.nosqlbench.command.fetch.subcommands.dlhf.DatasetDownloader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/// Command line interface for downloading datasets from Hugging Face.
/// Supports authentication via token and downloads datasets to a specified cache directory.
///
/// Usage:
/// ```
/// huggingface dl [--target dir][--token token][--envkey key] dataset_name...
///```
@CommandLine.Command(name = "dlhf",
    header = "Download Huggingface Datasets via API",
    description = """
                This provides a set of utilities for working with Hugging Face.
                The command includes subcommands for downloading datasets and
                other Hugging Face-specific operations.
        """,
    subcommands = {CommandLine.HelpCommand.class})
public class CMD_dlhf implements Callable<Integer> {

  private static final Logger logger = LogManager.getLogger(CMD_dlhf.class);

  /// Target directory for downloaded files. Defaults to ~/.cache/huggingface
  @CommandLine.Option(names = {
      "--target", "--cache_dir",
  },
      defaultValue = "~/.cache/huggingface",
      description = "The target directory to download " + "to (default: ${DEFAULT-VALUE})")
  private Path target = Path.of("downloads");

  /// List of dataset names to download
  @CommandLine.Parameters(description = "The dataset name")
  private List<String> datasetNames = new ArrayList<>();

  /// Environment variable name containing the Hugging Face API token
  @CommandLine.Option(names = {"--envkey", "-k"},
      description = "The environment variable name for the token (default: ${DEFAULT-VALUE})"
                    + "%n% This is NOT the actual token.")
  private String envKey = "HF_TOKEN";

  /// Direct token value for Hugging Face API authentication
  @CommandLine.Option(names = {"--token", "-t"}, description = "The token to use for the download")
  private String token;

  /// Run DownloadHF directly
  public static void main(String[] args) {
    int exitCode = new CommandLine(new CMD_dlhf()).execute(args);
    System.exit(exitCode);
  }

  /// Create a new HuggingDl command
  ///
  /// Default constructor that initializes a new dl subcommand instance.
  public CMD_dlhf() {
  }

  /// Executes the download command for each specified dataset
  /// @return 0 on success, non-zero on failure
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
