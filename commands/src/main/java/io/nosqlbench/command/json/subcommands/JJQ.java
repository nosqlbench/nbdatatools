package io.nosqlbench.command.json.subcommands;

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


import io.nosqlbench.command.hdf5.subcommands.build_hdf5.datasource.JJQSupplier;
import io.nosqlbench.command.json.subcommands.jjq.Filemode;
import io.nosqlbench.command.json.subcommands.jjq.evaluator.JJQInvoker;
import io.nosqlbench.command.json.subcommands.jjq.outputs.JsonlFileOutput;
import io.nosqlbench.command.json.subcommands.jjq.outputs.NullOutput;
import io.nosqlbench.command.json.subcommands.jjq.outputs.PrettyConsoleOutput;
import io.nosqlbench.nbdatatools.api.services.BundledCommand;
import net.thisptr.jackson.jq.Output;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;

/// Run jjq commands
@Command(name = "jjq", description = "run jjq commands with extended functions",
    subcommands = {CommandLine.HelpCommand.class})
public class JJQ implements Callable<Integer>, BundledCommand {
  /// Logger for this class
  private static final Logger logger = LogManager.getLogger(JJQ.class);

  @Option(names = {"-i", "--in"}, required = true)
  private Path inFile;

  @Option(names = {"-o", "--out"},
      description = "The output path for processed JSON, 'null' means discard",
      defaultValue = "stdout")
  private String outPath;

  @CommandLine.Parameters(defaultValue = ".", description = "The jq expression to apply")
  //  @Option(names = {"-q", "--jq", "--query"}, required = false, defaultValue = ".")
  private String jq;

  @Option(names = {"-t", "--threads"}, required = false, defaultValue = "0")
  private int threads;

  @Option(names = {"-p", "--parts"}, required = false, defaultValue = "0")
  private int parts;

  @Option(names = {"-d", "--diag"}, required = false, defaultValue = "false", description = "A debugging mode that provides a simpler execution path, more data, and cleaner stack traces.")
  private boolean diagnose;

  @Option(names = {"-m", "--filemode"},
      defaultValue = "checkpoint",
      description = "Valid values: ${COMPLETION-CANDIDATES}")
  private Filemode filemode;

  @Option(names = {"--skipdone"},
      required = false,
      description = "If this file exists, silently " + "skip invocation of this " + "command")
  private Path skipFile;

  /// run jjq command
  /// @param args command line args
  public static void main(String[] args) {
    JJQ command = new JJQ();
    CommandLine commandLine = new CommandLine(command).setCaseInsensitiveEnumValuesAllowed(true)
        .setOptionsCaseInsensitive(true);
    int exitCode = commandLine.execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {
    if (skipFile != null && Filemode.checkpoint.isSkip(skipFile)) {
      System.err.println("skipping command, since '" + skipFile + "' is already present");
      return 0;
    }

    Output output = null;
    if (outPath == null || outPath.equals("stdout")) {
      output = new PrettyConsoleOutput();
    } else if (outPath.equalsIgnoreCase("null")) {
      output = new NullOutput();
    } else {
      output = new JsonlFileOutput(Path.of(outPath));
    }

    Supplier<String> input = JJQSupplier.path(this.inFile);

    try (JJQInvoker invoker = new JJQInvoker(input, this.jq, output)) {
      invoker.run();
    }

    return 0;
  }
}